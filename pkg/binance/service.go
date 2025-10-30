package binance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// NewAwsConfig returns aws config for binance data
// Ultimately this is just S3 with anonymous access and ap-northeast-1 region
// Alternatives:
//   - https://data.binance.vision/?prefix=data/spot/monthly/trades/0GBNB/
//   - https://data.binance.vision.s3.amazonaws.com/
func NewAwsConfig(ctx context.Context) (*aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion("ap-northeast-1"),
		config.WithEndpointResolver(aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
			if service == s3.ServiceID {
				return aws.Endpoint{
					URL:           "https://s3-ap-northeast-1.amazonaws.com",
					SigningRegion: "ap-northeast-1",
				}, nil
			}
			return aws.Endpoint{}, fmt.Errorf("unknown service: %s", service)
		})),
		config.WithCredentialsProvider(aws.AnonymousCredentials{}),
	)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// HistoryConsumer of binance historical assets
type HistoryConsumer struct {
	db         *sql.DB             // DuckDB
	ctx        context.Context     // Context
	cfg        *aws.Config         // AWS Config
	client     *s3.Client          // S3 Client
	downloader *manager.Downloader // S3 Downloader
	bucket     string
	localDir   string // Local directory for downloaded files

	// Options
	RecreateParquet bool // Recreate parquet files if they already exist
}

type ListResult struct {
	Key  *string
	Page *s3.ListObjectsV2Output
	Obj  *types.Object
	Err  error
}

// List objects by paths
func (s *HistoryConsumer) List(path string) (ch chan ListResult) {
	ch = make(chan ListResult)
	go func() {
		// Close channel
		defer close(ch)

		// Create paginator
		list := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
			Bucket:  aws.String(s.bucket),
			MaxKeys: aws.Int32(100), // By default, 1000, but we need to iterate over all pages using callbacks
			Prefix:  aws.String(path),
			// StartAfter:               nil, // Optional, start after a key
		})

		// Iterate over pages
		for list.HasMorePages() {
			page, err := list.NextPage(s.ctx)

			// Handle error
			if err != nil {
				ch <- ListResult{
					Page: page,
					Err:  err,
				}
				continue
			}

			// Handle objects as they come extract paths
			for _, obj := range page.Contents {
				key := *obj.Key
				ch <- ListResult{
					Key:  &key,
					Page: page,
					Obj:  &obj,
				}
			}
		}

	}()
	return ch
}

var ErrNotZipLink = fmt.Errorf("not found")

func (s *HistoryConsumer) DownloadAndTransform(
	asset *HistoryAsset,
) (
	infoCh chan *AssetETLInfo,
	errCh chan error,
) {
	infoCh = make(chan *AssetETLInfo)
	errCh = make(chan error)

	go func() {
		defer close(infoCh)
		defer close(errCh)

		if !asset.IsZipLink() {
			errCh <- errors.Join(ErrNotZipLink, fmt.Errorf("invalid asset: %s", asset))
			return
		}

		link := asset.SymbolDateAssetZipLink()
		parquetPath := strings.TrimSuffix(link, ".zip") + ".parquet"

		if s.RecreateParquet {
			os.Remove(parquetPath)
		}

		// Check if a file already exists, then skip downloading, transforming and loading
		if fs.FileExists(parquetPath) {
			infoCh <- &AssetETLInfo{
				Path:   parquetPath,
				Status: StatusParquetDone,
				Info:   fmt.Sprintf("Parquet file already exists: %s", parquetPath),
			}
			return
		}

		// Download and cache zip file
		for zipInfo := range s.CacheZip(link) {
			if zipInfo.Err != nil {
				errCh <- errors.Join(zipInfo.Err, fmt.Errorf("error caching zip file: %v", zipInfo.Err))
				continue
			}

			if zipInfo.Status == StatusReadingZip {
				// Decompress ZIP file
				csvBuffer, err := data.Decompress(zipInfo.Buffer.Bytes())
				if err != nil {
					errCh <- fmt.Errorf("error decompressing zip file: %v", err)
					continue
				}
				// Store CSV file parallel to ZIP
				csvPath := strings.TrimSuffix(link, ".zip") + ".csv"
				infoCh <- &AssetETLInfo{
					Path:   csvPath,
					Status: StatusReadingCsv,
					Buffer: csvBuffer,
					Err:    csvBuffer.Persist(csvPath),
				}

				// Store CSV as structured data into duckdb hive partitioned table as parquet files
				if asset.Indicator == Klines {
					csvKlineCh, csvErrCh := data.ReadHeaderlessCSVChan[Kline](csvBuffer)
					parquetKlineCh, prqErrCh := data.WriteParquet[ParquetKline](parquetPath)
					wroteKlines := 0
				ETLLoop:
					// Fan-in and lifecycle management
					for {
						select {
						case csvKline, ok := <-csvKlineCh:
							if !ok {
								close(parquetKlineCh)
								break ETLLoop
							}
							parquetKlineCh <- NewParquetKline(csvKline)
							wroteKlines++
							// infoCh <- &AssetETLInfo{
							//	Path:   parquetPath,
							//	Status: StatusTransforming,
							// }
						case err, ok := <-csvErrCh:
							if !ok {
								close(parquetKlineCh)
								break ETLLoop
							}
							close(parquetKlineCh) // Close parquet channel on CSV error
							infoCh <- &AssetETLInfo{
								Path:   parquetPath,
								Status: StatusError,
								Err:    fmt.Errorf("error reading csv: %v", err),
							}
							break ETLLoop
						case err, ok := <-prqErrCh:
							if !ok {
								continue
							}
							close(parquetKlineCh) // Close parquet channel on parquet error
							infoCh <- &AssetETLInfo{
								Path:   parquetPath,
								Status: StatusError,
								Err:    fmt.Errorf("error writing parquet: %v", err),
							}
							break ETLLoop
						}
					}

					// Ensure parquet writer finishes and closes file before reporting done
					// for err := range prqErrCh {
					// 	if err != nil {
					// 		infoCh <- &AssetETLInfo{
					// 			Path:   parquetPath,
					// 			Status: StatusError,
					// 			Err:    fmt.Errorf("error writing parquet: %v", err),
					// 		}
					// 	}
					// }

					infoCh <- &AssetETLInfo{
						Path:   parquetPath,
						Status: StatusParquetDone,
						Info:   fmt.Sprintf("Wrote %d parquetKlines to parquet", wroteKlines),
					}
				} else if asset.Indicator == AggTrades {
					csvAggCh, csvErrCh := data.ReadHeaderlessCSVChan[AggTrade](csvBuffer)
					parquetAggCh, prqErrCh := data.WriteParquet[ParquetAggTrade](parquetPath)
					wroteTrades := 0
				ETLLoopAgg:
					for {
						select {
						case csvAgg, ok := <-csvAggCh:
							if !ok {
								close(parquetAggCh)
								break ETLLoopAgg
							}
							parquetAggCh <- NewParquetAggTrade(csvAgg)
							wroteTrades++
						case err, ok := <-csvErrCh:
							if !ok {
								close(parquetAggCh)
								break ETLLoopAgg
							}
							close(parquetAggCh) // Close parquet channel on CSV error
							infoCh <- &AssetETLInfo{
								Path:   parquetPath,
								Status: StatusError,
								Err:    fmt.Errorf("error reading csv: %v", err),
							}
							break ETLLoopAgg
						case err, ok := <-prqErrCh:
							if !ok {
								continue
							}
							close(parquetAggCh) // Close parquet channel on parquet error
							infoCh <- &AssetETLInfo{
								Path:   parquetPath,
								Status: StatusError,
								Err:    fmt.Errorf("error writing parquet: %v", err),
							}
							break ETLLoopAgg
						}
					}
					for err := range prqErrCh {
						if err != nil {
							infoCh <- &AssetETLInfo{
								Path:   parquetPath,
								Status: StatusError,
								Err:    fmt.Errorf("error writing parquet: %v", err),
							}
						}
					}
					infoCh <- &AssetETLInfo{
						Path:   parquetPath,
						Status: StatusParquetDone,
						Info:   fmt.Sprintf("Wrote %d parquetAggTrades to parquet", wroteTrades),
					}
				}
			}
		}

	}()
	return
}

// Download file from S3
//
//	Example: - data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2023-06.zip
func (s *HistoryConsumer) Download(path string, w io.WriterAt) (n int64, err error) {
	download, err := s.downloader.Download(s.ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
		// IfModifiedSince:            nil,
		// IfUnmodifiedSince:          nil,
		// VersionId:                  nil,
	})
	return download, err
}

// CacheZip - download and cache zip file
func (s *HistoryConsumer) CacheZip(path string) (info chan *AssetETLInfo) {
	info = make(chan *AssetETLInfo)
	// Check if a file already locally exists
	// ReadCSV existing file
	go func() {
		defer close(info)
		// Do we cache the file?
		if fs.FileExists(path) {
			// Read zip
			buf, err := data.ReadIntoBuffer(path)
			info <- &AssetETLInfo{
				Path:   path,
				Status: StatusReadingZip,
				Buffer: buf,
				Err:    err,
			}

			log.Printf("File %s already exists, loaded from disk", path)
		} else {
			// Download the file if it doesn't exist
			buf := &data.Buffer{}
			info <- &AssetETLInfo{
				Path:   path,
				Status: StatusDownloading,
			}
			_, err1 := s.Download(path, buf)
			a := &AssetETLInfo{
				Path:   path,
				Status: StatusReadingZip,
				Buffer: buf,
				Err:    err1,
			}
			info <- a

			// ReadCSV existing file
			info <- &AssetETLInfo{
				Path:   path,
				Status: StatusPersistingZip,
				Buffer: buf,
				Err:    buf.Persist(path),
			}
		}
	}()

	// Store a downloaded file into a local directory save it as a zip file
	return info
}

// GetAsset ETLBinanceHistoryAsset - download, extract, transform and load data from binance history assets
//   - If ZIP file already exists, load it from disk
//   - If CSV file already exists, load it from disk
//   - If a parquet file already exists, load it from disk
//   - Check if a parquet file is empty, if so, delete it and recreate it
func (s *HistoryConsumer) GetAsset(asset *HistoryAsset) (info chan *AssetETLInfo) {
	prefix := asset.SymbolFrameLink()
	info = make(chan *AssetETLInfo)

	go func() {
		for result := range s.List(prefix) {
			// Handle error
			if result.Err != nil {
				info <- &AssetETLInfo{
					Path:   *result.Key,
					Status: StatusError,
					Err:    fmt.Errorf("error listing files: %v", result.Err),
				}
				continue
			}
			path := *result.Key

			// Skip CHECKSUM and folders
			if strings.HasSuffix(path, "CHECKSUM") || strings.HasSuffix(path, "/") {
				continue
			}

			zipAsset, err := NewHistoryAssetByPath(path)
			if err != nil {
				info <- &AssetETLInfo{
					Path:   path,
					Status: StatusError,
					Err:    fmt.Errorf("error creating asset path: %v", err),
				}
			}

			assetInfoCh, assetErrCh := s.DownloadAndTransform(zipAsset)
			for inCh, erCh := assetInfoCh, assetErrCh; inCh != nil || erCh != nil; {
				select {
				case assetInfo, ok := <-inCh:
					if !ok {
						inCh = nil
						continue
					}
					info <- assetInfo
				case assetErr, ok := <-erCh:
					if !ok {
						erCh = nil
						continue
					}
					info <- &AssetETLInfo{
						Path:   path,
						Status: StatusError,
						Err:    fmt.Errorf("error downloading and transforming asset: %v", assetErr),
					}
				}
			}
			// fmt.Printf("Found %d files\n", len(pths))
		}
		close(info)
	}()

	return info
}

func NewHistoryConsumer(ctx context.Context) (*HistoryConsumer, error) {
	cfg, err := NewAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(*cfg)
	downloader := manager.NewDownloader(client)
	return &HistoryConsumer{
		RecreateParquet: true,
		bucket:          "data.binance.vision",
		client:          client,
		downloader:      downloader,
		cfg:             cfg,
		ctx:             ctx,
	}, nil
}
