package binance

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
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
			//StartAfter:               nil, // Optional, start after a key
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

func (s *HistoryConsumer) Download(path string, w io.WriterAt) (n int64, err error) {
	n, err = s.downloader.Download(s.ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
		//IfModifiedSince:            nil,
		//IfUnmodifiedSince:          nil,
		//VersionId:                  nil,
	})
	return
}

func (s *HistoryConsumer) DownloadAndExtract(
	asset HistoryAsset,
) (info chan *AssetETLInfo) {
	prefix := asset.SymbolFrameLink()
	pths := make([]string, 0)
	info = make(chan *AssetETLInfo)

	for result := range s.List(prefix) {
		if result.Err != nil {
			log.Printf("error listing files: %v", result.Err)
			continue
		}

		path := *result.Key

		// Skip CHECKSUM and folders
		if strings.HasSuffix(path, "CHECKSUM") || strings.HasSuffix(path, "/") {
			continue
		}

		pths = append(pths, path)

		// Collect all zip files
		for zipInfo := range s.CacheZip(path) {
			if zipInfo.Err != nil {
				log.Printf("error caching zip file: %v", zipInfo.Err)
				continue
			}

			if zipInfo.Status != READING_ZIP {
				// Decompress ZIP file
				csvBuffer, err := data.Decompress(zipInfo.Buffer.Bytes())
				if err != nil {
					log.Printf("error decompressing zip file: %v", err)
					continue
				}
				// Store CSV file parallel to ZIP
				csvPath := strings.TrimSuffix(path, ".zip") + ".csv"
				info <- &AssetETLInfo{
					Path:   path,
					Status: READING_CSV,
					Buffer: csvBuffer,
					Err:    csvBuffer.Persist(csvPath),
				}
			}
		}
	}
	return
}

// CacheZip - download and cache zip file
func (s *HistoryConsumer) CacheZip(path string) (info chan *AssetETLInfo) {
	// Check if a file already locally exists
	// ReadCSV existing file
	go func() {
		defer close(info)

		// Do we cache the file?
		if fs.FileExists(path) {
			// Read zip
			buf, err := fs.ReadZip(path)
			info <- &AssetETLInfo{
				Path:   path,
				Status: READING_ZIP,
				Buffer: buf,
				Err:    err,
			}

			log.Printf("File %s already exists, loaded from disk", path)
		} else {
			// Download the file if it doesn't exist
			buf := &data.Buffer{}
			info <- &AssetETLInfo{
				Path:   path,
				Status: DOWNLOADING,
			}
			_, err := s.Download(path, buf)
			info <- &AssetETLInfo{
				Path:   path,
				Status: READING_ZIP,
				Buffer: buf,
				Err:    err,
			}

			// ReadCSV existing file
			info <- &AssetETLInfo{
				Path:   path,
				Status: PERSISTED_ZIP,
				Buffer: buf,
				Err:    buf.Persist(path),
			}
		}
	}()

	// Store a downloaded file into a local directory save it as a zip file
	return info
}

// Get ETLBinanceHistoryAsset - download, extract, transform and load data from binance history assets
//   - If ZIP file already exists, load it from disk
//   - If CSV file already exists, load it from disk
//   - If a parquet file already exists, load it from disk
//   - Check if a parquet file is empty, if so, delete it and recreate it
func (s *HistoryConsumer) Get(asset *HistoryAsset) (info chan *AssetETLInfo) {
	prefix := asset.SymbolFrameLink()
	info = make(chan *AssetETLInfo)

	for result := range s.List(prefix) {
		if result.Err != nil {
			log.Printf("error listing files: %v", result.Err)
			continue
		}

		path := *result.Key

		// Skip CHECKSUM and folders
		if strings.HasSuffix(path, "CHECKSUM") || strings.HasSuffix(path, "/") {
			continue
		}

		go func() {
			// Collect all zip files
			for zipInfo := range s.CacheZip(path) {
				if zipInfo.Err != nil {
					log.Printf("error caching zip file: %v", zipInfo.Err)
					continue
				}

				if zipInfo.Status != READING_ZIP {
					// Decompress ZIP file
					csvBuffer, err := data.Decompress(zipInfo.Buffer.Bytes())
					if err != nil {
						log.Printf("error decompressing zip file: %v", err)
						continue
					}
					// Store CSV file parallel to ZIP
					csvPath := strings.TrimSuffix(path, ".zip") + ".csv"
					info <- &AssetETLInfo{
						Path:   path,
						Status: READING_CSV,
						Buffer: csvBuffer,
						Err:    csvBuffer.Persist(csvPath),
					}

					// Store CSV as structured data into duckdb hive partitioned table as parquet files
					if asset.Indicator == Klines {
						var klines []*ParquetKline
						klineCh, csvErrCh := data.ReadCSVChan[Kline](csvBuffer)
						pKlineCh := make(chan *ParquetKline)
						prqErrCh := make(chan error)
						parquetPath := strings.TrimSuffix(path, ".zip") + ".parquet"
						go fs.WriteParquet[ParquetKline](parquetPath, pKlineCh, prqErrCh)

						for {
							select {
							case kline, ok := <-klineCh:
								if !ok {
									fmt.Println("klineCh closed")
									return
								}
								pKlike := NewParquetKline(kline)
								pKlineCh <- pKlike
								klines = append(klines, pKlike)
							case err, ok := <-csvErrCh:
								if !ok {
									fmt.Println("csvErrCh closed")
								}
								if err != nil {
									fmt.Printf("Error reading CSV: %v\n", err)
								}
								return
							}
						}
						close(klineCh)
						close(csvErrCh)

					} else if asset.Indicator == AggTrades {
						var aggTrades []*ParquetAggTrade
						prqAggTradeCh := make(chan *ParquetAggTrade)
						prqErrCh := make(chan error)
						parquetPath := strings.TrimSuffix(path, ".zip") + ".parquet"
						go fs.WriteParquet[ParquetAggTrade](parquetPath, prqAggTradeCh, prqErrCh)
						err = data.ReadCSV[AggTrade](csvBuffer, func(a *AggTrade) error {
							prqAggTrade := NewParquetAggTrade(a)
							prqAggTradeCh <- prqAggTrade
							aggTrades = append(aggTrades, prqAggTrade)
							return nil
						})
						if err != nil {
							fmt.Printf("Error reading CSV: %v\n", err)
						}
					}
				}
			}

			close(info)
		}()
		//fmt.Printf("Found %d files\n", len(pths))
	}
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
		bucket:     "data.binance.vision",
		client:     client,
		downloader: downloader,
		cfg:        cfg,
		ctx:        ctx,
	}, nil
}
