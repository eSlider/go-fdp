package binance

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

var ErrNotZipLink = fmt.Errorf("not found")

type CandleParquetQuery struct {
	Market     string `validate:"required"`
	Frame      string `validate:"required"`
	Indicator  string `validate:"required"`
	MarketType string `validate:"required"`
	From       int64  `validate:"required"`
	To         int64  `validate:"required"`
	DataPath   string // Path to data directory
	HivePath   string // Path to parquet files. Example: "*/*/*.parquet"
}

// HistoryConsumer of binance historical assets
type HistoryConsumer struct {
	db         *sql.DB             // DuckDB
	ctx        context.Context     // Context
	cfg        *aws.Config         // AWS Config
	client     *s3.Client          // S3 Client
	downloader *manager.Downloader // S3 Downloader
	bucket     string              // Bucket
	localDir   string              // Local directory for downloaded files

	// Options
	recreateParquet bool // Recreate parquet files if they already exist
	storeZIP        bool // Store zip files locally flag
	storeCSV        bool // Store CSV files locally
}

type ListResult struct {
	Key  *string
	Page *s3.ListObjectsV2Output
	Obj  *types.Object
	Err  error
}

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

func (s *HistoryConsumer) DownloadAndTransform(
	asset *HistoryAsset,
) (
	infoCh chan *AssetETLInfo,
	errCh chan error,
) {
	infoCh = make(chan *AssetETLInfo)
	errCh = make(chan error)

	go func() {
		// If it's today, get candles from current api
		if asset.IsToday() {
			s.DownloadToday(asset, errCh, infoCh)
			return
		}
		defer close(infoCh)
		defer close(errCh)

		if err := asset.IsHistoryLinkAvailable(); err != nil {
			errCh <- errors.Join(ErrNotZipLink, fmt.Errorf("invalid asset: %s", asset))
			return
		}

		link := asset.SymbolDateAssetZipLink()
		parquetPath := fs.GetModuleRelativePath(asset.ParquetPath())

		// Delete parquet file if it exists
		// if s.recreateParquet {
		// 	os.Remove(parquetPath)
		// }

		// Check if a file already exists, then skip downloading, transforming and loading
		if fs.FileExists(parquetPath) {
			infoCh <- &AssetETLInfo{
				Path:   parquetPath,
				Status: StatusParquetReady,
				Info:   fmt.Sprintf("Parquet file already exists: %s", parquetPath),
			}
			return
		}

		// Download and cache zip file
		zipInfo := s.CacheZip(link)
		if zipInfo.Err != nil {
			errCh <- errors.Join(zipInfo.Err, fmt.Errorf("error caching zip file: %v", zipInfo.Err))
			return
		}

		if zipInfo.Status == StatusZipReady {
			// Decompress ZIP file
			csvBuffer, err := data.Decompress(zipInfo.Buffer.Bytes())
			if err != nil {
				errCh <- fmt.Errorf("error decompressing zip file: %v", err)
				return
			}

			if s.storeCSV {
				// Store CSV file parallel to ZIP
				csvPath := fs.GetModuleRelativePath(strings.TrimSuffix(link, ".zip") + ".csv")
				infoCh <- &AssetETLInfo{
					Path:   csvPath,
					Status: StatusCSVReading,
					Buffer: csvBuffer,
					Err:    csvBuffer.Persist(csvPath),
				}
			}

			// asset.Indicator == AggTrades
			// if asset.Indicator == Klines {
			// csvReadCh, csvErrCh := data.ReadHeaderlessCSVChan[Kline](csvBuffer)
			parquetWriteCh, prqErrCh := data.WriteParquet[ParquetKline](parquetPath)
			wroteKlines := 0

			// Read CSV and write to parquet
			for row := range data.ReadHeaderlessCSV[Kline](csvBuffer) {
				if row.Error != nil {
					errCh <- fmt.Errorf("error reading csv: %v", row.Error)
					continue
				}
				parquet, err := row.Value.Parquet()
				if err != nil {
					errCh <- fmt.Errorf("error converting csv entry to parquet: %v", err)
					continue
				}
				parquetWriteCh <- parquet
			}
			close(parquetWriteCh)

			for err := range prqErrCh {
				errCh <- fmt.Errorf("error writing parquet: %v", err)
			}

			infoCh <- &AssetETLInfo{
				Path:   parquetPath,
				Status: StatusParquetReady,
				Info:   fmt.Sprintf("Wrote %d parquetKlines to parquet", wroteKlines),
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
func (s *HistoryConsumer) CacheZip(link string) (info *AssetETLInfo) {
	// Check if a file already locally exists
	// ReadCSV existing file
	var err error
	path := fs.GetModuleRelativePath(link)
	info = &AssetETLInfo{Path: path}

	// Do we cache the file?
	if fs.FileExists(path) {
		// Read zip
		info.Status = StatusZipReading
		if info.Buffer, err = data.ReadIntoBuffer(path); err != nil {
			info.Err = fmt.Errorf("error reading zip file: %v", err)
			return
		}
	} else {
		// Download the file if it doesn't exist
		info.Status = StatusZipDownloading
		info.Buffer = &data.Buffer{}
		if _, err1 := s.Download(link, info.Buffer); err1 != nil {
			info.Err = fmt.Errorf("error downloading zip file: %v", err1)
			return
		}

		go func() {
			if s.storeZIP {
				// Store a downloaded file into a local directory save it as a zip file
				// info.Persistance Status = StatusZipPersisting
				if err := info.Buffer.Persist(path); err != nil {
					// info.Err = fmt.Errorf("error storing zip file: %v", err)
					fmt.Printf("error storing zip file: %v\n", err)
				}
			}
		}()
	}

	info.Status = StatusZipReady
	return info
}

// GetAssets ETLBinanceHistoryAsset - download, extract, transform and load data from binance history assets
//   - If ZIP file already exists, load it from disk
//   - If CSV file already exists, load it from disk
//   - If a parquet file already exists, load it from disk
//   - Check if a parquet file is empty, if so, delete it and recreate it
func (s *HistoryConsumer) GetAssets(asset *HistoryAsset) (info chan *AssetETLInfo) {
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

func (s *HistoryConsumer) DownloadToday(asset *HistoryAsset, errCh chan error, infoCh chan *AssetETLInfo) {
	duckDBPath := fs.GetModuleRelativePath(asset.TodayDuckDBPath())
	//
	// c, err := duckdb.NewConnector("", nil)
	// if err != nil {
	// 	log.Fatalf("could not initialize new connector: %s", err.Error())
	// }
	//
	// con, err := c.Connect(context.Background())
	// if err != nil {
	// 	log.Fatalf("could not connect: %s", err.Error())
	// }
	//
	// db := sql.OpenDB(c)
	// if _, err := db.Exec(`CREATE TABLE users (name VARCHAR, age INTEGER)`); err != nil {
	// 	log.Fatalf("could not create table users: %s", err.Error())
	// }

	// Open DuckDB file for today's cache
	db, err := sql.Open("duckdb", duckDBPath)
	if err != nil {
		errCh <- fmt.Errorf("error opening DuckDB file %s: %v", duckDBPath, err)
		return
	}
	// defer db.Close()

	// Create table for caching kline data if it doesn't exist
	createTableSQL := `
		-- Crate candles table
		CREATE TABLE IF NOT EXISTS candles
		(
			-- unique identifier
			openTime timestamp primary key,
			--closeTime TIMESTAMP GENERATED ALWAYS AS (openTime + INTERVAL '1 minute' + INTERVAL '-1 microsecond') ,

			open double NOT NULL,
			high double NOT NULL,
			low double NOT NULL,
			close double NOT NULL,

			volume double NOT NULL
		);

		CREATE TABLE IF NOT EXISTS klines (
			open_time BIGINT,
			close_time BIGINT,
			open_price DOUBLE,
			high_price DOUBLE,
			low_price DOUBLE,
			close_price DOUBLE,
			volume DOUBLE,
			PRIMARY KEY (open_time)
		);

		-- Saves the last processed query
		CHECKPOINT`
	if _, err := db.Exec(createTableSQL); err != nil {
		errCh <- fmt.Errorf("error creating klines table: %v", err)
		return
	}
	if err := db.Close(); err != nil {
		errCh <- fmt.Errorf("error closing DuckDB file: %v", err)
	}
	now := time.Now()
	// Process data in hourly chunks from midnight to now
	startTime := data.LastMomentOfYesterday()

	var wg sync.WaitGroup
	for startTime.Before(now) {
		start := startTime.UnixMicro()
		end := startTime.Add(time.Hour).UnixMicro()

		if end > now.UnixMicro() {
			end = now.UnixMicro()
		}

		go func(
			wg *sync.WaitGroup,
			db *sql.DB,
			errCh chan error,
		) {
			wg.Add(1)

			// Check if we already have data for this time range
			var lastOpenTime int
			checkSQL := `
				SELECT open_time
				FROM klines
				WHERE open_time >= ? AND open_time < ?
				ORDER BY open_time DESC
				LIMIT 1`
			if err := db.QueryRow(checkSQL, start, end).Scan(&lastOpenTime); err != nil {
				errCh <- fmt.Errorf("error checking existing data: %v", err)
				return
			}

			lastOpenDate := data.AnyTimestampToTime(int64(lastOpenTime))
			// fmt.Printf("Last open time: %s\n", lastOpenDate)
			start = lastOpenDate.UnixMicro()

			// Fetch data from API
			klines, err := GetCurrentCandles(
				&CandleRequestV3{
					Symbol:    asset.Market,
					Interval:  asset.Frame.String(),
					StartTime: &start,
					EndTime:   &end,
					TimeZone:  nil,
					Limit:     60,
				},
			)
			if err != nil {
				errCh <- fmt.Errorf("error getting current candle: %v", err)
				return
			}

			// Store new data in DuckDB
			for _, kline := range klines {
				insertSQL := `
					INSERT OR IGNORE INTO klines (open_time, close_time, open_price, high_price, low_price, close_price, volume)
					VALUES (?, ?, ?, ?, ?, ?, ?)`
				_, err = db.Exec(insertSQL,
					kline.OpenTime*1000, // Convert to microseconds
					kline.CloseTime*1000,
					kline.OpenPrice,
					kline.HighPrice,
					kline.LowPrice,
					kline.ClosePrice,
					kline.Volume,
				)
				if err != nil {
					errCh <- fmt.Errorf("error inserting kline data: %v", err)
					return
				}
			}
		}(&wg, db, errCh)

		startTime = startTime.Add(time.Hour)
	}
	wg.Wait()

	infoCh <- &AssetETLInfo{
		Path:   duckDBPath,
		Status: StatusParquetReady, // Reusing status for completion
		Info:   fmt.Sprintf("Cached today's klines in DuckDB: %s", duckDBPath),
	}
}

func NewHistoryConsumer(ctx context.Context) (*HistoryConsumer, error) {
	cfg, err := NewAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(*cfg)
	downloader := manager.NewDownloader(client)
	return &HistoryConsumer{
		recreateParquet: false,
		storeZIP:        false,
		bucket:          "data.binance.vision",
		client:          client,
		downloader:      downloader,
		cfg:             cfg,
		ctx:             ctx,
	}, nil
}
