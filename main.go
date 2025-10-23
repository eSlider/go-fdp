package main

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/duckdb/duckdb-go/v2"
)

type Frequency string

const (
	Monthly Frequency = "monthly"
	Daily             = "daily"
)

type Interval string

const (
	OneSecond   Interval = "1s"
	OneMinute            = "1m"
	ThreeMinute          = "3m"
	FiveMinute           = "5m"
	FifteenMin           = "15m"
	ThirtyMin            = "30m"
	OneHour              = "1h"
	TwoHour              = "2h"
	OneDay               = "1d"
)

type Market string

const (
	Spot    Market = "spot"
	Futures        = "futures"
	Option         = "option"
)

type Indicator string

const (
	Klines    Indicator = "klines"
	Trades              = "trades"
	AggTrades           = "aggTrades"
)

type HistoryQuery struct {
	Market
	Frequency
	Interval
	Indicator

	Date   time.Time
	Symbol string
}

// Link returns normalized link to zip file
func Link(hq *HistoryQuery) string {
	if hq.Market == "" {
		hq.Market = Spot
	}

	if hq.Frequency == "" {
		hq.Frequency = Monthly
	}
	if hq.Interval == "" {
		hq.Interval = OneSecond
	}

	if hq.Indicator == "" {
		hq.Indicator = Klines
	}

	return fmt.Sprintf("data/%s/%s/%s/%s/%s/%s-%s-%s.zip",
		hq.Market,
		hq.Frequency,
		hq.Indicator,
		hq.Symbol,
		hq.Interval,
		hq.Symbol,
		hq.Interval,
		hq.Date.Format("2006-01"),
	)
}

// NewBinanceAwsConfig returns aws config for binance data
// Ultimately this is just S3 with anonymous access and ap-northeast-1 region
// Alternatives:
//   - https://data.binance.vision/?prefix=data/spot/monthly/trades/0GBNB/
//   - https://data.binance.vision.s3.amazonaws.com/
func NewBinanceAwsConfig(ctx context.Context) (*aws.Config, error) {
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

type BinanceService struct {
	db         *sql.DB             // DuckDB
	ctx        context.Context     // Context
	cfg        *aws.Config         // AWS Config
	client     *s3.Client          // S3 Client
	downloader *manager.Downloader // S3 Downloader
	bucket     string
}

// List objects by path whic
func (s *BinanceService) List(
	path string,
	callbacks ...func(path string, page *s3.ListObjectsV2Output) error,
) (
	paths []string,
	err error,
) {

	// List all files in the bucket in the provided path

	// Create paginator
	list := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket:  aws.String(s.bucket),
		MaxKeys: aws.Int32(100), // By default 1000, but we need to iterate over all pages using callbacks
		Prefix:  aws.String(path),
		//StartAfter:               nil, // Optional, start after key
	})

	// Iterate over pages
	for list.HasMorePages() {
		page, err := list.NextPage(s.ctx)
		if err != nil {
			log.Fatalf("list error: %v", err)
		}

		// Handle objects as they come extract paths
		for _, obj := range page.Contents {
			key := *obj.Key
			paths = append(paths, key)

			// Handle callbacks
			for _, callback := range callbacks {
				// Run calback in goroutine's
				go func() {
					if errC := callback(key, page); errC != nil {
						err = errors.Join(err, errC)
					}
				}()
			}

		}
	}

	return paths, err
}

func (s *BinanceService) Download(path string, w io.WriterAt) (n int64, err error) {
	n, err = s.downloader.Download(s.ctx, w, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(path),
		//IfModifiedSince:            nil,
		//IfUnmodifiedSince:          nil,
		//VersionId:                  nil,
	})
	return
}

func NewBinanceService(ctx context.Context) (*BinanceService, error) {
	// Wal=true, shared=true, not locked
	c, err := duckdb.NewConnector("data.duckdb", nil)
	if err != nil {
		log.Fatalf("could not initialize new connector: %s", err.Error())
	}

	db := sql.OpenDB(c)
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS files (
		id UUID DEFAULT gen_random_uuid(),
    	name VARCHAR,
    	created_at TIMESTAMP
  	)`); err != nil {
		log.Fatalf("could not create files table: %s", err.Error())
	}

	cfg, err := NewBinanceAwsConfig(ctx)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(*cfg)
	downloader := manager.NewDownloader(client)

	return &BinanceService{
		bucket:     "data.binance.vision",
		client:     client,
		downloader: downloader,
		db:         db,
		cfg:        cfg,
		ctx:        ctx,
	}, nil
}

// BufferAt is a custom type that implements io.WriterAt
type BufferAt struct {
	mu   sync.Mutex
	data []byte
}

func (b *BufferAt) Read(p []byte) (n int, err error) {
	return 0, nil
}

// WriteAt implements the io.WriterAt interface
func (b *BufferAt) WriteAt(p []byte, off int64) (n int, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Ensure the buffer is large enough
	if int64(len(b.data)) < off+int64(len(p)) {
		newData := make([]byte, off+int64(len(p)))
		copy(newData, b.data)
		b.data = newData
	}
	// Copy the data at the specified offset
	n = copy(b.data[off:], p)
	if n < len(p) {
		return n, fmt.Errorf("incomplete write: wrote %d of %d bytes", n, len(p))
	}
	return n, nil
}

// Bytes return the underlying byte slice
func (b *BufferAt) Bytes() []byte {
	return b.data
}

// decompressZip decompresses ZIP bytes and returns the original content
func decompressZip(compressed []byte) (string, error) {
	// Create a reader for the compressed bytes
	reader := bytes.NewReader(compressed)
	zipReader, err := zip.NewReader(reader, int64(len(compressed)))
	if err != nil {
		return "", fmt.Errorf("failed to create zip reader: %v", err)
	}

	// Assume single file in ZIP for simplicity
	if len(zipReader.File) == 0 {
		return "", fmt.Errorf("no files found in zip")
	}

	// Open the first file in the ZIP
	file, err := zipReader.File[0].Open()
	if err != nil {
		return "", fmt.Errorf("failed to open zip file: %v", err)
	}
	defer file.Close()

	// Read the decompressed content
	var result strings.Builder
	_, err = io.Copy(&result, file)
	if err != nil {
		return "", fmt.Errorf("failed to read decompressed data: %v", err)
	}

	return result.String(), nil
}
func main() {
	const (
		prefix   = "data/spot/monthly/klines/"
		localDir = "./data/spot/monthly/klines/"
	)
	ctx := context.Background()
	srv, err := NewBinanceService(ctx)
	if err != nil {
		log.Fatalf("could not initialize binance service: %s", err.Error())
	}

	paths, err := srv.List(prefix, func(path string, page *s3.ListObjectsV2Output) error {
		if strings.HasSuffix(path, "CHECKSUM") {
			return nil
		}

		if strings.HasSuffix(path, "/") {
			return nil
		}
		//out, in := io.Pipe()
		in := &BufferAt{}
		_, err = srv.Download(path, in)
		if err != nil {
			return err
		}

		csvData, err := decompressZip(in.Bytes())
		if err != nil {
			return err
		}

		// Use duckdb to load csvData into table
		// Example:
		//1758499200000000,0.00096228,0.00719617,0.00096228,0.00486156,250418.32000000,1758585599999999,1113.83552852,5572,141985.82000000,604.43744610,0
		//1758585600000000,0.00479669,0.00730000,0.00461404,0.00565861,68079.64000000,1758671999999999,403.46344821,1621,39911.87000000,231.65703083,0
		_, err = srv.db.Exec(fmt.Sprintf(`
				CREATE TABLE my_table AS
				SELECT * FROM read_csv_auto('/dev/stdin')
		`), csvData)

		return nil
	})
	fmt.Printf("Found %d files\n", len(paths))
}
