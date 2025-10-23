package main

import (
	"archive/zip"
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/gocarina/gocsv"
	"github.com/jszwec/csvutil"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
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

// CSVKline - binance kline data
type CSVKline struct {
	OpenTime   int64   `csv:"0" parquet:"name=open_time, type=INT64, logicaltype=TIME,logicaltype.isadjustedtoutc=true, logicaltype.unit=MICROS"`
	OpenPrice  float64 `csv:"1" parquet:"name=open, type=DOUBLE"`
	HighPrice  float64 `csv:"2" parquet:"name=high, type=DOUBLE"`
	LowPrice   float64 `csv:"3" parquet:"name=low, type=DOUBLE"`
	ClosePrice float64 `csv:"4" parquet:"name=close, type=DOUBLE"`
	Volume     float64 `csv:"5" parquet:"name=volume, type=DOUBLE"`
	CloseTime  int64   `csv:"6" parquet:"name=close_time, type=INT64, logicaltype=TIME,logicaltype.isadjustedtoutc=true, logicaltype.unit=MICROS"`

	QuoteVolume    float64 `csv:"7"`
	NumberOfTrades int64   `csv:"8"`
	TakerBuyVolume float64 `csv:"9"`
	TakerBuyQuote  float64 `csv:"10"`
	Ignore         int64   `csv:"11"`
}

func TransformCSVToParquet(from *csv.Reader, parquetPath string) {
	userHeader, _ := csvutil.Header(CSVKline{}, "csv")
	dec, _ := csvutil.NewDecoder(from, userHeader...)

	// remove file if exists
	os.Remove(parquetPath)

	fw, err := local.NewLocalFileWriter(parquetPath)
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}
	defer fw.Close()
	pw, err := writer.NewParquetWriter(fw, new(CSVKline), 2)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}
	pw.RowGroupSize = 1 * 1024 * 1024 // 1
	// MB
	pw.CompressionType = parquet.CompressionCodec_ZSTD
	defer pw.WriteStop()
	for {
		var u CSVKline
		// Read records from csv
		if err := dec.Decode(&u); err == io.EOF {
			break
		}
		// Write records to parquet
		if err = pw.Write(&u); err != nil {
			log.Println("Write error", err)
		}
	}
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
	localDir   string // Local directory for downloaded files
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

		// Check if a file already exists locally
		_, err := os.Stat(path)
		if err == nil || !os.IsNotExist(err) {
			// Read existing file
			fileContent, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read existing file: %v", err)
			}

			// Write content to BufferAt
			if _, err = in.WriteAt(fileContent, 0); err != nil {
				return fmt.Errorf("failed to write to buffer: %v", err)
			}

			log.Printf("File %s already exists, loaded from disk", path)
		} else {
			// Download file if it doesn't exist
			_, err = srv.Download(path, in)
			if err != nil {
				return err
			}
			// Store a downloaded file into a local directory save it as a zip file
			go func() {
				// Create directory if not exists
				dest := path
				dir := filepath.Dir(dest)
				if err := os.MkdirAll(dir, 0755); err != nil {
					log.Printf("failed to create directory %s: %v", dir, err)
					return
				}
				// Save file
				if err := os.WriteFile(dest, in.Bytes(), 0644); err != nil {
					log.Printf("failed to write file %s: %v", dest, err)
					return
				}
			}()
		}

		// Store CSV as structured data into duckdb hive partitioned table as parquet files
		csvData, err := decompressZip(in.Bytes())
		if err != nil {
			return err
		}

		// Store CSV file parallel to ZIP
		go func() {
			csvPath := strings.TrimSuffix(path, ".zip") + ".csv"

			// Check if CSV already exists
			if _, err := os.Stat(csvPath); err == nil {
				log.Printf("CSV file %s already exists, skipping", csvPath)
				return
			}

			// Create directory if not exists
			csvDir := filepath.Dir(csvPath)
			if err := os.MkdirAll(csvDir, 0755); err != nil {
				log.Printf("failed to create directory for CSV %s: %v", csvDir, err)
				return
			}

			// Save CSV file
			if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
				log.Printf("failed to write CSV file %s: %v", csvPath, err)
				return
			}
		}()

		// Store parquet file parallel to ZIP based on CSV
		go func() {
			parquetPath := strings.TrimSuffix(path, ".zip") + ".parquet"

			// Check if parquet already exists
			if _, err := os.Stat(parquetPath); err == nil {
				log.Printf("Parquet file %s already exists, skipping", parquetPath)
				return
			}

			// Create directory if not exists
			parquetDir := filepath.Dir(parquetPath)
			if err := os.MkdirAll(parquetDir, 0755); err != nil {
				log.Printf("failed to create directory for parquet %s: %v", parquetDir, err)
				return
			}
		}()

		// Add these imports at the top:

		//reader := csv.NewReader(strings.NewReader(csvData))
		var klines []*CSVKline
		err = gocsv.Unmarshal(strings.NewReader(csvData), &klines)
		//
		//for {
		//	record, err := reader.Read()
		//	if err == io.EOF {
		//		break
		//	}
		//	if err != nil {
		//		return fmt.Errorf("error reading CSV: %v", err)
		//	}
		//
		//	// Parse the CSV record into CSVKline struct
		//	kline := CSVKline{}
		//
		//	if err != nil {
		//		return fmt.Errorf("error unmarshalling CSV record: %v", err)
		//	}
		//
		//	// Convert string values to appropriate types
		//	kline.OpenTime, _ = strconv.ParseInt(record[0], 10, 64)
		//	kline.OpenPrice, _ = strconv.ParseFloat(record[1], 64)
		//	kline.HighPrice, _ = strconv.ParseFloat(record[2], 64)
		//	kline.LowPrice, _ = strconv.ParseFloat(record[3], 64)
		//	kline.ClosePrice, _ = strconv.ParseFloat(record[4], 64)
		//	kline.Volume, _ = strconv.ParseFloat(record[5], 64)
		//	kline.CloseTime, _ = strconv.ParseInt(record[6], 10, 64)
		//	kline.QuoteVolume, _ = strconv.ParseFloat(record[7], 64)
		//	kline.NumberOfTrades, _ = strconv.ParseInt(record[8], 10, 64)
		//	kline.TakerBuyVolume, _ = strconv.ParseFloat(record[9], 64)
		//	kline.TakerBuyQuote, _ = strconv.ParseFloat(record[10], 64)
		//	kline.Ignore, _ = strconv.ParseInt(record[11], 10, 64)
		//
		//	klines = append(klines, kline)
		//}

		// Now create the table with proper column names and types
		_, err = srv.db.Exec(`CREATE TABLE IF NOT EXISTS klines
			(
				open_time        BIGINT,
				open_price       DOUBLE,
				high_price       DOUBLE,
				low_price        DOUBLE,
				close_price      DOUBLE,
				volume           DOUBLE,
				close_time       BIGINT,
				quote_volume     DOUBLE,
				number_of_trades BIGINT,
				taker_buy_volume DOUBLE,
				taker_buy_quote  DOUBLE,
				ignore           BIGINT
			)
		`)
		if err != nil {
			return fmt.Errorf("error creating table: %v", err)
		}

		return nil
	})
	fmt.Printf("Found %d files\n", len(paths))
}
