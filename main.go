package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// main - Init
func main() {
	//prefix := "data/spot/monthly/klines/ETHUSDT/"

	asset := binance.HistoryAsset{
		MarketType: binance.Spot,
		Frequency:  binance.Daily,
		Frame:      binance.OneMinute,
		Indicator:  binance.AggTrades,
		Date:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		Market:     "ETHUSDT",
	}
	prefix := asset.SymbolFrameLink()

	ctx := context.Background()
	srv, err := binance.NewHistoryConsumer(ctx)
	if err != nil {
		log.Fatalf("could not initialize binance service: %s", err.Error())
	}

	var wg sync.WaitGroup

	paths, err := srv.List(prefix, func(path string, page *s3.ListObjectsV2Output) error {
		wg.Add(1)
		defer wg.Done()

		if strings.HasSuffix(path, "CHECKSUM") {
			return nil
		}

		if strings.HasSuffix(path, "/") {
			return nil
		}

		//out, in := io.Pipe()
		zipBuffer := &data.Buffer{}

		// Check if a file already exists locally
		inf, err := os.Stat(path)
		isFileExists := inf != nil && inf.Size() > 0
		if err != nil && os.IsNotExist(err) {
			return err
		}

		// Check if file is empty
		if inf.Size() < 1 {
			// Remove file
			os.Remove(path)
			isFileExists = false
		}

		if isFileExists {
			// ReadCSV existing file
			fileContent, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read existing file: %v", err)
			}

			// Write content to Buffer
			if _, err = zipBuffer.Write(fileContent); err != nil {
				return fmt.Errorf("failed to write to buffer: %v", err)
			}

			log.Printf("File %s already exists, loaded from disk", path)
		} else {
			// Download the file if it doesn't exist
			_, err = srv.Download(path, zipBuffer)
			if err != nil {
				return err
			}
			// Store a downloaded file into a local directory save it as a zip file
			go func() {
				// Create a directory if not exists
				dest := path
				dir := filepath.Dir(dest)
				if err := os.MkdirAll(dir, 0755); err != nil {
					log.Printf("failed to create directory %s: %v", dir, err)
					return
				}
				// Save file
				if err := os.WriteFile(dest, zipBuffer.Bytes(), 0644); err != nil {
					log.Printf("failed to write file %s: %v", dest, err)
					return
				}
			}()
		}

		// Store CSV as structured data into duckdb hive partitioned table as parquet files
		csvData, err := data.Decompress(zipBuffer.Bytes())
		if err != nil {
			return err
		}

		// Store CSV file parallel to ZIP
		go func() {
			wg.Add(1)
			defer wg.Done()

			csvPath := strings.TrimSuffix(path, ".zip") + ".csv"

			// Check if CSV already exists
			if _, err := os.Stat(csvPath); err == nil {
				log.Printf("CSV file %s already exists, skipping", csvPath)
				return
			}

			// Create a directory if not exists
			csvDir := filepath.Dir(csvPath)
			if err := os.MkdirAll(csvDir, 0755); err != nil {
				log.Printf("failed to create directory for CSV %s: %v", csvDir, err)
				return
			}

			// Save CSV file
			if err := os.WriteFile(csvPath, csvData.Bytes(), 0644); err != nil {
				log.Printf("failed to write CSV file %s: %v", csvPath, err)
				return
			}
		}()

		if asset.Indicator == binance.Klines {
			var klines []binance.Kline
			err = data.ReadCSV[binance.Kline](csvData, func(k binance.Kline) error {
				klines = append(klines, k)
				return nil
			})

			// Store parquet file parallel to ZIP based on CSV
			go func() {
				wg.Add(1)
				defer wg.Done()

				parquetPath := strings.TrimSuffix(path, ".zip") + ".parquet"

				// Check if parquet already exists
				if _, err := os.Stat(parquetPath); err == nil {
					log.Printf("Parquet file %s already exists, skipping", parquetPath)
					return
				}

				// Create a directory if not exists
				parquetDir := filepath.Dir(parquetPath)
				if err := os.MkdirAll(parquetDir, 0755); err != nil {
					log.Printf("failed to create directory for parquet %s: %v", parquetDir, err)
					return
				}

				fw, err := local.NewLocalFileWriter(parquetPath)
				if err != nil {
					log.Println("Can't create local file", err)
					return
				}
				defer fw.Close()
				pw, err := writer.NewParquetWriter(fw, new(binance.ParquetKline), 2)
				if err != nil {
					log.Println("Can't create parquet writer", err)
					return
				}
				// len(csvData.Bytes())/6
				pw.RowGroupSize = 1 * 1024 * 1024 // 1
				pw.PageSize = 8 * 1024            // default 8K

				// Recaluclate row group size and page size by number of rows and binance kline size

				pw.CompressionType = parquet.CompressionCodec_ZSTD
				defer pw.WriteStop()
				for _, kline := range klines {
					// Write records to parquet
					bkl := binance.NewParquetKline(&kline)
					if err = pw.Write(bkl); err != nil {
						log.Println("Write error", err)
					}
				}
			}()
		} else if asset.Indicator == binance.AggTrades {
			var aggTrades []binance.AggTrade
			err = data.ReadCSV[binance.AggTrade](csvData, func(a binance.AggTrade) error {
				aggTrades = append(aggTrades, a)
				return nil
			})
			// Store parquet file parallel to ZIP based on CSV
			go func() {
				wg.Add(1)
				defer wg.Done()

				parquetPath := strings.TrimSuffix(path, ".zip") + ".parquet"

				// Check if parquet already exists
				inf, err := os.Stat(parquetPath)
				if err == nil {
					// Check if file is empty
					if inf.Size() < 5 {
						os.Remove(parquetPath)
					} else {
						log.Printf("Parquet file %s already exists, skipping", parquetPath)
						return
					}
				}

				// Create a directory if not exists
				parquetDir := filepath.Dir(parquetPath)
				if err := os.MkdirAll(parquetDir, 0755); err != nil {
					log.Printf("failed to create directory for parquet %s: %v", parquetDir, err)
					return
				}

				fw, err := local.NewLocalFileWriter(parquetPath)
				if err != nil {
					log.Println("Can't create local file", err)
					return
				}
				defer fw.Close()
				pw, err := writer.NewParquetWriter(fw, new(binance.ParquetAggTrade), 2)
				if err != nil {
					log.Println("Can't create parquet writer", err)
					return
				}
				pw.RowGroupSize = 1 * 1024 * 1024 // 8MB
				pw.PageSize = 8 * 1024            // default 8K

				// Recaluclate row group size and page size by number of rows and binance kline size

				pw.CompressionType = parquet.CompressionCodec_ZSTD
				defer pw.WriteStop()

				var count int = 0
				var errors int = 0
				for _, aggTrade := range aggTrades {
					// Write records to parquet
					bkl := binance.NewParquetAggTrade(&aggTrade)
					if bkl == nil {
						continue
					}
					if err = pw.Write(bkl); err != nil {
						log.Println("Write error", err)
						errors++
					} else {
						count++
					}
				}

				fmt.Printf("Saved %d records, %d errors\n", count, errors)
			}()
		}

		if err != nil {
			return fmt.Errorf("error creating table: %v", err)
		}

		return nil
	})
	wg.Wait()

	time.Sleep(10 * time.Second)
	fmt.Printf("Found %d files\n", len(paths))
}
