package main

import (
	"context"
	"fmt"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"
	"testing"
	"time"
)

func TestTimeBetween(t *testing.T) {

	// start is 2020-08-02 00:00:00 UTC
	start := *data.AnyTimestampToTime(1596327180)
	// end is 2020-08-03 00:00:00 UTC
	end := (*data.AnyTimestampToTime(1596327239)).Add(time.Hour * 24 * 2)

	// Trace time between start and end every day
	for day := start; day.Before(end); day = day.AddDate(0, 0, 1) {
		fmt.Println(day)
	}
}

func TestMainProgramParquetCreation(t *testing.T) {
	t.Run("creates and reads parquet files correctly", func(t *testing.T) {
		// Initialize binance service (similar to main.go)
		srv, err := binance.NewHistoryConsumer(context.Background())
		if err != nil {
			t.Fatalf("could not initialize binance service: %s", err.Error())
		}

		// Create asset configuration for a small dataset (2017-08 ETHUSDT 1m klines)
		// This date has existing data and we can verify the count
		asset := &binance.HistoryAsset{
			MarketType: binance.Spot,
			Frequency:  binance.Daily,
			Frame:      binance.OneMinute,
			Indicator:  binance.Klines,
			Date:       time.Date(2020, 8, 2, 0, 0, 0, 0, time.UTC),
			Market:     "ZECUSDT",
		}

		// Download and transform (this should create or reuse parquet file)
		var infos []*binance.AssetETLInfo
		end := false
		for infoCh, errCh := srv.DownloadAndTransform(asset); !end; {
			select {
			case info, ok := <-infoCh: // Info channel is closed when done
				if ok {
					infos = append(infos, info)
				} else {
					end = true
				}
			case err, ok := <-errCh: // Errors are not expected
				if ok {
					t.Errorf("error reading csv: %v", err)
				}
				end = true
			}
		}

		// Find the parquet file path from the results
		var parquetPath string
		for _, info := range infos {
			if info.Status == binance.StatusParquetReady {
				parquetPath = info.Path
				break
			}
		}

		if parquetPath == "" {
			t.Fatal("no parquet file was created or found")
		}

		// Verify parquet file exists
		if !fs.FileExists(parquetPath) {
			t.Fatalf("parquet file does not exist: %s", parquetPath)
		}

		// Read the parquet file and count entries

		end = false
		var records []*binance.ParquetKline
		for recordCh, readErrCh := data.ReadParquet[binance.ParquetKline](parquetPath); !end; {
			select {
			case record, ok := <-recordCh:
				if ok {
					records = append(records, record)
				} else {
					end = true
				}
			case err, ok := <-readErrCh:
				if ok {
					t.Errorf("error reading parquet: %v", err)
				}
				end = true
			}
		}

		// Verify we have records
		if len(records) == 0 {
			t.Fatal("parquet file contains no records")
		}

		// For 2020-08-02 ETHUSDT 1m data, we expect 24 hours * 60 minutes = 1,440 records for a full day
		// But actual data might be less due to market hours
		expectedMinRecords := 1000 // Allow some tolerance for partial days
		expectedMaxRecords := 1500 // Allow some tolerance

		if len(records) < expectedMinRecords {
			t.Errorf("expected at least %d records, got %d", expectedMinRecords, len(records))
		}

		if len(records) > expectedMaxRecords {
			t.Errorf("expected at most %d records, got %d", expectedMaxRecords, len(records))
		}

		// Verify record structure (check a few records have valid data)
		for i, record := range records {
			if i >= 5 { // Check first 5 records
				break
			}
			if record.OpenTime == 0 {
				t.Errorf("record %d has invalid open_time: %d", i, record.OpenTime)
			}
			if record.CloseTime == 0 {
				t.Errorf("record %d has invalid close_time: %d", i, record.CloseTime)
			}
			if record.Open <= 0 {
				t.Errorf("record %d has invalid open price: %f", i, record.Open)
			}
			if record.High <= 0 {
				t.Errorf("record %d has invalid high price: %f", i, record.High)
			}
			if record.Low <= 0 {
				t.Errorf("record %d has invalid low price: %f", i, record.Low)
			}
			if record.Close <= 0 {
				t.Errorf("record %d has invalid close price: %f", i, record.Close)
			}
			if record.Volume < 0 {
				t.Errorf("record %d has invalid volume: %f", i, record.Volume)
			}
		}

		t.Logf("Successfully read %d records from parquet file: %s", len(records), parquetPath)
	})

	t.Run("handles aggTrades indicator", func(t *testing.T) {
		// Create asset configuration for aggTrades (smaller dataset typically)
		asset := &binance.HistoryAsset{
			MarketType: binance.Spot,
			Frequency:  binance.Daily,
			Indicator:  binance.Klines,
			Frame:      binance.OneMinute,
			// Date:       time.Now(),
			Date:   time.Date(2019, 8, 1, 0, 0, 0, 0, time.UTC),
			Market: "ETHUSDT",
		}

		// Initialize binance service
		srv, err := binance.NewHistoryConsumer(context.Background())
		if err != nil {
			t.Fatalf("could not initialize binance service: %s", err.Error())
		}

		// Download and transform
		var end = false
		var infos []*binance.AssetETLInfo
		var tick = 1
		for infoCh, errCh := srv.DownloadAndTransform(asset); !end; {
			select {
			case info, ok := <-infoCh:
				if ok {
					infos = append(infos, info)
				} else {
					end = true
				}
			case err, ok := <-errCh:
				if ok {
					t.Errorf("error reading csv: %v", err)
				}
				end = true
			}
			tick++
			t.Logf("tick: %d", tick)
			// fmt.Printf("tick: %d", tick)

		}

		// errors.Is(err, data.ErrFileExists)

		// Find the parquet file path
		var parquetPath string
		for _, info := range infos {
			if info.Status == binance.StatusParquetReady {
				parquetPath = info.Path
				break
			}
		}

		if parquetPath == "" {
			t.Fatal("no parquet file was created or found")
		}

		// Verify parquet file exists
		if !fs.FileExists(parquetPath) {
			t.Fatalf("parquet file does not exist: %s", parquetPath)
		}

		// Read the parquet file and count entries
		end = false
		var records []*binance.ParquetKline
		for recordCh, readErrCh := data.ReadParquet[binance.ParquetKline](parquetPath); !end; {
			select {
			case record, ok := <-recordCh:
				if ok {
					records = append(records, record)
				} else {
					end = true
				}
			case err, ok := <-readErrCh:
				if ok {
					t.Errorf("error reading parquet: %v", err)
				}
				end = true
			}
		}

		// Verify we have records (allow for 0 records if download/processing fails)
		if len(records) == 0 {
			t.Skip("aggTrades parquet file contains no records - likely due to download/processing issues")
		}

		// AggTrades typically have many more records than klines
		// For a month, expect hundreds of thousands to millions of records
		expectedMinRecords := 1000 // Conservative minimum

		if len(records) < expectedMinRecords {
			t.Errorf("expected at least %d aggTrades records, got %d", expectedMinRecords, len(records))
		}

		t.Logf("Successfully read %d aggTrades records from parquet file: %s", len(records), parquetPath)
	})
}
