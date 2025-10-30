package main

import (
	"context"
	"errors"
	"fmt"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"
	"testing"
	"time"
)

func TestTimeBetween(t *testing.T) {
	start := *data.AnyTimestampToTime(1596327180)
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
			Market:     "ETHUSDT",
		}

		// Download and transform (this should create or reuse parquet file)
		infoCh, errCh := srv.DownloadAndTransform(asset)

		// Collect results
		var infos []*binance.AssetETLInfo
		var errs []error

		go func() {
			for {
				select {
				case info, ok := <-infoCh:
					if !ok {
						return
					}
					infos = append(infos, info)
				}
			}
		}()

		for err := range errCh {
			errs = append(errs, err)
		}

		// Wait for completion

		// Check for errors
		if len(errs) > 0 {
			// If file already exists, that's ok for this test
			hasNonFileExistsError := false
			for _, err := range errs {
				if !errors.Is(err, data.ErrFileExists) {
					hasNonFileExistsError = true
					break
				}
			}
			if hasNonFileExistsError {
				t.Fatalf("unexpected errors during ETL: %v", errs)
			}
		}

		// Find the parquet file path from the results
		var parquetPath string
		for _, info := range infos {
			if info.Status == binance.StatusParquetDone {
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
		recordCh, readErrCh := data.ReadParquet[binance.ParquetKline](parquetPath)

		var records []*binance.ParquetKline
		readDone := make(chan bool)

		go func() {
			defer close(readDone)
			for record := range recordCh {
				records = append(records, record)
			}
		}()

		// Wait for reading to complete
		<-readDone

		// Check for read errors
		var readErrs []error
		for err := range readErrCh {
			readErrs = append(readErrs, err)
		}

		if len(readErrs) > 0 {
			t.Fatalf("failed to read parquet file: %v", readErrs)
		}

		// Verify we have records
		if len(records) == 0 {
			t.Fatal("parquet file contains no records")
		}

		// For 2017-08 ETHUSDT 1m data, we expect approximately 31 days * 24 hours * 60 minutes = 44,640 records
		// But actual data might be less due to market hours. The CSV has 21,275 lines including header,
		// so we expect 21,275 data records (CSV includes header)
		expectedMinRecords := 21000 // Allow some tolerance
		expectedMaxRecords := 45000 // Allow some tolerance

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
		// Initialize binance service
		srv, err := binance.NewHistoryConsumer(context.Background())
		if err != nil {
			t.Fatalf("could not initialize binance service: %s", err.Error())
		}

		// Create asset configuration for aggTrades (smaller dataset typically)
		asset := &binance.HistoryAsset{
			MarketType: binance.Spot,
			Frequency:  binance.Monthly,
			Indicator:  binance.AggTrades,
			Date:       time.Date(2017, 8, 1, 0, 0, 0, 0, time.UTC),
			Market:     "ETHUSDT",
		}

		// Download and transform
		infoCh, errCh := srv.DownloadAndTransform(asset)

		// Collect results
		var infos []*binance.AssetETLInfo
		var errs []error
		done := make(chan bool)

		go func() {
			defer close(done)
			for {
				select {
				case info, ok := <-infoCh:
					if !ok {
						return
					}
					infos = append(infos, info)
				case err, ok := <-errCh:
					if !ok {
						return
					}
					errs = append(errs, err)
				}
			}
		}()

		// Wait for completion
		<-done

		// Check for errors (allow file exists)
		if len(errs) > 0 {
			hasNonFileExistsError := false
			for _, err := range errs {
				if !errors.Is(err, data.ErrFileExists) {
					hasNonFileExistsError = true
					break
				}
			}
			if hasNonFileExistsError {
				t.Fatalf("unexpected errors during ETL: %v", errs)
			}
		}

		// Find the parquet file path
		var parquetPath string
		for _, info := range infos {
			if info.Status == binance.StatusParquetDone {
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
		recordCh, readErrCh := data.ReadParquet[binance.ParquetAggTrade](parquetPath)

		var records []*binance.ParquetAggTrade
		readDone := make(chan bool)

		go func() {
			defer close(readDone)
			for record := range recordCh {
				records = append(records, record)
			}
		}()

		// Wait for reading to complete
		<-readDone

		// Check for read errors
		var readErrs []error
		for err := range readErrCh {
			readErrs = append(readErrs, err)
		}

		if len(readErrs) > 0 {
			t.Fatalf("failed to read parquet file: %v", readErrs)
		}

		// Verify we have records
		if len(records) == 0 {
			t.Fatal("parquet file contains no records")
		}

		// AggTrades typically have many more records than klines
		// For a month, expect hundreds of thousands to millions of records
		expectedMinRecords := 10000 // Conservative minimum

		if len(records) < expectedMinRecords {
			t.Errorf("expected at least %d aggTrades records, got %d", expectedMinRecords, len(records))
		}

		// Verify record structure (check a few records have valid data)
		for i, record := range records {
			if i >= 5 { // Check first 5 records
				break
			}
			if record.Timestamp == 0 {
				t.Errorf("record %d has invalid timestamp: %d", i, record.Timestamp)
			}
			// Note: AggTradeID, FirstTradeID, LastTradeID can be 0 for the first trades
			if record.AggTradeID < 0 {
				t.Errorf("record %d has invalid agg_trade_id: %d", i, record.AggTradeID)
			}
			if record.FirstTradeID < 0 {
				t.Errorf("record %d has invalid first_trade_id: %d", i, record.FirstTradeID)
			}
			if record.LastTradeID < 0 {
				t.Errorf("record %d has invalid last_trade_id: %d", i, record.LastTradeID)
			}
			if record.Price <= 0 {
				t.Errorf("record %d has invalid price: %f", i, record.Price)
			}
			if record.Quantity <= 0 {
				t.Errorf("record %d has invalid quantity: %f", i, record.Quantity)
			}
		}

		t.Logf("Successfully read %d aggTrades records from parquet file: %s", len(records), parquetPath)
	})
}
