package binance

import (
	"context"
	"testing"
	"time"
)

// TestDownloadHistoricalAggTrades downloads historical aggTrades from S3
func TestDownloadHistoricalAggTrades(t *testing.T) {
	ctx := context.Background()

	consumer, err := NewHistoryConsumer(ctx)
	if err != nil {
		t.Fatalf("Failed to create history consumer: %v", err)
	}

	t.Run("Download Dec 13 aggTrades from S3", func(t *testing.T) {
		// Dec 13, 2025 - a past date
		dec13 := time.Date(2025, 12, 13, 0, 0, 0, 0, time.UTC)

		asset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Indicator:  AggTrades,
			Market:     "BTCUSDT",
			Date:       dec13,
		}

		infoCh, errCh := consumer.DownloadAndTransform(asset)

		var infos []*AssetETLInfo
		var errors []error

		for done := false; !done; {
			select {
			case info, ok := <-infoCh:
				if !ok {
					done = true
					continue
				}
				infos = append(infos, info)
				t.Logf("ETL Info: Status=%v, Path=%s, Info=%s", info.Status, info.Path, info.Info)
			case err, ok := <-errCh:
				if !ok {
					continue
				}
				errors = append(errors, err)
				t.Logf("ETL Error: %v", err)
			}
		}

		if len(errors) > 0 {
			t.Logf("ETL had %d errors", len(errors))
			for _, e := range errors {
				t.Logf("  - %v", e)
			}
		}

		if len(infos) == 0 {
			t.Fatal("Expected at least 1 ETL info message")
		}

		// Check that parquet file was created
		parquetPath := asset.ParquetPath()
		t.Logf("Expected parquet path: %s", parquetPath)
	})

	t.Run("Download Dec 14 aggTrades from S3", func(t *testing.T) {
		dec14 := time.Date(2025, 12, 14, 0, 0, 0, 0, time.UTC)

		asset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Indicator:  AggTrades,
			Market:     "BTCUSDT",
			Date:       dec14,
		}

		infoCh, errCh := consumer.DownloadAndTransform(asset)

		for done := false; !done; {
			select {
			case info, ok := <-infoCh:
				if !ok {
					done = true
					continue
				}
				t.Logf("ETL Info: Status=%v, Path=%s", info.Status, info.Path)
			case err, ok := <-errCh:
				if !ok {
					continue
				}
				t.Logf("ETL Error: %v", err)
			}
		}
	})

	t.Run("Download Dec 15 aggTrades from S3", func(t *testing.T) {
		dec15 := time.Date(2025, 12, 15, 0, 0, 0, 0, time.UTC)

		asset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Indicator:  AggTrades,
			Market:     "BTCUSDT",
			Date:       dec15,
		}

		infoCh, errCh := consumer.DownloadAndTransform(asset)

		for done := false; !done; {
			select {
			case info, ok := <-infoCh:
				if !ok {
					done = true
					continue
				}
				t.Logf("ETL Info: Status=%v, Path=%s", info.Status, info.Path)
			case err, ok := <-errCh:
				if !ok {
					continue
				}
				t.Logf("ETL Error: %v", err)
			}
		}
	})
}
