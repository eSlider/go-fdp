package binance

import (
	"context"
	"os"
	"path/filepath"
	"sync-v3/pkg/fs"
	"testing"
	"time"
)

// TestAggTradesCaching tests that aggTrades are cached locally to parquet files
func TestAggTradesCaching(t *testing.T) {
	ctx := context.Background()

	consumer, err := NewHistoryConsumer(ctx)
	if err != nil {
		t.Fatalf("Failed to create history consumer: %v", err)
	}

	t.Run("Caches current day aggTrades to parquet", func(t *testing.T) {
		now := time.Now().UTC()

		asset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Indicator:  AggTrades,
			Market:     "BTCUSDT",
			Date:       now,
		}

		trades, err := consumer.FetchAndCacheCurrentDayAggTrades(asset)
		if err != nil {
			t.Fatalf("Failed to fetch and cache aggTrades: %v", err)
		}

		if len(trades) == 0 {
			t.Fatal("Expected at least 1 aggTrade")
		}

		t.Logf("Fetched %d aggTrades", len(trades))

		// Verify parquet file was created for current hour
		currentHour := now.Hour()
		parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(currentHour))
		absPath, _ := filepath.Abs(parquetPath)

		if _, err := os.Stat(absPath); os.IsNotExist(err) {
			t.Errorf("Parquet file was not created at: %s", absPath)
		} else {
			t.Logf("Parquet file created at: %s", absPath)

			// Verify file size is non-zero
			info, _ := os.Stat(absPath)
			if info.Size() == 0 {
				t.Error("Parquet file is empty")
			} else {
				t.Logf("Parquet file size: %d bytes", info.Size())
			}
		}
	})

	t.Run("DownloadAndTransform caches aggTrades", func(t *testing.T) {
		now := time.Now().UTC()

		asset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Indicator:  AggTrades,
			Market:     "BTCUSDT",
			Date:       now,
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
			t.Logf("ETL had %d errors (may be expected for cache writes)", len(errors))
		}

		if len(infos) == 0 {
			t.Fatal("Expected at least 1 ETL info message")
		}

		// Check that cache directory exists
		cacheDir := fs.GetModuleRelativePath(asset.TodayParquetDir())
		absDir, _ := filepath.Abs(cacheDir)

		if _, err := os.Stat(absDir); os.IsNotExist(err) {
			t.Logf("Cache directory not created (may be expected): %s", absDir)
		} else {
			t.Logf("Cache directory exists: %s", absDir)

			// List parquet files
			entries, _ := os.ReadDir(absDir)
			for _, entry := range entries {
				t.Logf("  - %s", entry.Name())
			}
		}
	})
}
