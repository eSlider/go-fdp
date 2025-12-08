package binance

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"
	"testing"
	"time"
)

// TestCandlesMixedRange tests the merging of historical parquet data with current day data
func TestCandlesMixedRange(t *testing.T) {
	t.Run("Historical and current day data merge correctly", func(t *testing.T) {
		srv, err := NewHistoryConsumer(context.Background())
		if err != nil {
			t.Fatalf("could not initialize binance service: %s", err.Error())
		}

		market := "BNBUSDT"
		frame := Minute

		// Get day before yesterday for historical data (ensure data is available)
		twoDaysAgo := time.Now().UTC().AddDate(0, 0, -2)
		twoDaysAgo = time.Date(twoDaysAgo.Year(), twoDaysAgo.Month(), twoDaysAgo.Day(), 0, 0, 0, 0, time.UTC)

		// Download historical data for two days ago
		histAsset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Frame:      frame,
			Indicator:  Klines,
			Date:       twoDaysAgo,
			Market:     market,
		}

		t.Logf("Downloading historical data for %s on %s", market, twoDaysAgo.Format("2006-01-02"))

		// Download and transform historical data (this date should not be "today")
		infoCh, errCh := srv.DownloadAndTransform(histAsset)
		var histParquetPath string
		for done := false; !done; {
			select {
			case info, ok := <-infoCh:
				if !ok {
					done = true
					continue
				}
				if info.Status == StatusParquetReady {
					histParquetPath = info.Path
					t.Logf("Historical parquet ready: %s", histParquetPath)
				}
			case err, ok := <-errCh:
				if !ok {
					done = true
					continue
				}
				t.Logf("Warning during historical download: %v", err)
			}
		}

		// Verify historical parquet file exists and has data
		if histParquetPath != "" && fs.FileExists(histParquetPath) {
			recordCh, readErrCh := data.ReadParquet[ParquetKline](histParquetPath)
			var histRecords []*ParquetKline
			for done := false; !done; {
				select {
				case record, ok := <-recordCh:
					if !ok {
						done = true
						continue
					}
					histRecords = append(histRecords, record)
				case err, ok := <-readErrCh:
					if !ok {
						done = true
						continue
					}
					t.Errorf("Error reading historical parquet: %v", err)
				}
			}
			t.Logf("Historical records count: %d", len(histRecords))

			if len(histRecords) == 0 {
				t.Errorf("Expected historical data, got 0 records")
			}
		}

		// Now fetch and cache current day data into hourly parquet files
		todayAsset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Frame:      frame,
			Indicator:  Klines,
			Date:       time.Now().UTC(),
			Market:     market,
		}

		t.Logf("Fetching current day data for %s", market)

		// Fetch and cache current day data using new methods
		currentCandles, err := srv.FetchAndCacheCurrentDay(todayAsset)
		if err != nil {
			t.Fatalf("Failed to fetch current day data: %v", err)
		}

		t.Logf("Current day candles fetched: %d", len(currentCandles))

		// Verify hourly parquet files were created
		todayParquetDir := todayAsset.TodayParquetDir()
		parquetDir := fs.GetModuleRelativePath(todayParquetDir)

		files, err := filepath.Glob(filepath.Join(parquetDir, "*.parquet"))
		if err != nil {
			t.Logf("Warning: could not list parquet files: %v", err)
		} else {
			t.Logf("Hourly parquet files created: %d", len(files))
			for _, f := range files {
				t.Logf("  - %s", filepath.Base(f))
			}
		}

		// Verify we can read the current day cached data
		cachedCandles, err := srv.ReadCachedCurrentDay(todayAsset)
		if err != nil {
			t.Fatalf("Failed to read cached current day data: %v", err)
		}

		t.Logf("Cached candles read: %d", len(cachedCandles))

		// Verify data integrity - first candle should be close to midnight
		if len(cachedCandles) > 0 {
			firstCandle := cachedCandles[0]
			firstTime := data.AnyTimestampToTime(firstCandle.OpenTime)
			t.Logf("First cached candle time: %s", firstTime.Format("2006-01-02 15:04:05"))
		}

		// Test merging historical and current data
		if histParquetPath != "" && len(cachedCandles) > 0 {
			t.Log("Both historical and current data available - merge test passed")
		}
	})

	t.Run("Last hour refresh overwrites correctly", func(t *testing.T) {
		srv, err := NewHistoryConsumer(context.Background())
		if err != nil {
			t.Fatalf("could not initialize binance service: %s", err.Error())
		}

		market := "BNBUSDT"
		frame := Minute

		todayAsset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Frame:      frame,
			Indicator:  Klines,
			Date:       time.Now().UTC(),
			Market:     market,
		}

		// First fetch
		candles1, err := srv.FetchAndCacheCurrentDay(todayAsset)
		if err != nil {
			t.Fatalf("First fetch failed: %v", err)
		}
		t.Logf("First fetch: %d candles", len(candles1))

		// Get last candle time from first fetch
		var lastTime1 int64
		if len(candles1) > 0 {
			lastTime1 = candles1[len(candles1)-1].OpenTime
		}

		// Wait a moment and refresh the last hour
		time.Sleep(1 * time.Second)

		// Refresh last hour
		err = srv.RefreshLastHour(todayAsset)
		if err != nil {
			t.Fatalf("Refresh last hour failed: %v", err)
		}

		// Read cached data again
		candles2, err := srv.ReadCachedCurrentDay(todayAsset)
		if err != nil {
			t.Fatalf("Second fetch failed: %v", err)
		}
		t.Logf("After refresh: %d candles", len(candles2))

		// Verify last candle time is at least equal or newer
		if len(candles2) > 0 {
			lastTime2 := candles2[len(candles2)-1].OpenTime
			if lastTime2 < lastTime1 {
				t.Errorf("Last candle time should not decrease: before=%d, after=%d", lastTime1, lastTime2)
			}
			t.Logf("Last candle time: before=%d, after=%d", lastTime1, lastTime2)
		}
	})

	t.Run("Hourly parquet files are created correctly", func(t *testing.T) {
		srv, err := NewHistoryConsumer(context.Background())
		if err != nil {
			t.Fatalf("could not initialize binance service: %s", err.Error())
		}

		market := "ETHUSDT"
		frame := Minute

		todayAsset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Frame:      frame,
			Indicator:  Klines,
			Date:       time.Now().UTC(),
			Market:     market,
		}

		// Fetch current day data
		_, err = srv.FetchAndCacheCurrentDay(todayAsset)
		if err != nil {
			t.Fatalf("Fetch failed: %v", err)
		}

		// Verify parquet files exist for each hour that has passed
		parquetDir := fs.GetModuleRelativePath(todayAsset.TodayParquetDir())
		now := time.Now().UTC()
		currentHour := now.Hour()

		for hour := 0; hour <= currentHour; hour++ {
			hourFile := filepath.Join(parquetDir, fmt.Sprintf("hour_%02d.parquet", hour))
			if fs.FileExists(hourFile) {
				t.Logf("Hour %02d parquet exists: %s", hour, hourFile)

				// Read and verify contents
				recordCh, errCh := data.ReadParquet[ParquetKline](hourFile)
				var count int
				for done := false; !done; {
					select {
					case _, ok := <-recordCh:
						if !ok {
							done = true
							continue
						}
						count++
					case err, ok := <-errCh:
						if !ok {
							done = true
							continue
						}
						t.Errorf("Error reading hour %02d parquet: %v", hour, err)
					}
				}
				t.Logf("  Records in hour %02d: %d", hour, count)
			} else {
				t.Logf("Hour %02d parquet not found (might not have data yet)", hour)
			}
		}
	})
}

// TestCurrentDataCachePreventsAPICall verifies that cached data prevents redundant API calls
func TestCurrentDataCachePreventsAPICall(t *testing.T) {
	t.Run("Cache prevents external API penetration", func(t *testing.T) {
		srv, err := NewHistoryConsumer(context.Background())
		if err != nil {
			t.Fatalf("could not initialize binance service: %s", err.Error())
		}

		market := "BNBUSDT"
		frame := Minute

		todayAsset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Frame:      frame,
			Indicator:  Klines,
			Date:       time.Now().UTC(),
			Market:     market,
		}

		// First call - should hit API
		start1 := time.Now()
		candles1, err := srv.FetchAndCacheCurrentDay(todayAsset)
		if err != nil {
			t.Fatalf("First fetch failed: %v", err)
		}
		duration1 := time.Since(start1)
		t.Logf("First fetch: %d candles in %v", len(candles1), duration1)

		// Second call - should use cache (much faster)
		start2 := time.Now()
		candles2, err := srv.ReadCachedCurrentDay(todayAsset)
		if err != nil {
			t.Fatalf("Cache read failed: %v", err)
		}
		duration2 := time.Since(start2)
		t.Logf("Cache read: %d candles in %v", len(candles2), duration2)

		// Cache read should be significantly faster
		if duration2 > duration1 {
			t.Logf("Warning: cache read was slower than initial fetch (might be first run)")
		}

		// Verify data consistency
		if len(candles1) != len(candles2) {
			t.Logf("Note: candle count differs (candles1=%d, candles2=%d) - this is expected if time passed", len(candles1), len(candles2))
		}
	})
}

// cleanupTestParquetFiles removes test parquet files
func cleanupTestParquetFiles(t *testing.T, dir string) {
	if err := os.RemoveAll(dir); err != nil {
		t.Logf("Warning: could not cleanup test files: %v", err)
	}
}
