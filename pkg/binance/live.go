package binance

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/eslider/go-binance-fdp/pkg/fs"
	"github.com/google/uuid"
)

// FetchAndCacheCurrentDay fetches current day data from API and caches into hourly parquet files
func (s *HistoryConsumer) FetchAndCacheCurrentDay(asset *HistoryAsset) ([]*Kline, error) {
	now := time.Now().UTC()
	midnight := s.midnightUTC(now)
	currentHour := now.Hour()

	var allCandles []*Kline
	for hour := 0; hour <= currentHour; hour++ {
		if hour == currentHour {
			if err := s.RefreshLastHour(asset); err != nil {
				return nil, fmt.Errorf("failed to refresh current hour: %w", err)
			}
			parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(hour))
			candles, err := s.readHourlyParquet(parquetPath, midnight)
			if err != nil {
				return nil, fmt.Errorf("failed to read current hour %d parquet: %w", hour, err)
			}
			allCandles = append(allCandles, candles...)
			continue
		}

		candles, err := s.loadOrSealHour(asset, hour, midnight)
		if err != nil {
			return nil, fmt.Errorf("hour %d: %w", hour, err)
		}
		allCandles = append(allCandles, candles...)
	}

	return allCandles, nil
}

// FetchAndCacheCurrentDayAggTrades fetches current day aggTrades from API and caches into hourly parquet files
// Returns data even if caching fails (logs warnings for cache errors)
func (s *HistoryConsumer) FetchAndCacheCurrentDayAggTrades(asset *HistoryAsset) ([]*AggTrade, error) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	var allTrades []*AggTrade

	// Process each completed hour
	currentHour := now.Hour()
	for hour := 0; hour <= currentHour; hour++ {
		hourStart := midnight.Add(time.Duration(hour) * time.Hour)
		hourEnd := hourStart.Add(time.Hour)

		// For the current hour, use current time as end
		isCurrentHour := hour == currentHour
		if isCurrentHour {
			hourEnd = now
		}

		parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(hour))

		// Check if we already have cached data for completed hours
		if !isCurrentHour && fs.FileExists(parquetPath) {
			// Read from cache for completed hours
			trades, err := s.readHourlyAggTradesParquet(parquetPath, midnight)
			if err == nil && len(trades) > 0 {
				allTrades = append(allTrades, (trades)...)
				continue
			}
		}

		if isCurrentHour {
			// Refresh last hour - fetch directly from API
			trades, err := s.fetchCurrentHourAggTrades(asset)
			if err != nil {
				fmt.Printf("Warning: failed to fetch current hour aggTrades: %v\n", err)
			} else {
				allTrades = append(allTrades, trades...)
				// Try to cache (best effort, don't fail on error)
				if err := s.writeHourlyAggTradesParquet(parquetPath, trades); err != nil {
					fmt.Printf("Warning: failed to cache current hour aggTrades: %v\n", err)
				}
			}
			continue
		}

		// Fetch from API for completed hours (if not cached)
		trades, err := s.fetchHourAggTradesData(asset, hourStart, hourEnd)
		if err != nil {
			fmt.Printf("Warning: failed to fetch hour %d aggTrades: %v\n", hour, err)
			continue // Continue with other hours
		}

		if len(trades) > 0 {
			allTrades = append(allTrades, trades...)
			// Try to cache (best effort, don't fail on error)
			if err := s.writeHourlyAggTradesParquet(parquetPath, trades); err != nil {
				fmt.Printf("Warning: failed to cache hour %d aggTrades: %v\n", hour, err)
			}
		}
	}

	return allTrades, nil
}

// fetchCurrentHourAggTrades fetches aggTrades for the current hour from API
func (s *HistoryConsumer) fetchCurrentHourAggTrades(asset *HistoryAsset) ([]*AggTrade, error) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	currentHour := now.Hour()
	hourStart := midnight.Add(time.Duration(currentHour) * time.Hour)

	return s.fetchHourAggTradesData(asset, hourStart, now)
}

// RefreshLastHourAggTrades refreshes the last hour's aggTrades parquet file by fetching new data from API
func (s *HistoryConsumer) RefreshLastHourAggTrades(asset *HistoryAsset) error {
	now := time.Now().UTC()
	currentHour := now.Hour()
	parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(currentHour))

	// Read last trade ID from existing file to avoid re-fetching all data
	var lastTradeID int64 = 0
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	hourStart := midnight.Add(time.Duration(currentHour) * time.Hour)

	if fs.FileExists(parquetPath) {
		trades, err := s.readHourlyAggTradesParquet(parquetPath, midnight)
		if err == nil && len(trades) > 0 {
			// Get the last trade to continue from
			lastTradeID = trades[len(trades)-1].AggTradeID
		}
	}

	// Fetch new data
	var newTrades []*AggTrade
	var err error

	if lastTradeID > 0 {
		// Fetch from last trade ID
		newTrades, err = s.fetchAggTradesFromID(asset, lastTradeID+1)
	} else {
		// Fetch from hour start
		newTrades, err = s.fetchHourAggTradesData(asset, hourStart, now)
	}

	if err != nil {
		return fmt.Errorf("failed to fetch new aggTrades: %w", err)
	}

	if len(newTrades) == 0 {
		return nil // No new data
	}

	// Read existing trades and merge
	var existingTrades []*AggTrade
	if fs.FileExists(parquetPath) {
		existingTrades, _ = s.readHourlyAggTradesParquet(parquetPath, midnight)
	}

	// Merge: keep existing trades that are not in new data
	merged := make(map[int64]*AggTrade)
	for _, t := range existingTrades {
		merged[t.AggTradeID] = t
	}
	for _, t := range newTrades {
		merged[t.AggTradeID] = t // New data overwrites existing
	}

	// Convert back to slice and sort
	var allTrades []*AggTrade
	for _, t := range merged {
		allTrades = append(allTrades, t)
	}

	// Sort by AggTradeID
	data.SortBy(allTrades, func(t *AggTrade) int64 { return t.AggTradeID })

	// Write merged data back
	return s.writeHourlyAggTradesParquet(parquetPath, allTrades)
}

// fetchHourAggTradesData fetches aggTrades data for a specific time range
func (s *HistoryConsumer) fetchHourAggTradesData(asset *HistoryAsset, start, end time.Time) ([]*AggTrade, error) {
	return FetchAggTrades(s.ctx, &AggTradeRequest{
		Base: SymbolRequest{
			Symbol:    asset.Market,
			StartTime: new(start.UnixMilli()),
			EndTime:   new(end.UnixMilli()),
		},
		Limit: 1000,
	})
}

// fetchAggTradesFromID fetches aggTrades starting from a specific trade ID
func (s *HistoryConsumer) fetchAggTradesFromID(asset *HistoryAsset, fromID int64) ([]*AggTrade, error) {
	return FetchAggTrades(s.ctx, &AggTradeRequest{
		Base: SymbolRequest{
			Symbol: asset.Market,
		},
		FromID: &fromID,
		Limit:  1000,
	})
}

// readHourlyAggTradesParquet reads aggTrades from an hourly parquet file
func (s *HistoryConsumer) readHourlyAggTradesParquet(path string, date time.Time) ([]*AggTrade, error) {
	recordCh, errCh := data.ReadParquet[AggTradeParquet](path)
	records, err := data.DrainParquet[AggTradeParquet](s.ctx, recordCh, errCh)
	if err != nil {
		return nil, err
	}
	trades := make([]*AggTrade, 0, len(records))
	for _, record := range records {
		trades = append(trades, record.ToAggTrade(date))
	}
	return trades, nil
}

// writeHourlyAggTradesParquet writes aggTrades to an hourly parquet file atomically
func (s *HistoryConsumer) writeHourlyAggTradesParquet(path string, trades []*AggTrade) error {
	// Convert to absolute path
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for %s: %w", path, err)
	}
	path = absPath

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to a temp file first to avoid race conditions
	tempPath := fmt.Sprintf("%s.%s.tmp", path, uuid.New().String()[:8])
	writeCh, errCh := data.WriteParquet[AggTradeParquet](tempPath)

	// Write all trades
	for _, trade := range trades {
		parquet, err := trade.Parquet()
		if err != nil {
			close(writeCh)
			os.Remove(tempPath)
			return fmt.Errorf("failed to convert aggTrade to parquet: %w", err)
		}
		select {
		case writeCh <- parquet:
		case err := <-errCh:
			os.Remove(tempPath)
			return fmt.Errorf("failed to write parquet: %w", err)
		}
	}
	close(writeCh)

	// Wait for errors
	for err := range errCh {
		os.Remove(tempPath)
		return fmt.Errorf("failed to write parquet: %w", err)
	}

	// Verify temp file exists before renaming
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		return fmt.Errorf("temp parquet file was not created: %s", tempPath)
	}

	// Atomic rename
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}

// ReadCachedCurrentDay reads cached current day data from hourly parquet files
func (s *HistoryConsumer) ReadCachedCurrentDay(asset *HistoryAsset) ([]*Kline, error) {
	now := time.Now().UTC()
	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	currentHour := now.Hour()

	var allCandles []*Kline

	// If it's the current day, we might need to refresh the last hour data first
	if asset.IsToday() {
		if err := s.RefreshLastHour(asset); err != nil {
			// Log error but continue with what we have
			fmt.Printf("Failed to refresh last hour data: %v\n", err)
		}
	}

	for hour := 0; hour <= currentHour; hour++ {
		parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(hour))

		if !fs.FileExists(parquetPath) {
			continue
		}

		candles, err := s.readHourlyParquet(parquetPath, midnight)
		if err != nil {
			return nil, fmt.Errorf("failed to read hour %d parquet: %w", hour, err)
		}

		allCandles = append(allCandles, candles...)
	}

	return allCandles, nil
}

// RefreshLastHour refreshes the last hour's parquet file by fetching new data from API
func (s *HistoryConsumer) RefreshLastHour(asset *HistoryAsset) error {
	now := time.Now().UTC()
	currentHour := now.Hour()
	parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(currentHour))

	midnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	hourWindowStart := midnight.Add(time.Duration(currentHour) * time.Hour)
	frameMs := int64(time.Duration(asset.Frame) / time.Millisecond)

	var existingCandles []*Kline
	if fs.FileExists(parquetPath) {
		existingCandles, _ = s.readHourlyParquet(parquetPath, midnight)
	}
	fetchStart := hourFetchStart(asset, hourWindowStart, frameMs, existingCandles)

	newCandles, err := s.fetchHourData(asset, fetchStart, now)
	if err != nil {
		return fmt.Errorf("failed to fetch new data: %w", err)
	}

	hourEndBound := midnight.Add(time.Duration(currentHour+1) * time.Hour)
	allCandles := KlineSeries(KlineSeries(existingCandles).Merge(KlineSeries(newCandles))).Filter(hourWindowStart, hourEndBound.Sub(hourWindowStart))
	if len(allCandles) == 0 {
		return nil
	}

	hourEnd := hourEndBound
	if err := s.writeHourlyParquet(parquetPath, allCandles); err != nil {
		return err
	}

	if !s.hourParquetIntegrityOK(s.ctx, parquetPath, asset, hourWindowStart, hourEnd, currentHour, false) {
		return fmt.Errorf("current hour integrity check failed after refresh: %s", parquetPath)
	}
	return nil
}

// hourFetchStart chooses API start: hour start when the file has a leading gap, else last open bar.
func hourFetchStart(asset *HistoryAsset, hourStart time.Time, frameMs int64, existing []*Kline) time.Time {
	if len(existing) == 0 {
		return hourStart
	}
	data.SortBy(existing, func(k *Kline) int64 { return k.OpenTime })
	firstMs := existing[0].OpenTime
	startMs := hourStart.UnixMilli()
	if frameMs > 0 && firstMs > startMs {
		return hourStart
	}
	lastMs := existing[len(existing)-1].OpenTime
	if frameMs > 0 {
		lastMs = (lastMs / frameMs) * frameMs
	}
	if lastMs < startMs {
		lastMs = startMs
	}
	return time.UnixMilli(lastMs).UTC()
}

// fetchHourData fetches kline data for a specific time range (paginated).
func (s *HistoryConsumer) fetchHourData(asset *HistoryAsset, start, end time.Time) ([]*Kline, error) {
	endMs := end.UnixMilli()
	return FetchKlinesAll(s.ctx, &KlineRequest{
		Base: SymbolRequest{
			Symbol:    asset.Market,
			StartTime: new(start.UnixMilli()),
			EndTime:   &endMs,
		},
		Interval: asset.Frame.String(),
		Limit:    1000,
	})
}

// readHourlyParquet reads klines from an hourly parquet file
func (s *HistoryConsumer) readHourlyParquet(path string, date time.Time) ([]*Kline, error) {
	recordCh, errCh := data.ReadParquet[KlineParquet](path)
	records, err := data.DrainParquet[KlineParquet](s.ctx, recordCh, errCh)
	if err != nil {
		return nil, err
	}
	klines := make([]*Kline, 0, len(records))
	for _, record := range records {
		klines = append(klines, record.ToKline(date))
	}
	data.SortBy(klines, func(k *Kline) int64 { return k.OpenTime })
	return klines, nil
}

// writeHourlyParquet writes klines to an hourly parquet file atomically
// Uses temp file + rename to prevent race conditions with readers
func (s *HistoryConsumer) writeHourlyParquet(path string, klines []*Kline) error {
	// Convert to absolute path to avoid relative path resolution issues
	absPath, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("failed to get absolute path for %s: %w", path, err)
	}
	path = absPath

	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to a temp file first to avoid race conditions
	// Use unique suffix to prevent concurrent requests from interfering
	tempPath := fmt.Sprintf("%s.%s.tmp", path, uuid.New().String()[:8])
	writeCh, errCh := data.WriteParquet[KlineParquet](tempPath)

	// Write all klines
	for _, kline := range klines {
		parquet, err := kline.Parquet()
		if err != nil {
			close(writeCh)
			os.Remove(tempPath) // Clean up temp file
			return fmt.Errorf("failed to convert kline to parquet: %w", err)
		}
		select {
		case writeCh <- parquet:
		case err := <-errCh:
			os.Remove(tempPath)
			return fmt.Errorf("failed to write parquet: %w", err)
		}
	}
	close(writeCh)

	// Wait for errors
	for err := range errCh {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to write parquet: %w", err)
	}

	// Verify temp file exists before renaming
	if _, err := os.Stat(tempPath); os.IsNotExist(err) {
		return fmt.Errorf("temp parquet file was not created: %s", tempPath)
	}

	// Atomic rename: temp file -> final path
	// This ensures readers only see complete files
	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return fmt.Errorf("failed to rename temp file: %w", err)
	}

	return nil
}
