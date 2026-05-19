package binance

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/eslider/go-binance-fdp/pkg/fs"
	"github.com/eslider/go-binance-fdp/pkg/integrity"
)

func (s *HistoryConsumer) midnightUTC(now time.Time) time.Time {
	return time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
}

func (s *HistoryConsumer) hourWindow(midnight time.Time, hour int) (start, end time.Time) {
	start = midnight.Add(time.Duration(hour) * time.Hour)
	end = start.Add(time.Hour)
	return start, end
}

func (s *HistoryConsumer) auditHourParquet(ctx context.Context, path string, asset *HistoryAsset, hourStart, hourEnd time.Time, hour int, checkMissing bool) ([]integrity.Issue, error) {
	db, err := integrity.OpenDB()
	if err != nil {
		return nil, err
	}
	defer db.Close()

	frameMs := int64(time.Duration(asset.Frame) / time.Millisecond)
	midnight := time.Date(hourStart.Year(), hourStart.Month(), hourStart.Day(), 0, 0, 0, 0, time.UTC)
	issues, _, err := integrity.AuditParquet(ctx, db, integrity.AuditConfig{
		Path:         path,
		Midnight:     midnight,
		WindowStart:  hourStart,
		WindowEnd:    hourEnd,
		FrameMs:      frameMs,
		CheckMissing: checkMissing,
		Hour:         hour,
	})
	return issues, err
}

func (s *HistoryConsumer) hourParquetIntegrityOK(ctx context.Context, path string, asset *HistoryAsset, hourStart, hourEnd time.Time, hour int, checkMissing bool) bool {
	issues, err := s.auditHourParquet(ctx, path, asset, hourStart, hourEnd, hour, checkMissing)
	if err != nil {
		slog.Warn("kline integrity audit failed", "path", path, "error", err)
		return false
	}
	failed := integrity.HasStructuralErrors(issues)
	if checkMissing {
		failed = failed || integrity.HasErrors(issues)
	}
	if failed {
		for _, iss := range issues {
			if iss.Severity != integrity.SeverityError {
				continue
			}
			if !checkMissing && iss.Code == integrity.CodeMissingInterval {
				continue
			}
			slog.Warn("kline integrity", "code", iss.Code, "detail", iss.Detail, "open_time", iss.OpenTime)
		}
	}
	return !failed
}

// SealHour fetches the full hour from API, writes parquet, and audits via DuckDB.
func (s *HistoryConsumer) SealHour(asset *HistoryAsset, hour int, midnight time.Time) ([]*Kline, error) {
	hourStart, hourEnd := s.hourWindow(midnight, hour)
	parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(hour))

	candles, err := s.fetchHourData(asset, hourStart, hourEnd)
	if err != nil {
		return nil, fmt.Errorf("seal hour %d: %w", hour, err)
	}
	candles = KlineSeries(candles).Filter(hourStart, hourEnd.Sub(hourStart))

	if len(candles) > 0 {
		if err := s.writeHourlyParquet(parquetPath, candles); err != nil {
			return nil, fmt.Errorf("seal hour %d write: %w", hour, err)
		}
	}

	if !s.hourParquetIntegrityOK(s.ctx, parquetPath, asset, hourStart, hourEnd, hour, true) {
		return candles, fmt.Errorf("seal hour %d: integrity check failed", hour)
	}
	return candles, nil
}

// LoadOrSealHour returns cached hour data if integrity passes, otherwise seals from API.
func (s *HistoryConsumer) LoadOrSealHour(asset *HistoryAsset, hour int, midnight time.Time) ([]*Kline, error) {
	return s.loadOrSealHour(asset, hour, midnight)
}

// loadOrSealHour returns cached hour data if integrity passes, otherwise seals from API.
func (s *HistoryConsumer) loadOrSealHour(asset *HistoryAsset, hour int, midnight time.Time) ([]*Kline, error) {
	hourStart, hourEnd := s.hourWindow(midnight, hour)
	parquetPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(hour))

	if fs.FileExists(parquetPath) {
		candles, readErr := s.readHourlyParquet(parquetPath, midnight)
		if readErr == nil && s.hourHasLeadingGap(candles, hourStart, asset) {
			slog.Warn("hour cache has leading gap, re-sealing", "hour", hour, "path", parquetPath)
			return s.SealHour(asset, hour, midnight)
		}
		if s.hourParquetIntegrityOK(s.ctx, parquetPath, asset, hourStart, hourEnd, hour, true) {
			if readErr != nil {
				return nil, readErr
			}
			return candles, nil
		}
		slog.Warn("hour cache failed integrity, re-sealing", "hour", hour, "path", parquetPath)
	}
	return s.SealHour(asset, hour, midnight)
}

func (s *HistoryConsumer) hourHasLeadingGap(candles []*Kline, hourStart time.Time, asset *HistoryAsset) bool {
	if len(candles) == 0 {
		return false
	}
	frameMs := int64(time.Duration(asset.Frame) / time.Millisecond)
	if frameMs <= 0 {
		return false
	}
	data.SortBy(candles, func(k *Kline) int64 { return k.OpenTime })
	return candles[0].OpenTime > hourStart.UnixMilli()
}
