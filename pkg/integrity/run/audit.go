package run

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/fs"
	"github.com/eslider/go-fdp/pkg/integrity"
)

// Options configures a kline integrity audit run.
type Options struct {
	MarketType string
	Market     string
	Frame      data.Frame
	From       time.Time
	To         time.Time
	TodayOnly  bool
	CountOnly  bool
}

// Result holds issues and optional count rows from an audit run.
type Result struct {
	Issues []integrity.Issue
	Counts []integrity.CountResult
}

// Audit runs count and/or deep audits for the configured date range.
func Audit(ctx context.Context, db *sql.DB, opt Options) (Result, error) {
	frameMs := int64(time.Duration(opt.Frame) / time.Millisecond)
	var out Result

	if data.IsToday(opt.To) || opt.TodayOnly {
		ih, cr := auditToday(ctx, db, opt.MarketType, opt.Market, opt.Frame, frameMs, opt.To, opt.CountOnly)
		out.Issues = append(out.Issues, ih...)
		out.Counts = append(out.Counts, cr...)
	}
	if !opt.TodayOnly {
		for d := opt.From; !d.After(opt.To); d = d.AddDate(0, 0, 1) {
			if data.IsToday(d) {
				continue
			}
			id, dayCounts := auditDay(ctx, db, opt.MarketType, opt.Market, opt.Frame, frameMs, d, opt.CountOnly)
			out.Issues = append(out.Issues, id...)
			out.Counts = append(out.Counts, dayCounts...)
		}
	}
	return out, nil
}

func auditToday(ctx context.Context, db *sql.DB, mtype, market string, frame data.Frame, frameMs int64, day time.Time, countOnly bool) ([]integrity.Issue, []integrity.CountResult) {
	asset := &binance.HistoryAsset{
		MarketType: binance.NewMarketType(mtype),
		Frequency:  binance.Daily,
		Frame:      frame,
		Indicator:  binance.Klines,
		Market:     market,
		Date:       day,
	}
	dir := fs.GetModuleRelativePath(asset.TodayParquetDir())
	currentHour := time.Now().UTC().Hour()
	if !data.IsToday(day) {
		currentHour = 23
	}

	targets := binance.BuildHourlyTargets(asset, day, 0, currentHour, func(hour int) bool {
		return hour < currentHour
	})

	issues, counts, err := integrity.AuditHourlyTargets(ctx, db, targets, frameMs)
	if err != nil {
		return []integrity.Issue{{
			Code:     integrity.CodeCountMismatch,
			Severity: integrity.SeverityError,
			Detail:   err.Error(),
		}}, counts
	}

	if countOnly {
		return issues, counts
	}

	for i, t := range targets {
		if i < currentHour && !fs.FileExists(t.Path) {
			issues = append(issues, integrity.Issue{
				Code:     integrity.CodeCountMismatch,
				Severity: integrity.SeverityError,
				Detail: fmt.Sprintf("missing completed hour file: %s (expected %d rows for full hour)",
					t.Path, integrity.ExpectedCandlesPerHour(frame)),
			})
		}
	}
	if _, err := os.Stat(dir); err != nil && len(targets) > 0 {
		issues = append(issues, integrity.Issue{
			Code:     integrity.CodeCountMismatch,
			Severity: integrity.SeverityError,
			Detail:   fmt.Sprintf("today directory missing: %s", dir),
		})
	}
	return issues, counts
}

func auditDay(ctx context.Context, db *sql.DB, mtype, market string, frame data.Frame, frameMs int64, day time.Time, countOnly bool) ([]integrity.Issue, []integrity.CountResult) {
	asset := &binance.HistoryAsset{
		MarketType: binance.NewMarketType(mtype),
		Frequency:  binance.Daily,
		Frame:      frame,
		Indicator:  binance.Klines,
		Market:     market,
		Date:       day,
	}
	path := fs.GetModuleRelativePath(asset.ParquetPath())
	if !fs.FileExists(path) {
		return []integrity.Issue{{
			Code:     integrity.CodeCountMismatch,
			Severity: integrity.SeverityError,
			Detail: fmt.Sprintf("missing daily parquet: %s (expected %d rows for full day)",
				path, integrity.ExpectedCandlesPerDay(frame)),
		}}, nil
	}

	midnight := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	end := midnight.Add(24 * time.Hour)
	cfg := integrity.AuditConfig{
		Path:         path,
		Midnight:     midnight,
		WindowStart:  midnight,
		WindowEnd:    end,
		FrameMs:      frameMs,
		CheckMissing: !countOnly,
		CountOnly:    countOnly,
		Hour:         -1,
	}
	issues, cr, err := integrity.AuditParquet(ctx, db, cfg)
	if err != nil {
		return []integrity.Issue{{
			Code:     integrity.CodeMissingInterval,
			Severity: integrity.SeverityError,
			Detail:   fmt.Sprintf("audit daily parquet %s: %v", path, err),
		}}, nil
	}
	return issues, []integrity.CountResult{cr}
}
