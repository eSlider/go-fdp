package integrity

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/fs"
)

// AuditConfig configures a parquet time-series audit.
type AuditConfig struct {
	Path         string
	Midnight     time.Time
	WindowStart  time.Time
	WindowEnd    time.Time
	FrameMs      int64
	CheckMissing bool
	CountOnly    bool
	Hour         int
}

// HourlyTarget describes one parquet file and UTC window to audit.
type HourlyTarget struct {
	Hour         int
	Path         string
	Midnight     time.Time
	HourStart    time.Time
	HourEnd      time.Time
	CheckMissing bool
}

// AuditParquet runs gap and count checks on one parquet file.
func AuditParquet(ctx context.Context, db *sql.DB, cfg AuditConfig) ([]Issue, CountResult, error) {
	cr := CountResult{Path: cfg.Path, Hour: cfg.Hour, Expected: expectedBetweenMs(cfg.WindowStart, cfg.WindowEnd, cfg.FrameMs)}
	if !fs.FileExists(cfg.Path) {
		if cfg.CheckMissing {
			cr.Got = 0
			cr.OK = false
			return []Issue{{
				Code:     CodeCountMismatch,
				Severity: SeverityError,
				Detail:   fmt.Sprintf("missing file: %s", cfg.Path),
			}}, cr, nil
		}
		cr.OK = true
		return nil, cr, nil
	}

	got, err := countInWindow(ctx, db, cfg)
	if err != nil {
		return nil, cr, err
	}
	cr.Got = got
	cr.OK = got == cr.Expected
	if !cfg.CheckMissing && cr.Expected > 0 {
		cr.OK = got <= cr.Expected
	}

	var issues []Issue
	if !cr.OK {
		severity := SeverityError
		if !cfg.CheckMissing && got > cr.Expected {
			// Open hour may include boundary rows; do not block reads.
			severity = SeverityWarning
		}
		issues = append(issues, Issue{
			Code:     CodeCountMismatch,
			Severity: severity,
			OpenTime: int64(cfg.Hour),
			Detail:   fmt.Sprintf("%s: got %d rows, expected %d", cfg.Path, got, cr.Expected),
		})
	}
	if cfg.CountOnly {
		return issues, cr, nil
	}

	extra, err := auditStructural(ctx, db, cfg)
	if err != nil {
		return issues, cr, err
	}
	issues = append(issues, extra...)
	return issues, cr, nil
}

// AuditHourlyTargets audits multiple hour files and hour-boundary continuity.
func AuditHourlyTargets(ctx context.Context, db *sql.DB, targets []HourlyTarget, frameMs int64) ([]Issue, []CountResult, error) {
	var all []Issue
	var counts []CountResult
	var prev *HourlyTarget
	var prevMax int64

	for _, t := range targets {
		cfg := AuditConfig{
			Path:         t.Path,
			Midnight:     t.Midnight,
			WindowStart:  t.HourStart,
			WindowEnd:    t.HourEnd,
			FrameMs:      frameMs,
			CheckMissing: t.CheckMissing,
			Hour:         t.Hour,
		}
		issues, cr, err := AuditParquet(ctx, db, cfg)
		if err != nil {
			return all, counts, err
		}
		all = append(all, issues...)
		counts = append(counts, cr)

		if t.CheckMissing && prev != nil && prev.CheckMissing && frameMs > 0 {
			nextMin, err := minOpenMS(ctx, db, cfg)
			if err != nil {
				return all, counts, err
			}
			if gap := boundaryGapFromMinMax(prevMax, nextMin, frameMs); gap != nil {
				all = append(all, *gap)
			}
		}

		maxMs, err := maxOpenMS(ctx, db, cfg)
		if err != nil {
			return all, counts, err
		}
		prev = &t
		prevMax = maxMs
	}
	return all, counts, nil
}

func expectedBetweenMs(start, end time.Time, frameMs int64) int {
	if frameMs <= 0 || !end.After(start) {
		return 0
	}
	return int((end.UnixMilli() - alignOpenTime(start.UnixMilli(), frameMs)) / frameMs)
}

func alignOpenTime(ms, step int64) int64 {
	if step <= 0 {
		return ms
	}
	return (ms / step) * step
}

func countInWindow(ctx context.Context, db *sql.DB, cfg AuditConfig) (int, error) {
	q := fmt.Sprintf(`
		SELECT count(*)::INT FROM (
			SELECT %s AS open_ms FROM read_parquet(%s)
		) WHERE open_ms >= %d AND open_ms < %d`,
		openMsExpr(cfg.Midnight), sqlQuotePath(cfg.Path),
		cfg.WindowStart.UnixMilli(), cfg.WindowEnd.UnixMilli())
	var n int
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, fmt.Errorf("count %s: %w", cfg.Path, err)
	}
	return n, nil
}

func maxOpenMS(ctx context.Context, db *sql.DB, cfg AuditConfig) (int64, error) {
	q := fmt.Sprintf(`
		SELECT coalesce(max(open_ms), 0)::BIGINT FROM (
			SELECT %s AS open_ms FROM read_parquet(%s)
		) WHERE open_ms >= %d AND open_ms < %d`,
		openMsExpr(cfg.Midnight), sqlQuotePath(cfg.Path),
		cfg.WindowStart.UnixMilli(), cfg.WindowEnd.UnixMilli())
	var n int64
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func minOpenMS(ctx context.Context, db *sql.DB, cfg AuditConfig) (int64, error) {
	q := fmt.Sprintf(`
		SELECT coalesce(min(open_ms), 0)::BIGINT FROM (
			SELECT %s AS open_ms FROM read_parquet(%s)
		) WHERE open_ms >= %d AND open_ms < %d`,
		openMsExpr(cfg.Midnight), sqlQuotePath(cfg.Path),
		cfg.WindowStart.UnixMilli(), cfg.WindowEnd.UnixMilli())
	var n int64
	if err := db.QueryRowContext(ctx, q).Scan(&n); err != nil {
		return 0, err
	}
	return n, nil
}

func openMsExpr(midnight time.Time) string {
	return data.ParquetOpenTimeMSExpr(midnight.Year(), int(midnight.Month()), midnight.Day())
}

func auditStructural(ctx context.Context, db *sql.DB, cfg AuditConfig) ([]Issue, error) {
	startMs := cfg.WindowStart.UnixMilli()
	endMs := cfg.WindowEnd.UnixMilli()
	aligned := alignOpenTime(startMs, cfg.FrameMs)
	path := sqlQuotePath(cfg.Path)
	openExpr := openMsExpr(cfg.Midnight)

	var parts []string

	parts = append(parts, fmt.Sprintf(`
		SELECT 'duplicate_open_time' AS code, 'error' AS severity, open_ms AS open_time,
			'duplicate open_time' AS detail
		FROM (
			SELECT %s AS open_ms FROM read_parquet(%s)
		) WHERE open_ms >= %d AND open_ms < %d
		GROUP BY open_ms HAVING count(*) > 1`, openExpr, path, startMs, endMs))

	parts = append(parts, fmt.Sprintf(`
		SELECT 'open_time_not_increasing' AS code, 'error' AS severity, open_ms AS open_time,
			'open_time not strictly increasing' AS detail
		FROM (
			SELECT open_ms, lag(open_ms) OVER (ORDER BY open_ms) AS prev_ms
			FROM (
				SELECT %s AS open_ms FROM read_parquet(%s)
			) WHERE open_ms >= %d AND open_ms < %d
		) WHERE prev_ms IS NOT NULL AND open_ms <= prev_ms`, openExpr, path, startMs, endMs))

	if cfg.FrameMs > 0 {
		parts = append(parts, fmt.Sprintf(`
			SELECT 'wrong_step' AS code, 'error' AS severity, open_ms AS open_time,
				'unexpected step between candles' AS detail
			FROM (
				SELECT open_ms, lag(open_ms) OVER (ORDER BY open_ms) AS prev_ms
				FROM (
					SELECT %s AS open_ms FROM read_parquet(%s)
				) WHERE open_ms >= %d AND open_ms < %d
			) WHERE prev_ms IS NOT NULL AND (open_ms - prev_ms) != %d`,
			openExpr, path, startMs, endMs, cfg.FrameMs))
	}

	parts = append(parts, fmt.Sprintf(`
		SELECT 'out_of_window' AS code, 'warning' AS severity, open_ms AS open_time,
			'open_time outside window' AS detail
		FROM (
			SELECT %s AS open_ms FROM read_parquet(%s)
		) WHERE open_ms < %d OR open_ms >= %d`, openExpr, path, startMs, endMs))

	if cfg.CheckMissing && cfg.FrameMs > 0 && endMs > aligned {
		parts = append(parts, fmt.Sprintf(`
			SELECT 'missing_interval' AS code, 'error' AS severity, slot AS open_time,
				'missing candle at open_time' AS detail
			FROM (
				SELECT unnest(generate_series(%d::BIGINT, %d::BIGINT - %d::BIGINT, %d::BIGINT)) AS slot
			) e
			LEFT JOIN (
				SELECT %s AS open_ms FROM read_parquet(%s)
			) c ON e.slot = c.open_ms
			WHERE c.open_ms IS NULL`, aligned, endMs, cfg.FrameMs, cfg.FrameMs, openExpr, path))
	}

	q := strings.Join(parts, "\nUNION ALL\n")
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("audit structural %s: %w", cfg.Path, err)
	}
	defer rows.Close()
	return scanIssues(rows)
}

func boundaryGapFromMinMax(prevMax, nextMin, frameMs int64) *Issue {
	if prevMax == 0 || nextMin == 0 || frameMs <= 0 {
		return nil
	}
	expected := prevMax + frameMs
	if nextMin == expected || nextMin == prevMax {
		return nil
	}
	return &Issue{
		Code:     CodeHourBoundaryGap,
		Severity: SeverityError,
		OpenTime: nextMin,
		Detail:   fmt.Sprintf("hour boundary gap: last=%d expected next=%d got=%d", prevMax, expected, nextMin),
	}
}
