package integrity

import (
	"fmt"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
)

// CountResult is a row-count audit for one parquet file.
type CountResult struct {
	Path     string
	Got      int
	Expected int
	Hour     int
	OK       bool
}

// ExpectedCandlesPerHour returns the candle count for a full UTC hour at frame.
func ExpectedCandlesPerHour(frame data.Frame) int {
	return expectedBetween(frame, time.Unix(0, 0), time.Unix(0, 0).Add(time.Hour))
}

// ExpectedCandlesPerDay returns the candle count for a full UTC day at frame.
func ExpectedCandlesPerDay(frame data.Frame) int {
	start := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	return expectedBetween(frame, start, start.Add(24*time.Hour))
}

// ExpectedCandlesBetween returns candles for [start, end) at frame.
func ExpectedCandlesBetween(frame data.Frame, start, end time.Time) int {
	return expectedBetween(frame, start, end)
}

func expectedBetween(frame data.Frame, start, end time.Time) int {
	step := time.Duration(frame)
	if step <= 0 || !end.After(start) {
		return 0
	}
	return int(end.Sub(start) / step)
}

// FormatCountReport prints count audit lines.
func FormatCountReport(results []CountResult, frame data.Frame) string {
	var b string
	b += fmt.Sprintf("frame %s: expected %d/hour (full), %d/day (full)\n",
		frame.String(), ExpectedCandlesPerHour(frame), ExpectedCandlesPerDay(frame))
	for _, r := range results {
		status := "OK"
		if !r.OK {
			status = "MISMATCH"
		}
		if r.Hour >= 0 {
			b += fmt.Sprintf("  hour_%02d %s got=%d expected=%d %s\n", r.Hour, status, r.Got, r.Expected, r.Path)
		} else {
			b += fmt.Sprintf("  daily %s got=%d expected=%d %s\n", status, r.Got, r.Expected, r.Path)
		}
	}
	return b
}

// FormatIssues returns a human-readable report.
func FormatIssues(issues []Issue) string {
	if len(issues) == 0 {
		return "OK: no issues found\n"
	}
	var b string
	for i, iss := range issues {
		b += fmt.Sprintf("[%d] %s %s open_time=%d %s\n", i+1, iss.Severity, iss.Code, iss.OpenTime, iss.Detail)
	}
	return b
}
