package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/integrity"
	"github.com/eslider/go-fdp/pkg/integrity/run"
)

func parseDate(timeStr *string, def time.Time) (v time.Time) {
	if *timeStr == "" {
		v = def
		return
	}
	var err error
	v, err = time.ParseInLocation("2006-01-02", *timeStr, time.UTC)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -to: %v\n", err)
		os.Exit(2)
	}
	return v
}

func main() {
	now := time.Now().UTC()
	to := parseDate(flag.String("from", "", "start date YYYY-MM-DD (UTC)"), now)
	opts := run.Options{
		MarketType: *flag.String("mtype", "spot", "market type"),
		Market:     *flag.String("market", "BTCUSDT", "trading pair"),
		Frame:      data.NewFrame(*flag.String("frame", "1m", "kline interval")),
		To:         to,
		From:       parseDate(flag.String("to", "", "end date YYYY-MM-DD (UTC, default today)"), to),
		TodayOnly:  *flag.Bool("today", false, "audit only current day hourly files"),
		CountOnly:  *flag.Bool("count-only", true, "audit row counts only (no open_time grid checks)"),
	}
	if opts.TodayOnly {
		opts.From = time.Date(opts.To.Year(), opts.To.Month(), opts.To.Day(), 0, 0, 0, 0, time.UTC)
	}

	ctx := context.Background()
	db, err := integrity.OpenDB()
	if err != nil {
		fmt.Fprintf(os.Stderr, "duckdb: %v\n", err)
		os.Exit(2)
	}
	defer db.Close()

	// Run audit
	result, err := run.Audit(ctx, db, opts)
	if err != nil {
		fmt.Fprintf(os.Stderr, "audit: %v\n", err)
		os.Exit(2)
	}

	// Print counts only
	if opts.CountOnly {
		fmt.Print(integrity.FormatCountReport(result.Counts, opts.Frame))
	}

	// Print issues
	fmt.Print(integrity.FormatIssues(result.Issues))
	if integrity.HasErrors(result.Issues) {
		os.Exit(1)
	}
}
