package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/eslider/go-binance-fdp/pkg/integrity"
	"github.com/eslider/go-binance-fdp/pkg/integrity/run"
)

func main() {
	market := flag.String("market", "BTCUSDT", "trading pair")
	frameStr := flag.String("frame", "1m", "kline interval")
	mtype := flag.String("mtype", "spot", "market type")
	fromStr := flag.String("from", "", "start date YYYY-MM-DD (UTC)")
	toStr := flag.String("to", "", "end date YYYY-MM-DD (UTC, default today)")
	todayOnly := flag.Bool("today", false, "audit only current day hourly files")
	countOnly := flag.Bool("count-only", true, "audit row counts only (no open_time grid checks)")
	flag.Parse()

	frame := data.NewFrame(*frameStr)
	now := time.Now().UTC()
	to := now
	if *toStr != "" {
		var err error
		to, err = time.ParseInLocation("2006-01-02", *toStr, time.UTC)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -to: %v\n", err)
			os.Exit(2)
		}
	}
	from := to
	if *fromStr != "" {
		var err error
		from, err = time.ParseInLocation("2006-01-02", *fromStr, time.UTC)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid -from: %v\n", err)
			os.Exit(2)
		}
	}
	if *todayOnly {
		from = time.Date(to.Year(), to.Month(), to.Day(), 0, 0, 0, 0, time.UTC)
	}

	ctx := context.Background()
	db, err := integrity.OpenDB()
	if err != nil {
		fmt.Fprintf(os.Stderr, "duckdb: %v\n", err)
		os.Exit(2)
	}
	defer db.Close()

	result, err := run.Audit(ctx, db, run.Options{
		MarketType: *mtype,
		Market:     *market,
		Frame:      frame,
		From:       from,
		To:         to,
		TodayOnly:  *todayOnly,
		CountOnly:  *countOnly,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "audit: %v\n", err)
		os.Exit(2)
	}

	if *countOnly {
		fmt.Print(integrity.FormatCountReport(result.Counts, frame))
	}
	fmt.Print(integrity.FormatIssues(result.Issues))
	if integrity.HasErrors(result.Issues) {
		os.Exit(1)
	}
}
