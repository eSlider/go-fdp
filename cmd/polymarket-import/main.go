package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/polymarket"
)

func main() {
	source := flag.String("source", "hf", "bulk source: hf, polydata, or api (live Gamma+CLOB backfill)")
	dataPath := flag.String("data-dir", "", "hf: directory of FDP-schema .parquet files; polydata: path to .csv or .csv.xz file")
	url := flag.String("url", "", "reserved for future HTTP download of bulk files")
	market := flag.String("market", polymarket.DefaultMarket, "market symbol")
	frame := flag.String("frame", "5m", "frame: 1m 5m 15m 1h 4h")
	fromMs := flag.Int64("from", 0, "start unix ms (UTC)")
	toMs := flag.Int64("to", 0, "end unix ms (UTC)")
	year := flag.Int("year", 0, "calendar year in UTC: [Jan 1, min(now, Jan 1 next year))). Overrides -from/-to when -year is set")
	maxDays := flag.Int("max-days", 0, "api source only: backfill at most this many UTC days from range start (0 = all days in range)")
	maxWindows := flag.Int("max-windows", 0, "api source only: cap Polymarket windows per UTC day (0 = unlimited; e.g. 48 for smoke tests)")
	flag.Parse()

	_ = url
	now := time.Now().UTC()
	var from, to time.Time

	if *year != 0 {
		from = time.Date(*year, 1, 1, 0, 0, 0, 0, time.UTC)
		nextY := time.Date(*year+1, 1, 1, 0, 0, 0, 0, time.UTC)
		if nextY.After(now) {
			to = now
		} else {
			to = nextY
		}
	} else {
		from = time.UnixMilli(*fromMs).UTC()
		to = time.UnixMilli(*toMs).UTC()
		if *toMs == 0 {
			to = now
		}
		if *fromMs == 0 {
			from = to.Add(-7 * 24 * time.Hour)
		}
	}

	if *source == "hf" || *source == "polydata" {
		if *dataPath == "" {
			slog.Error("data-dir is required for hf and polydata sources")
			os.Exit(1)
		}
	}

	st := polymarket.NewStore("")
	f := data.NewFrame(*frame)

	switch *source {
	case "hf":
		importer := polymarket.NewBulkImporter(st)
		if err := importer.ImportFromPath(context.Background(), polymarket.BulkSourceHF, *dataPath, *market, f, from, to); err != nil {
			slog.Error("import failed", "error", err)
			os.Exit(1)
		}
	case "polydata":
		importer := polymarket.NewBulkImporter(st)
		if err := importer.ImportFromPath(context.Background(), polymarket.BulkSourcePolyData, *dataPath, *market, f, from, to); err != nil {
			slog.Error("import failed", "error", err)
			os.Exit(1)
		}
	case "api":
		client := polymarket.NewClient()
		collector := polymarket.NewCollector(client, st)
		collector.MaxWindowsPerDay = *maxWindows
		rangeTo := to
		if *maxDays > 0 {
			cap := from.AddDate(0, 0, *maxDays)
			if cap.Before(rangeTo) {
				rangeTo = cap
			}
		}
		slog.Info("api backfill", "market", *market, "frame", f.String(), "from", from.Format(time.RFC3339), "to", rangeTo.Format(time.RFC3339), "max_windows_per_day", *maxWindows)
		if err := collector.EnsureRange(context.Background(), *market, f, from, rangeTo); err != nil {
			slog.Error("api import failed", "error", err)
			os.Exit(1)
		}
	default:
		slog.Error("unknown source", "source", *source)
		os.Exit(1)
	}
	slog.Info("import complete", "source", *source, "market", *market, "frame", f.String())
}
