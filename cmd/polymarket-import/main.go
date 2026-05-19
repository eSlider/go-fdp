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
	source := flag.String("source", "hf", "bulk source: hf or polydata")
	dataPath := flag.String("data-dir", "", "local HF parquet dir or polydata CSV path")
	url := flag.String("url", "", "optional URL to download polydata CSV (not implemented in MVP; use local file)")
	market := flag.String("market", polymarket.DefaultMarket, "market symbol")
	frame := flag.String("frame", "5m", "frame: 1m 5m 15m 1h 4h")
	fromMs := flag.Int64("from", 0, "start unix ms")
	toMs := flag.Int64("to", 0, "end unix ms")
	flag.Parse()

	_ = url
	if *dataPath == "" {
		slog.Error("data-dir is required")
		os.Exit(1)
	}
	from := time.UnixMilli(*fromMs).UTC()
	to := time.UnixMilli(*toMs).UTC()
	if *toMs == 0 {
		to = time.Now().UTC()
	}
	if *fromMs == 0 {
		from = to.Add(-7 * 24 * time.Hour)
	}

	st := polymarket.NewStore("")
	importer := polymarket.NewBulkImporter(st)
	src := polymarket.BulkSourceHF
	if *source == "polydata" {
		src = polymarket.BulkSourcePolyData
	}
	if err := importer.ImportFromPath(context.Background(), src, *dataPath, *market, data.NewFrame(*frame), from, to); err != nil {
		slog.Error("import failed", "error", err)
		os.Exit(1)
	}
	slog.Info("import complete", "source", *source, "market", *market, "frame", *frame)
}
