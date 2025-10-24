package main

import (
	"encoding/csv"
	"os"
	"strings"
	"sync-v3/binance"
	"testing"
	"time"
)

// Test normalization by getting zip file
func TestNormalization(t *testing.T) {
	path := binance.HistoryAsset{
		Market:    binance.Spot,
		Frequency: binance.Monthly,
		Date:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		Interval:  binance.OneMinute,
		Symbol:    "BTCUSDT",
	}.Link()

	t.Run("Candles typed and storage as parquet ", func(t *testing.T) {
		from := "data/spot/monthly/klines/0GBNB/30m/0GBNB-30m-2025-09.csv"
		to := strings.TrimSuffix(from, ".csv") + ".parquet" // Use original file name as parquet file name
		csvFile, _ := os.Open(from)
		reader := csv.NewReader(csvFile)

		if err := os.Remove(to); err != nil {
			t.Errorf("failed to remove file %s: %v", to, err)
		}

		if reader.Comma == ',' {
			t.Errorf("unexpected comma: %c", reader.Comma)
		}

	})

	t.Run("SymbolDateAssetZipLink", func(t *testing.T) {
		if path != "data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2021-01.zip" {
			t.Errorf("unexpected path: %s", path)
		}
	})

	t.Run("Download", func(t *testing.T) {

	})
}
