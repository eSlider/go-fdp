package binance

import (
	"encoding/csv"
	"os"
	"strings"
	"sync-v3/pkg/fs"
	"testing"
	"time"
)

// Test normalization by getting zip file
func TestNormalization(t *testing.T) {
	//asset := &binance.HistoryAsset{
	//	MarketType: binance.Spot,
	//	Frequency:  binance.Monthly,
	//	Date:       time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	//	Frame:      binance.OneMinute,
	//	Market:     "BTCUSDT",
	//}

	t.Run("Parsing and creation history asset links", func(t *testing.T) {

		for _, market := range []string{"BTCUSDT", "ETHUSDT"} {
			for _, frame := range []string{"1m", "1h"} {
				for _, freq := range []string{"daily", "monthly"} {
					for _, indicator := range []Indicator{Klines, Trades, AggTrades} {
						for _, marketType := range []MarketType{Spot} { // TODO:

							asset := &HistoryAsset{
								MarketType: marketType,
								Frequency:  Frequency(freq),
								Indicator:  indicator,
								Date:       time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
								Frame:      Frame(frame),
								Market:     market,
							}

							zipLink := asset.SymbolDateAssetZipLink()
							reverseAsset, err := NewHistoryAssetByPath(zipLink)
							if err != nil {
								t.Errorf("could not parse path: %s", err.Error())
							}
							link := reverseAsset.SymbolDateAssetZipLink()
							if link != zipLink {
								t.Errorf("unexpected asset: %v", zipLink)
							}
						}
					}
				}
			}
		}
	})

	t.Run("Candles typed and storage as parquet ", func(t *testing.T) {

		// Get module root directory
		rootPath := fs.ModuleRootPath() + "/"
		from := "data/spot/monthly/klines/0GBNB/30m/0GBNB-30m-2025-09.csv"
		to := strings.TrimSuffix(from, ".csv") + ".parquet" // Use original file name as parquet file name
		csvFile, _ := os.Open(rootPath + from)
		reader := csv.NewReader(csvFile)

		if err := os.Remove(rootPath + to); err != nil {
			t.Errorf("failed to remove file %s: %v", to, err)
		}

		if reader.Comma == ',' {
			t.Errorf("unexpected comma: %c", reader.Comma)
		}

	})

	t.Run("Download", func(t *testing.T) {

	})
}
