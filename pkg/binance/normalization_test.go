package binance

import (
	"testing"
	"time"
)

// Test normalization by getting zip file
func TestNormalization(t *testing.T) {
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
								Frame:      StringToFrame(frame),
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
}
