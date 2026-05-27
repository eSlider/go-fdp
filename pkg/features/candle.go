package features

import (
	"time"

	"github.com/eslider/go-fdp/pkg/binance"
	trade "github.com/eslider/go-trade"
)

// CandleFromBinance converts a Binance kline to go-trade Candle.
func CandleFromBinance(k *binance.Kline) trade.Candle {
	open := time.UnixMilli(k.OpenTime).UTC()
	closeT := time.UnixMilli(k.CloseTime).UTC()
	if closeT.Before(open) {
		closeT = open
	}
	return trade.Candle{
		TimeOpen:    open,
		TimeClose:   closeT,
		Open:        k.OpenPrice,
		High:        k.HighPrice,
		Low:         k.LowPrice,
		Close:       k.ClosePrice,
		Volume:      k.Volume,
		QuoteVolume: k.QuoteVolume,
		TradesCount: float64(k.NumberOfTrades),
	}
}

// CandlesFromBinance converts klines to go-trade candles (preserves order).
func CandlesFromBinance(klines []*binance.Kline) []trade.Candle {
	out := make([]trade.Candle, 0, len(klines))
	for _, k := range klines {
		if k == nil {
			continue
		}
		out = append(out, CandleFromBinance(k))
	}
	return out
}

// OHLCFromCandles extracts aligned OHLCV series from go-trade candles.
func OHLCFromCandles(candles []trade.Candle) OHLC {
	n := len(candles)
	o := OHLC{
		Open:   make([]float64, n),
		High:   make([]float64, n),
		Low:    make([]float64, n),
		Close:  make([]float64, n),
		Volume: make([]float64, n),
	}
	for i, c := range candles {
		o.Open[i] = c.Open
		o.High[i] = c.High
		o.Low[i] = c.Low
		o.Close[i] = c.Close
		o.Volume[i] = c.Volume
	}
	return o
}
