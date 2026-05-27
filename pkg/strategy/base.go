package strategy

import (
	trade "github.com/eslider/go-trade"

	"github.com/eslider/go-fdp/pkg/features"
)

// BaseConfig configures MACD+RSI+SMA200 entries from the plan.
type BaseConfig struct {
	RSIPeriod    int
	RSIMinLong   float64
	RSIMaxLong   float64
	SMAPeriod    int
	MACDFast     int
	MACDSlow     int
	MACDSignal   int
}

// DefaultBaseConfig returns intraday defaults.
func DefaultBaseConfig() BaseConfig {
	return BaseConfig{
		RSIPeriod:  14,
		RSIMinLong: 45,
		RSIMaxLong: 60,
		SMAPeriod:  200,
		MACDFast:   12,
		MACDSlow:   26,
		MACDSignal: 9,
	}
}

// ComputeFeatures builds indicator series for candles.
func ComputeFeatures(candles []trade.Candle, cfg BaseConfig) ([]BarFeatures, error) {
	ohlc := features.OHLCFromCandles(candles)
	if ohlc.Len() == 0 {
		return nil, nil
	}
	rsi := features.RSI(ohlc.Close, cfg.RSIPeriod)
	sma := features.SMA(ohlc.Close, cfg.SMAPeriod)
	macd := features.MACDCompute(ohlc.Close, cfg.MACDFast, cfg.MACDSlow, cfg.MACDSignal)
	atr := features.ATR(ohlc.High, ohlc.Low, ohlc.Close, 14)
	vol := make([]float64, ohlc.Len())
	for i := range ohlc.Close {
		if i >= 100 {
			vol[i] = features.RealizedVol(ohlc.Close[:i+1], 100)
		}
	}
	out := make([]BarFeatures, ohlc.Len())
	for i := range ohlc.Close {
		out[i] = BarFeatures{
			RSI:         rsi[i],
			MACDHist:    macd.Histogram[i],
			SMA200:      sma[i],
			ATR:         atr[i],
			RealizedVol: vol[i],
			Close:       ohlc.Close[i],
		}
	}
	return out, nil
}

// BaseSignals returns long/flat signals from MACD cross + RSI band + SMA200 filter.
func BaseSignals(candles []trade.Candle, cfg BaseConfig) ([]Signal, []BarFeatures, error) {
	feats, err := ComputeFeatures(candles, cfg)
	if err != nil {
		return nil, nil, err
	}
	if len(feats) == 0 {
		return nil, feats, nil
	}
	ohlc := features.OHLCFromCandles(candles)
	macd := features.MACDCompute(ohlc.Close, cfg.MACDFast, cfg.MACDSlow, cfg.MACDSignal)

	var signals []Signal
	for i := 1; i < len(feats); i++ {
		f := feats[i]
		if features.IsNaN(f.RSI) || features.IsNaN(f.SMA200) {
			continue
		}
		if !features.MACDCrossUp(macd.Histogram, i) {
			continue
		}
		if f.RSI < cfg.RSIMinLong || f.RSI > cfg.RSIMaxLong {
			continue
		}
		if f.Close <= f.SMA200 {
			continue
		}
		signals = append(signals, Signal{Index: i, Side: SideLong})
	}
	return signals, feats, nil
}
