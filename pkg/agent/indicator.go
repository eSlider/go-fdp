package agent

import (
	"github.com/eslider/go-fdp/pkg/features"
	"github.com/eslider/go-fdp/pkg/strategy"
)

// IndicatorAgent votes from MACD+RSI+SMA base rules at index i.
type IndicatorAgent struct {
	Base strategy.BaseConfig
}

func (a IndicatorAgent) Name() string { return "indicator" }

func (a IndicatorAgent) Vote(ctx Context) Vote {
	cfg := a.Base
	if cfg.RSIPeriod == 0 {
		cfg = strategy.DefaultBaseConfig()
	}
	if ctx.Index < 1 || ctx.Index >= len(ctx.Features) {
		return Vote{Agent: a.Name(), Action: ActionHold, Weight: 1, Reason: "no features"}
	}
	f := ctx.Features[ctx.Index]
	if features.IsNaN(f.RSI) || features.IsNaN(f.SMA200) {
		return Vote{Agent: a.Name(), Action: ActionHold, Weight: 1, Reason: "warming up"}
	}
	ohlc := features.OHLCFromCandles(ctx.Candles)
	macd := features.MACDCompute(ohlc.Close, cfg.MACDFast, cfg.MACDSlow, cfg.MACDSignal)
	if features.MACDCrossUp(macd.Histogram, ctx.Index) &&
		f.RSI >= cfg.RSIMinLong && f.RSI <= cfg.RSIMaxLong &&
		f.Close > f.SMA200 {
		return Vote{Agent: a.Name(), Action: ActionBuy, Weight: 1.2, Reason: "macd+rsi+sma long"}
	}
	if features.MACDCrossDown(macd.Histogram, ctx.Index) && f.Close < f.SMA200 {
		return Vote{Agent: a.Name(), Action: ActionSell, Weight: 1, Reason: "macd cross down below sma"}
	}
	return Vote{Agent: a.Name(), Action: ActionHold, Weight: 1, Reason: "no setup"}
}
