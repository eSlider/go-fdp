package agent

import (
	trade "github.com/eslider/go-trade"
)

// MicroAgent uses candle taker-buy imbalance when aggTrades are unavailable.
type MicroAgent struct{}

func (MicroAgent) Name() string { return "micro" }

func (MicroAgent) Vote(ctx Context) Vote {
	if ctx.Index < 2 || ctx.Index >= len(ctx.Candles) {
		return Vote{Agent: "micro", Action: ActionHold, Weight: 0.6}
	}
	c := ctx.Candles[ctx.Index]
	if c.IsBullish() && ctx.Candles[ctx.Index-1].IsBullish() {
		return Vote{Agent: "micro", Action: ActionBuy, Weight: 0.6, Reason: "bullish micro"}
	}
	if c.IsBearish() && ctx.Candles[ctx.Index-1].IsBearish() {
		return Vote{Agent: "micro", Action: ActionSell, Weight: 0.6, Reason: "bearish micro"}
	}
	return Vote{Agent: "micro", Action: ActionHold, Weight: 0.6}
}

// AggTradeImbalance computes buy-sell volume imbalance from TimeAndSale (-1..1).
func AggTradeImbalance(trades []trade.TimeAndSale) float64 {
	if len(trades) == 0 {
		return 0
	}
	var buy, sell float64
	for _, t := range trades {
		v := float64(t.Volume)
		switch t.AggressorSide {
		case trade.AggressorBuy:
			buy += v
		case trade.AggressorSell:
			sell += v
		}
	}
	total := buy + sell
	if total == 0 {
		return 0
	}
	return (buy - sell) / total
}
