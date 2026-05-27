package agent

import (
	"github.com/eslider/go-fdp/pkg/features"
)

// TrendAgent uses higher-timeframe SMA200 bias (features already on signal TF).
type TrendAgent struct{}

func (TrendAgent) Name() string { return "trend" }

func (TrendAgent) Vote(ctx Context) Vote {
	if ctx.Index >= len(ctx.Features) {
		return Vote{Agent: "trend", Action: ActionHold, Weight: 0.8}
	}
	f := ctx.Features[ctx.Index]
	if features.IsNaN(f.SMA200) {
		return Vote{Agent: "trend", Action: ActionHold, Weight: 0.8, Reason: "no sma"}
	}
	if f.Close > f.SMA200 {
		return Vote{Agent: "trend", Action: ActionBuy, Weight: 0.8, Reason: "above sma200"}
	}
	if f.Close < f.SMA200 {
		return Vote{Agent: "trend", Action: ActionSell, Weight: 0.8, Reason: "below sma200"}
	}
	return Vote{Agent: "trend", Action: ActionHold, Weight: 0.8}
}
