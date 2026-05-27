package agent

import (
	"github.com/eslider/go-fdp/pkg/strategy"
)

// PredictionAgent uses Polymarket P(Up) vs spot/strike gap.
type PredictionAgent struct {
	MinUpProb float64
}

func (a PredictionAgent) Name() string { return "prediction" }

func (a PredictionAgent) Vote(ctx Context) Vote {
	minUp := a.MinUpProb
	if minUp <= 0 {
		minUp = 0.45
	}
	if ctx.Index >= len(ctx.Candles) {
		return Vote{Agent: a.Name(), Action: ActionHold, Weight: 1}
	}
	t := ctx.Candles[ctx.Index].TimeClose
	p, ok := strategy.NearestPrediction(ctx.Predictions, t)
	if !ok {
		return Vote{Agent: a.Name(), Action: ActionHold, Weight: 0.5, Reason: "no poly data"}
	}
	f := ctx.Features[ctx.Index]
	gap := strategy.StrikeGap(ctx.Spot, p.UpPrice, f.RealizedVol, ctx.Spot)
	if p.UpPrice >= minUp && gap >= 0 {
		return Vote{Agent: a.Name(), Action: ActionBuy, Weight: 1, Reason: "poly favors up"}
	}
	if p.UpPrice < 1-minUp {
		return Vote{Agent: a.Name(), Action: ActionSell, Weight: 0.8, Reason: "poly favors down"}
	}
	return Vote{Agent: a.Name(), Action: ActionHold, Weight: 0.5, Reason: "poly neutral"}
}
