package strategy

import (
	"time"

	"github.com/eslider/go-fdp/pkg/features"
	"github.com/eslider/go-fdp/pkg/marketdata/fdp"
)

// PolyFeatures holds Polymarket-derived features at a point in time.
type PolyFeatures struct {
	UpProb     float64
	StrikeGap  float64 // spot - implied strike (USD); 0 if unknown
	WindowEnd  time.Time
}

// StrikeGap computes start - implied strike from Up probability and realized vol.
func StrikeGap(start, upProb, sigma, spot float64) float64 {
	strike := features.ImpliedStrike(start, upProb, sigma)
	if strike <= 0 {
		return 0
	}
	return start - strike
}

// NearestPrediction picks the latest prediction at or before t.
func NearestPrediction(preds []fdp.Prediction, t time.Time) (fdp.Prediction, bool) {
	var best fdp.Prediction
	ok := false
	for _, p := range preds {
		if p.Time.After(t) {
			continue
		}
		if !ok || p.Time.After(best.Time) {
			best = p
			ok = true
		}
	}
	return best, ok
}

// PolyFilter skips long signals when crowd P(Up) is below minUp for active window.
func PolyFilter(signals []Signal, candleTimes []time.Time, preds []fdp.Prediction, minUp float64) []Signal {
	if minUp <= 0 || len(preds) == 0 {
		return signals
	}
	out := make([]Signal, 0, len(signals))
	for _, s := range signals {
		if s.Side != SideLong {
			out = append(out, s)
			continue
		}
		if s.Index >= len(candleTimes) {
			continue
		}
		p, ok := NearestPrediction(preds, candleTimes[s.Index])
		if !ok || p.UpPrice < minUp {
			continue
		}
		out = append(out, s)
	}
	return out
}
