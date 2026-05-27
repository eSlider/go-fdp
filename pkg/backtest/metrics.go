package backtest

import (
	"math"

	"github.com/eslider/go-fdp/pkg/features"
)

func sharpeFromCurve(curve []float64) float64 {
	if len(curve) < 3 {
		return 0
	}
	rets := make([]float64, 0, len(curve)-1)
	for i := 1; i < len(curve); i++ {
		if curve[i-1] > 0 {
			rets = append(rets, (curve[i]-curve[i-1])/curve[i-1])
		}
	}
	if len(rets) < 2 {
		return 0
	}
	sd := features.Stddev(rets)
	if sd == 0 {
		return 0
	}
	var mean float64
	for _, r := range rets {
		mean += r
	}
	mean /= float64(len(rets))
	// Annualize assuming ~365 daily points; for bar-level use sqrt(bars/year) approx.
	return mean / sd * math.Sqrt(252)
}
