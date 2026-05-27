package features

// OHLC holds aligned price series (same length slices).
type OHLC struct {
	Open   []float64
	High   []float64
	Low    []float64
	Close  []float64
	Volume []float64
}

// Len returns the number of bars; 0 if series differ in length.
func (o OHLC) Len() int {
	n := len(o.Close)
	if len(o.High) != n || len(o.Low) != n || len(o.Open) != n {
		return 0
	}
	if len(o.Volume) > 0 && len(o.Volume) != n {
		return 0
	}
	return n
}

// ClosesFrom extracts close prices from a slice of values.
func ClosesFrom(closes []float64) []float64 {
	out := make([]float64, len(closes))
	copy(out, closes)
	return out
}
