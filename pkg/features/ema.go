package features

// EMA computes exponential moving average with span = period.
func EMA(values []float64, period int) []float64 {
	n := len(values)
	out := make([]float64, n)
	if period < 1 || n == 0 {
		return out
	}
	k := 2.0 / (float64(period) + 1)
	out[0] = values[0]
	for i := 1; i < n; i++ {
		out[i] = values[i]*k + out[i-1]*(1-k)
	}
	return out
}
