package features

// SMA computes a simple moving average; out[i] is NaN until period-1 samples exist.
func SMA(values []float64, period int) []float64 {
	n := len(values)
	out := make([]float64, n)
	if period < 1 || n == 0 {
		return out
	}
	for i := 0; i < n; i++ {
		if i < period-1 {
			out[i] = nan()
			continue
		}
		var sum float64
		for j := i - period + 1; j <= i; j++ {
			sum += values[j]
		}
		out[i] = sum / float64(period)
	}
	return out
}
