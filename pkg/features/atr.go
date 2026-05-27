package features

// ATR computes average true range (SMA of TR over period).
func ATR(high, low, close []float64, period int) []float64 {
	n := len(close)
	out := make([]float64, n)
	if period < 1 || n == 0 || len(high) != n || len(low) != n {
		return out
	}
	tr := make([]float64, n)
	for i := 0; i < n; i++ {
		if i == 0 {
			tr[i] = high[i] - low[i]
			continue
		}
		hl := high[i] - low[i]
		hc := abs(high[i] - close[i-1])
		lc := abs(low[i] - close[i-1])
		tr[i] = max3(hl, hc, lc)
	}
	return SMA(tr, period)
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func max3(a, b, c float64) float64 {
	m := a
	if b > m {
		m = b
	}
	if c > m {
		m = c
	}
	return m
}
