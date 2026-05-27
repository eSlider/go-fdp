package features

// RSI computes Wilder's RSI; out[i] is NaN until enough samples exist.
func RSI(closes []float64, period int) []float64 {
	n := len(closes)
	out := make([]float64, n)
	if period < 1 || n == 0 {
		return out
	}
	for i := 0; i < n; i++ {
		out[i] = nan()
	}
	if n <= period {
		return out
	}

	var gain, loss float64
	for i := 1; i <= period; i++ {
		d := closes[i] - closes[i-1]
		if d >= 0 {
			gain += d
		} else {
			loss -= d
		}
	}
	gain /= float64(period)
	loss /= float64(period)
	out[period] = rsiValue(gain, loss)

	for i := period + 1; i < n; i++ {
		d := closes[i] - closes[i-1]
		var g, l float64
		if d >= 0 {
			g = d
		} else {
			l = -d
		}
		gain = (gain*float64(period-1) + g) / float64(period)
		loss = (loss*float64(period-1) + l) / float64(period)
		out[i] = rsiValue(gain, loss)
	}
	return out
}

func rsiValue(avgGain, avgLoss float64) float64 {
	if avgLoss == 0 {
		if avgGain == 0 {
			return 50
		}
		return 100
	}
	rs := avgGain / avgLoss
	return 100 - (100 / (1 + rs))
}
