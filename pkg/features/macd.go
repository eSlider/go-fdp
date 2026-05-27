package features

// MACD holds MACD line, signal line, and histogram (same length as input).
type MACD struct {
	Line      []float64
	Signal    []float64
	Histogram []float64
}

// MACDCompute returns MACD(12,26,9) from close prices.
func MACDCompute(closes []float64, fast, slow, signal int) MACD {
	n := len(closes)
	line := make([]float64, n)
	sig := make([]float64, n)
	hist := make([]float64, n)
	if fast < 1 || slow < 1 || signal < 1 || n == 0 {
		return MACD{Line: line, Signal: sig, Histogram: hist}
	}
	fastEMA := EMA(closes, fast)
	slowEMA := EMA(closes, slow)
	for i := 0; i < n; i++ {
		line[i] = fastEMA[i] - slowEMA[i]
	}
	sig = EMA(line, signal)
	for i := 0; i < n; i++ {
		hist[i] = line[i] - sig[i]
	}
	return MACD{Line: line, Signal: sig, Histogram: hist}
}

// MACDCrossUp is true when histogram crosses from <=0 to >0 at index i.
func MACDCrossUp(hist []float64, i int) bool {
	if i < 1 || i >= len(hist) || IsNaN(hist[i]) || IsNaN(hist[i-1]) {
		return false
	}
	return hist[i-1] <= 0 && hist[i] > 0
}

// MACDCrossDown is true when histogram crosses from >=0 to <0 at index i.
func MACDCrossDown(hist []float64, i int) bool {
	if i < 1 || i >= len(hist) || IsNaN(hist[i]) || IsNaN(hist[i-1]) {
		return false
	}
	return hist[i-1] >= 0 && hist[i] < 0
}
