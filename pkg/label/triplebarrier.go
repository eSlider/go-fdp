package label

// Outcome is the triple-barrier label: which barrier was hit first.
type Outcome int8

const (
	OutcomeNone Outcome = 0
	OutcomeUp   Outcome = 1  // take-profit (long win / short loss)
	OutcomeDown Outcome = -1 // stop-loss (long loss / short win)
)

// Config defines vertical and horizontal barriers from entry.
type Config struct {
	TPMult   float64 // take-profit distance in ATR multiples
	SLMult   float64 // stop-loss distance in ATR multiples
	MaxHold  int     // vertical barrier in bars
}

// DefaultConfig matches the plan defaults (TP=2×ATR, SL=1.5×ATR, 24 bars).
func DefaultConfig() Config {
	return Config{TPMult: 2, SLMult: 1.5, MaxHold: 24}
}

// LabelAt applies triple-barrier labeling for a long entry at index i.
// high, low, close, atr must share length; atr[i] is entry ATR.
func LabelAt(high, low, close, atr []float64, i int, cfg Config) Outcome {
	n := len(close)
	if i < 0 || i >= n || cfg.MaxHold < 1 {
		return OutcomeNone
	}
	entry := close[i]
	atrv := atr[i]
	if entry <= 0 || atrv <= 0 || atrv != atrv {
		return OutcomeNone
	}
	tp := entry + cfg.TPMult*atrv
	sl := entry - cfg.SLMult*atrv
	end := i + cfg.MaxHold
	if end >= n {
		end = n - 1
	}
	for j := i + 1; j <= end; j++ {
		if high[j] >= tp {
			return OutcomeUp
		}
		if low[j] <= sl {
			return OutcomeDown
		}
	}
	return OutcomeNone
}

// LabelSeries labels each index for long entries (OutcomeNone where invalid).
func LabelSeries(high, low, close, atr []float64, cfg Config) []Outcome {
	n := len(close)
	out := make([]Outcome, n)
	for i := 0; i < n; i++ {
		out[i] = LabelAt(high, low, close, atr, i, cfg)
	}
	return out
}

// PurgeEmbargo splits indices into train/test with embargo bars between folds.
// Returns train and test index slices for fold k of nFolds.
func PurgeEmbargo(n, fold, nFolds, embargo int) (train, test []int) {
	if nFolds < 2 || fold < 0 || fold >= nFolds || n <= 0 {
		return nil, nil
	}
	chunk := n / nFolds
	start := fold * chunk
	end := start + chunk
	if fold == nFolds-1 {
		end = n
	}
	test = make([]int, 0, end-start)
	for i := start; i < end; i++ {
		test = append(test, i)
	}
	train = make([]int, 0, n-(end-start))
	for i := 0; i < n; i++ {
		if i >= start-embargo && i < end+embargo {
			continue
		}
		train = append(train, i)
	}
	return train, test
}
