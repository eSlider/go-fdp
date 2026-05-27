package strategy

// MetaConfig filters base signals (meta-labeling without external ML).
type MetaConfig struct {
	MinATRPct      float64 // skip if ATR/close below this (too quiet)
	MaxATRPct      float64 // skip if ATR/close above this (too volatile)
	MinRealizedVol float64
}

// DefaultMetaConfig returns conservative meta filters.
func DefaultMetaConfig() MetaConfig {
	return MetaConfig{
		MinATRPct:      0.001,
		MaxATRPct:      0.05,
		MinRealizedVol: 0.0001,
	}
}

// MetaFilter returns signals that pass meta-label checks.
func MetaFilter(signals []Signal, feats []BarFeatures, cfg MetaConfig) []Signal {
	if len(feats) == 0 {
		return nil
	}
	out := make([]Signal, 0, len(signals))
	for _, s := range signals {
		if s.Index < 0 || s.Index >= len(feats) {
			continue
		}
		f := feats[s.Index]
		if f.Close <= 0 || f.ATR <= 0 {
			continue
		}
		atrPct := f.ATR / f.Close
		if atrPct < cfg.MinATRPct || atrPct > cfg.MaxATRPct {
			continue
		}
		if f.RealizedVol > 0 && f.RealizedVol < cfg.MinRealizedVol {
			continue
		}
		out = append(out, s)
	}
	return out
}
