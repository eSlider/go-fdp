package features

import "math"

func nan() float64 { return math.NaN() }

// IsNaN reports whether v is NaN.
func IsNaN(v float64) bool { return math.IsNaN(v) }
