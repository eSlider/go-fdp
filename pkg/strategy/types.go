package strategy

// Side is a proposed trade direction.
type Side int8

const (
	SideFlat  Side = 0
	SideLong  Side = 1
	SideShort Side = -1
)

// Signal is a base-strategy output at bar index i.
type Signal struct {
	Index int
	Side  Side
}

// BarFeatures holds computed indicators at a bar (for meta-labeling).
type BarFeatures struct {
	RSI        float64
	MACDHist   float64
	SMA200     float64
	ATR        float64
	RealizedVol float64
	Close      float64
}
