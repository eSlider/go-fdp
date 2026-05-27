package paper

import (
	"time"

	trade "github.com/eslider/go-trade"
)

// Side is position direction.
type Side int8

const (
	SideFlat  Side = 0
	SideLong  Side = 1
	SideShort Side = -1
)

// Position is an open paper position.
type Position struct {
	Market    trade.Market
	Side      Side
	EntryTime time.Time
	EntryPx   float64
	Qty       float64
	StopLoss  float64
	TakeProfit float64
}

// Fill records an executed paper fill.
type Fill struct {
	Time   time.Time
	Market trade.Market
	Side   Side
	Price  float64
	Qty    float64
	Fee    float64
	PnL    float64
}
