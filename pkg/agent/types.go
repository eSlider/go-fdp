package agent

import (
	trade "github.com/eslider/go-trade"

	"github.com/eslider/go-fdp/pkg/marketdata/fdp"
	"github.com/eslider/go-fdp/pkg/strategy"
)

// Action is a trade action proposal.
type Action int8

const (
	ActionHold Action = 0
	ActionBuy  Action = 1
	ActionSell Action = -1
)

// Context is shared state for all agents at bar i.
type Context struct {
	Index     int
	Candles   []trade.Candle
	Features  []strategy.BarFeatures
	Predictions []fdp.Prediction
	Spot      float64
}

// Vote is one agent's opinion.
type Vote struct {
	Agent  string
	Action Action
	Weight float64
	Reason string
}

// Decision is the aggregated pipeline output.
type Decision struct {
	Action Action
	Votes  []Vote
	Reason string
}
