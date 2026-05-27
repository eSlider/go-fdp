package agent

import (
	"testing"

	trade "github.com/eslider/go-trade"
	"github.com/stretchr/testify/assert"
)

func TestPipeline_RiskVeto(t *testing.T) {
	pipe := Pipeline{Agents: []Agent{
		RiskAgent{Limits: RiskLimits{MaxDailyLossPct: 0.01, DailyLossPct: 0.02}},
	}}
	dec := pipe.Decide(Context{})
	assert.Equal(t, ActionHold, dec.Action)
	assert.Equal(t, "risk veto", dec.Reason)
}

func TestPipeline_ConsensusBuy(t *testing.T) {
	candles := []trade.Candle{{Close: 100}, {Close: 101}}
	pipe := Pipeline{Agents: []Agent{
		voteAgent{name: "a", action: ActionBuy, w: 1},
		voteAgent{name: "b", action: ActionBuy, w: 1},
		RiskAgent{},
	}}
	dec := pipe.Decide(Context{Index: 1, Candles: candles})
	assert.Equal(t, ActionBuy, dec.Action)
}

type voteAgent struct {
	name   string
	action Action
	w      float64
}

func (v voteAgent) Name() string { return v.name }
func (v voteAgent) Vote(Context) Vote {
	return Vote{Agent: v.name, Action: v.action, Weight: v.w}
}
