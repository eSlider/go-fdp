package agent

// Agent produces a vote for the current context.
type Agent interface {
	Name() string
	Vote(ctx Context) Vote
}

// Pipeline runs agents and aggregates weighted votes.
type Pipeline struct {
	Agents []Agent
}

// Decide aggregates votes; risk veto forces HOLD.
func (p Pipeline) Decide(ctx Context) Decision {
	votes := make([]Vote, 0, len(p.Agents))
	for _, a := range p.Agents {
		votes = append(votes, a.Vote(ctx))
	}
	if RiskVeto(votes) {
		return Decision{Action: ActionHold, Votes: votes, Reason: "risk veto"}
	}

	var score float64
	for _, v := range votes {
		if v.Action == ActionHold {
			continue
		}
		score += float64(v.Action) * v.Weight
	}
	const thresh = 0.5
	switch {
	case score >= thresh:
		return Decision{Action: ActionBuy, Votes: votes, Reason: "consensus long"}
	case score <= -thresh:
		return Decision{Action: ActionSell, Votes: votes, Reason: "consensus short"}
	default:
		return Decision{Action: ActionHold, Votes: votes, Reason: "no consensus"}
	}
}

// DefaultPipeline returns the standard agent stack from the plan.
func DefaultPipeline() Pipeline {
	return Pipeline{Agents: []Agent{
		IndicatorAgent{},
		TrendAgent{},
		MicroAgent{},
		PredictionAgent{MinUpProb: 0.45},
		RiskAgent{Limits: RiskLimits{MaxDailyLossPct: 0.05, MaxPositionPct: 1}},
	}}
}
