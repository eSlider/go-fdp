package agent

// RiskLimits configures deterministic risk agent (never LLM).
type RiskLimits struct {
	MaxDailyLossPct float64
	MaxPositionPct  float64
	DailyLossPct    float64 // current day loss (set by broker)
	OpenExposurePct float64
}

// RiskAgent vetoes trades when limits are breached.
type RiskAgent struct {
	Limits RiskLimits
}

func (RiskAgent) Name() string { return "risk" }

func (a RiskAgent) Vote(ctx Context) Vote {
	lim := a.Limits
	if lim.MaxDailyLossPct > 0 && lim.DailyLossPct >= lim.MaxDailyLossPct {
		return Vote{Agent: "risk", Action: ActionHold, Weight: 10, Reason: "daily loss kill"}
	}
	if lim.MaxPositionPct > 0 && lim.OpenExposurePct >= lim.MaxPositionPct {
		return Vote{Agent: "risk", Action: ActionHold, Weight: 10, Reason: "max exposure"}
	}
	return Vote{Agent: "risk", Action: ActionHold, Weight: 10, Reason: "risk ok"}
}

// RiskVeto returns true if risk agent blocks any directional action.
func RiskVeto(votes []Vote) bool {
	for _, v := range votes {
		if v.Agent == "risk" && v.Weight >= 10 && v.Reason != "risk ok" {
			return true
		}
	}
	return false
}
