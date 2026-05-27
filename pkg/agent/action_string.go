package agent

import "fmt"

// String returns a human-readable action name.
func (a Action) String() string {
	switch a {
	case ActionBuy:
		return "BUY"
	case ActionSell:
		return "SELL"
	default:
		return "HOLD"
	}
}

// FormatDecision logs a decision with votes.
func FormatDecision(d Decision) string {
	return fmt.Sprintf("%s (%s)", d.Action, d.Reason)
}
