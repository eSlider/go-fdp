package hourplan

import "time"

// Op describes what to do for one UTC hour on a calendar day.
type Op int

const (
	OpSealCompleted Op = iota
	OpRefreshOpen
	OpSkip
)

// Step is one hour action in a plan.
type Step struct {
	Hour int
	Op   Op
}

// PlanHours returns hour steps for [fromHour, toHour] on day in UTC.
func PlanHours(day time.Time, fromHour, toHour, currentHour int) []Step {
	if fromHour < 0 {
		fromHour = 0
	}
	if toHour > 23 {
		toHour = 23
	}
	if toHour > currentHour {
		toHour = currentHour
	}
	out := make([]Step, 0, toHour-fromHour+1)
	for hour := fromHour; hour <= toHour; hour++ {
		op := OpSealCompleted
		if hour > currentHour {
			op = OpSkip
		} else if hour == currentHour && day.UTC().Truncate(24*time.Hour).Equal(time.Now().UTC().Truncate(24*time.Hour)) {
			op = OpRefreshOpen
		}
		if op == OpSkip {
			continue
		}
		out = append(out, Step{Hour: hour, Op: op})
	}
	return out
}
