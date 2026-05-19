package integrity

// TargetsForIssues returns audit targets that need repair for the given issues.
func TargetsForIssues(targets []HourlyTarget, issues []Issue) []HourlyTarget {
	if len(issues) == 0 {
		return nil
	}
	for _, iss := range issues {
		if iss.Code == CodeHourBoundaryGap && iss.Severity == SeverityError {
			return targets
		}
	}
	out := make([]HourlyTarget, 0, len(targets))
	seen := make(map[int]struct{})
	for _, t := range targets {
		if _, ok := seen[t.Hour]; ok {
			continue
		}
		for _, iss := range issues {
			if iss.AffectsTarget(t) {
				seen[t.Hour] = struct{}{}
				out = append(out, t)
				break
			}
		}
	}
	return out
}

// AffectsTarget reports whether an error issue applies to the given hourly target.
func (i Issue) AffectsTarget(t HourlyTarget) bool {
	if i.Severity != SeverityError {
		return false
	}
	switch i.Code {
	case CodeCountMismatch:
		return int64(t.Hour) == i.OpenTime
	case CodeMissingInterval, CodeWrongStep,
		CodeDuplicateOpenTime, CodeOpenTimeNotIncreasing:
		return HourOverlapsTarget(t, i)
	default:
		return false
	}
}

// HourOverlapsTarget reports whether the issue open time falls in the target window.
func HourOverlapsTarget(t HourlyTarget, iss Issue) bool {
	if iss.OpenTime == 0 {
		return true
	}
	return iss.OpenTime >= t.HourStart.UnixMilli() && iss.OpenTime < t.HourEnd.UnixMilli()
}
