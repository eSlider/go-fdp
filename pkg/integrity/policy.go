package integrity

import "time"

// HasErrors returns true if any issue has error severity.
func HasErrors(issues []Issue) bool {
	for _, i := range issues {
		if i.IsError() {
			return true
		}
	}
	return false
}

// HasStructuralErrors returns true for duplicate, ordering, or step issues.
func HasStructuralErrors(issues []Issue) bool {
	for _, i := range issues {
		if i.IsStructural() {
			return true
		}
	}
	return false
}

// HasBlockingGaps returns true for gaps that must block API responses.
func HasBlockingGaps(issues []Issue) bool {
	for _, i := range issues {
		if i.BlocksRead() {
			return true
		}
	}
	return false
}

// HasGapIssues returns true when data is incomplete or discontinuous (triggers ETL repair).
func HasGapIssues(issues []Issue) bool {
	for _, i := range issues {
		if i.NeedsRepair() {
			return true
		}
	}
	return false
}

// IssuesAffectingRange returns issues that impact [from, to).
func IssuesAffectingRange(issues []Issue, from, to time.Time) []Issue {
	out := make([]Issue, 0, len(issues))
	for _, iss := range issues {
		if iss.AffectsRange(from, to) {
			out = append(out, iss)
		}
	}
	return out
}
