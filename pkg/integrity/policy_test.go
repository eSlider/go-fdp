package integrity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestIssuesAffectingRange_CountMismatchByHour(t *testing.T) {
	from := time.Date(2026, 5, 17, 18, 30, 0, 0, time.UTC)
	to := time.Date(2026, 5, 17, 19, 30, 0, 0, time.UTC)
	issues := []Issue{{
		Code:     CodeCountMismatch,
		Severity: SeverityError,
		OpenTime: 18,
	}}
	filtered := IssuesAffectingRange(issues, from, to)
	assert.Len(t, filtered, 1)
	assert.False(t, HasGapIssues(IssuesAffectingRange([]Issue{{
		Code: CodeOutOfWindow, Severity: SeverityWarning, OpenTime: 1,
	}}, from, to)))
}

func TestHasGapIssues_IgnoresPartialHourCountWarning(t *testing.T) {
	assert.False(t, HasGapIssues([]Issue{{
		Code: CodeCountMismatch, Severity: SeverityWarning, OpenTime: 20,
	}}))
}
