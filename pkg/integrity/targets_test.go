package integrity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIssueAffectsTarget_CountMismatchUsesHourIndex(t *testing.T) {
	midnight := time.Date(2026, 5, 17, 0, 0, 0, 0, time.UTC)
	target := HourlyTarget{
		Hour:      18,
		HourStart: midnight.Add(18 * time.Hour),
		HourEnd:   midnight.Add(19 * time.Hour),
	}
	issue := Issue{
		Code:     CodeCountMismatch,
		Severity: SeverityError,
		OpenTime: 18,
		Detail:   "got 35 expected 36",
	}
	assert.True(t, issue.AffectsTarget(target))
	assert.False(t, HourOverlapsTarget(target, issue))
}

func TestTargetsForIssues_CountMismatchSelectsHour(t *testing.T) {
	midnight := time.Date(2026, 5, 17, 0, 0, 0, 0, time.UTC)
	targets := []HourlyTarget{
		{Hour: 17, HourStart: midnight.Add(17 * time.Hour), HourEnd: midnight.Add(18 * time.Hour)},
		{Hour: 18, HourStart: midnight.Add(18 * time.Hour), HourEnd: midnight.Add(19 * time.Hour)},
	}
	issues := []Issue{{
		Code:     CodeCountMismatch,
		Severity: SeverityError,
		OpenTime: 18,
	}}
	got := TargetsForIssues(targets, issues)
	require.Len(t, got, 1)
	assert.Equal(t, 18, got[0].Hour)
}
