package polymarket

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseOutcomePrices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		upIdx   int
		downIdx int
		wantUp  float64
		wantDn  float64
		wantOK  bool
	}{
		{
			name:    "up down",
			raw:     `["0.455", "0.545"]`,
			upIdx:   0,
			downIdx: 1,
			wantUp:  0.455,
			wantDn:  0.545,
			wantOK:  true,
		},
		{
			name:    "empty",
			raw:     ``,
			upIdx:   0,
			downIdx: 1,
			wantOK:  false,
		},
		{
			name:    "invalid",
			raw:     `["x"]`,
			upIdx:   0,
			downIdx: 1,
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			up, dn, ok := parseOutcomePrices(tt.raw, tt.upIdx, tt.downIdx)
			assert.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				assert.InDelta(t, tt.wantUp, up, 1e-9)
				assert.InDelta(t, tt.wantDn, dn, 1e-9)
			}
		})
	}
}

func TestParseResolvedEvent_outcomePrices(t *testing.T) {
	t.Parallel()

	ev := &gammaEvent{
		Slug:      "btc-updown-5m-1",
		StartDate: mustTime(t, "2026-05-20T09:50:00Z"),
		EndDate:   mustTime(t, "2026-05-20T10:00:00Z"),
		Markets: []gammaMarket{{
			ClobTokenIds:  `["upTok","downTok"]`,
			Outcomes:      `["Up","Down"]`,
			OutcomePrices: `["0.455", "0.545"]`,
			EndDate:       mustTime(t, "2026-05-20T09:55:00Z"),
		}},
	}
	res, err := parseResolvedEvent(ev)
	require.NoError(t, err)
	assert.InDelta(t, 0.455, res.OutcomeUp, 1e-9)
	assert.InDelta(t, 0.545, res.OutcomeDown, 1e-9)
}
