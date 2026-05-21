package polymarket

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTime_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		raw     string
		wantUTC string
		wantErr bool
	}{
		{
			name:    "RFC3339 quoted",
			raw:     `"2026-05-20T09:50:00Z"`,
			wantUTC: "2026-05-20T09:50:00Z",
		},
		{
			name:    "RFC3339Nano quoted",
			raw:     `"2026-05-20T09:50:00.123456789Z"`,
			wantUTC: "2026-05-20T09:50:00.123456789Z",
		},
		{
			name: "null",
			raw:  `null`,
		},
		{
			name:    "empty string",
			raw:     `""`,
			wantErr: false,
		},
		{
			name:    "invalid",
			raw:     `"not-a-time"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var got Time
			err := json.Unmarshal([]byte(tt.raw), &got)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("UnmarshalJSON: %v", err)
			}
			if tt.wantUTC == "" {
				if !got.IsZero() {
					t.Fatalf("want zero, got %v", got.Time())
				}
				return
			}
			want, err := time.Parse(time.RFC3339Nano, tt.wantUTC)
			if err != nil {
				want, err = time.Parse(time.RFC3339, tt.wantUTC)
				if err != nil {
					t.Fatalf("parse want: %v", err)
				}
			}
			if !got.Time().Equal(want.UTC()) {
				t.Fatalf("got %v want %v", got.Time(), want.UTC())
			}
		})
	}
}

func TestParseResolvedEvent_windowEndFallback(t *testing.T) {
	t.Parallel()

	ev := &gammaEvent{
		Slug:      "btc-updown-5m-1",
		StartDate: mustTime(t, "2026-05-20T09:50:00Z"),
		EndDate:   mustTime(t, "2026-05-20T10:00:00Z"),
		Markets: []gammaMarket{{
			ClobTokenIds: `["up","down"]`,
			Outcomes:     `["Up","Down"]`,
			EndDate:      mustTime(t, "2026-05-20T09:55:00Z"),
		}},
	}
	res, err := parseResolvedEvent(ev)
	if err != nil {
		t.Fatalf("parseResolvedEvent: %v", err)
	}
	if res.WindowEnd.Format(time.RFC3339) != "2026-05-20T09:55:00Z" {
		t.Fatalf("WindowEnd = %s", res.WindowEnd.Format(time.RFC3339))
	}
}

func mustTime(t *testing.T, s string) Time {
	t.Helper()
	var dt Time
	if err := json.Unmarshal([]byte(`"`+s+`"`), &dt); err != nil {
		t.Fatalf("mustTime: %v", err)
	}
	return dt
}
