package polymarket

import (
	"context"
	"encoding/json"
	"net/url"
	"time"
)

type gammaEvent struct {
	Slug      string        `json:"slug"`
	StartDate string        `json:"startDate"`
	EndDate   string        `json:"endDate"`
	Markets   []gammaMarket `json:"markets"`
}

type gammaMarket struct {
	ConditionID   string `json:"conditionId"`
	ClobTokenIds  string `json:"clobTokenIds"`
	Outcomes      string `json:"outcomes"`
	OutcomePrices string `json:"outcomePrices"`
	EndDate       string `json:"endDate"`
}

// FetchEventBySlug loads event metadata from Gamma API.
func (c *Client) FetchEventBySlug(ctx context.Context, slug string) (*ResolvedEvent, error) {
	q := url.Values{"slug": {slug}}
	var events []gammaEvent
	if err := c.getJSON(ctx, c.gamma, "/events", q, &events); err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, ErrNotFound
	}
	return parseResolvedEvent(&events[0])
}

func parseResolvedEvent(ev *gammaEvent) (*ResolvedEvent, error) {
	if len(ev.Markets) == 0 {
		return nil, ErrNotFound
	}
	m := ev.Markets[0]
	var tokenIDs []string
	if err := json.Unmarshal([]byte(m.ClobTokenIds), &tokenIDs); err != nil || len(tokenIDs) < 2 {
		return nil, ErrNotFound
	}
	var outcomes []string
	_ = json.Unmarshal([]byte(m.Outcomes), &outcomes)

	upIdx, downIdx := 0, 1
	for i, o := range outcomes {
		switch o {
		case "Up", "Yes":
			upIdx = i
		case "Down", "No":
			downIdx = i
		}
	}
	if upIdx >= len(tokenIDs) || downIdx >= len(tokenIDs) {
		return nil, ErrNotFound
	}

	ws := parseTime(ev.StartDate)
	we := parseTime(m.EndDate)
	if we.IsZero() {
		we = parseTime(ev.EndDate)
	}

	return &ResolvedEvent{
		Slug:        ev.Slug,
		ConditionID: m.ConditionID,
		UpTokenID:   tokenIDs[upIdx],
		DownTokenID: tokenIDs[downIdx],
		WindowStart: ws,
		WindowEnd:   we,
	}, nil
}

func parseTime(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t, err = time.Parse(time.RFC3339Nano, s)
	}
	if err != nil {
		return time.Time{}
	}
	return t.UTC()
}
