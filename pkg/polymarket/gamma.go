package polymarket

import (
	"context"
	"encoding/json"
	"net/url"
	"strconv"
)

type gammaEvent struct {
	Slug      string        `json:"slug"`
	StartDate Time          `json:"startDate"`
	EndDate   Time          `json:"endDate"`
	Markets   []gammaMarket `json:"markets"`
}

type gammaMarket struct {
	ConditionID   string `json:"conditionId"`
	ClobTokenIds  string `json:"clobTokenIds"`
	Outcomes      string `json:"outcomes"`
	OutcomePrices string `json:"outcomePrices"`
	EndDate       Time   `json:"endDate"`
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

	ws := ev.StartDate.Time()
	we := m.EndDate.Time()
	if we.IsZero() {
		we = ev.EndDate.Time()
	}

	out := &ResolvedEvent{
		Slug:        ev.Slug,
		ConditionID: m.ConditionID,
		UpTokenID:   tokenIDs[upIdx],
		DownTokenID: tokenIDs[downIdx],
		WindowStart: ws,
		WindowEnd:   we,
	}
	if up, down, ok := parseOutcomePrices(m.OutcomePrices, upIdx, downIdx); ok {
		out.OutcomeUp = up
		out.OutcomeDown = down
	}
	return out, nil
}

func parseOutcomePrices(raw string, upIdx, downIdx int) (float64, float64, bool) {
	if raw == "" {
		return 0, 0, false
	}
	var prices []string
	if err := json.Unmarshal([]byte(raw), &prices); err != nil {
		return 0, 0, false
	}
	if upIdx >= len(prices) || downIdx >= len(prices) {
		return 0, 0, false
	}
	up, err := strconv.ParseFloat(prices[upIdx], 64)
	if err != nil || up < 0 || up > 1 {
		return 0, 0, false
	}
	down, err := strconv.ParseFloat(prices[downIdx], 64)
	if err != nil || down < 0 || down > 1 {
		return 0, 0, false
	}
	return up, down, true
}
