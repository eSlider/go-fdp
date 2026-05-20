package polymarket

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"
)

type pricePoint struct {
	T int64   `json:"t"`
	P float64 `json:"p"`
}

type pricesHistoryResponse struct {
	History []pricePoint `json:"history"`
}

type clobPriceResponse struct {
	Price string `json:"price"`
}

type clobMidpointResponse struct {
	Mid string `json:"mid"`
}

// FetchPricesHistory returns CLOB price history for a token.
func (c *Client) FetchPricesHistory(ctx context.Context, tokenID string, start, end time.Time, fidelityMin int) ([]pricePoint, error) {
	q := url.Values{
		"market": {tokenID},
	}
	if !start.IsZero() {
		q.Set("startTs", fmt.Sprintf("%d", start.Unix()))
	}
	if !end.IsZero() {
		q.Set("endTs", fmt.Sprintf("%d", end.Unix()))
	}
	if fidelityMin > 0 {
		q.Set("fidelity", strconv.Itoa(fidelityMin))
	}
	var resp pricesHistoryResponse
	if err := c.getJSON(ctx, c.clob, "/prices-history", q, &resp); err != nil {
		return nil, err
	}
	return resp.History, nil
}

// FetchPrice returns the current implied probability for a token.
// Tries /midpoint first (no side argument); falls back to /price?side=BUY.
func (c *Client) FetchPrice(ctx context.Context, tokenID string) (float64, error) {
	mq := url.Values{"token_id": {tokenID}}
	var mid clobMidpointResponse
	if err := c.getJSON(ctx, c.clob, "/midpoint", mq, &mid); err == nil && mid.Mid != "" {
		if p, perr := strconv.ParseFloat(mid.Mid, 64); perr == nil {
			return p, nil
		}
	}
	q := url.Values{"token_id": {tokenID}, "side": {"BUY"}}
	var resp clobPriceResponse
	if err := c.getJSON(ctx, c.clob, "/price", q, &resp); err != nil {
		return 0, err
	}
	p, err := strconv.ParseFloat(resp.Price, 64)
	if err != nil {
		return 0, fmt.Errorf("parse price %q: %w", resp.Price, err)
	}
	return p, nil
}

func historyToSnapshots(ev *ResolvedEvent, history []pricePoint) []Snapshot {
	out := make([]Snapshot, 0, len(history))
	for _, pt := range history {
		ts := pt.T
		if ts > 0 && ts < 1e12 {
			ts *= 1000
		}
		t := time.UnixMilli(ts).UTC()
		up := pt.P
		out = append(out, Snapshot{
			Time:        t,
			UpPrice:     up,
			DownPrice:   1 - up,
			EventSlug:   ev.Slug,
			ConditionID: ev.ConditionID,
			WindowStart: ev.WindowStart,
			WindowEnd:   ev.WindowEnd,
		})
	}
	return out
}
