package market

import (
	"context"

	"github.com/eslider/go-fdp/internal/query"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/polymarket"
)

// PredictionsService handles Polymarket prediction reads and backfill.
type PredictionsService struct {
	Collector *polymarket.Collector
}

// Predictions returns prediction history for the query range (lazy backfill + store read).
func (a *API) Predictions(ctx context.Context, req query.PredictionQuery) ([]*query.Prediction, error) {
	if a.PredictionsSvc != nil && a.PredictionsSvc.Collector != nil {
		if err := a.PredictionsSvc.Collector.EnsureRange(ctx, req.Market, req.Frame, req.From, req.To); err != nil {
			return nil, err
		}
	}
	return a.Store.GetPredictions(ctx, req)
}

// NewPredictionQuery builds a query with defaults.
func NewPredictionQuery(market, exchange, frame string, from, to int64) query.PredictionQuery {
	m := market
	if m == "" {
		m = polymarket.DefaultMarket
	}
	ex := exchange
	if ex == "" {
		ex = polymarket.SourceID
	}
	f := data.NewFrame(frame)
	if frame == "" {
		f = data.FiveMinute
	}
	fromT := data.AnyTimestampToTime(from)
	toT := data.AnyTimestampToTime(to)
	if fromT == nil || toT == nil {
		return query.PredictionQuery{}
	}
	return query.PredictionQuery{
		From:     *fromT,
		To:       *toT,
		Market:   m,
		Exchange: ex,
		Frame:    f,
	}
}
