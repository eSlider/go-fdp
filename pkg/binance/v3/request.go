package v3

import (
	"github.com/go-playground/validator/v10"
	"github.com/google/go-querystring/query"
)

// AggTradeRequest is the query for GET /api/v3/aggTrades.
// Ref: https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
type AggTradeRequest struct {
	Symbol string `url:"symbol" validate:"required"`

	FromID    *int64 `url:"fromId,omitempty"`
	StartTime *int64 `url:"startTime,omitempty"`
	EndTime   *int64 `url:"endTime,omitempty"`
	Limit     int64  `url:"limit,omitempty"`
}

func (r *AggTradeRequest) Validate() error {
	return validator.New().Struct(r)
}

func (r *AggTradeRequest) urlParams() string {
	v, _ := query.Values(r)
	return v.Encode()
}

// CandleRequest is the query for GET /api/v3/klines.
type CandleRequest struct {
	Symbol   string `url:"symbol" validate:"required"`
	Interval string `url:"interval" validate:"required"`

	StartTime *int64  `url:"startTime,omitempty"`
	EndTime   *int64  `url:"endTime,omitempty"`
	TimeZone  *string `url:"timeZone,omitempty"`
	Limit     int64   `url:"limit,omitempty"`
}

func (r *CandleRequest) Validate() error {
	return validator.New().Struct(r)
}

func (r *CandleRequest) urlParams() string {
	v, _ := query.Values(r)
	return v.Encode()
}
