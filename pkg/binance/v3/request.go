package v3

import (
	"net/url"

	"github.com/go-playground/validator/v10"
	"github.com/gorilla/schema"
)

var (
	queryEncoder = schema.NewEncoder()
	validate     = validator.New()
)

// AggTradeRequest is the query for GET /api/v3/aggTrades.
// Ref: https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
type AggTradeRequest struct {
	Symbol    string `schema:"symbol" validate:"required"`
	StartTime *int64 `schema:"startTime,omitempty"`
	EndTime   *int64 `schema:"endTime,omitempty"`
	FromID    *int64 `schema:"fromId,omitempty"`
	Limit     int64  `schema:"limit,omitempty"`
}

func (r *AggTradeRequest) Validate() error {
	return validate.Struct(r)
}

func (r *AggTradeRequest) urlParams() (string, error) {
	return encodeQuery(r)
}

// CandleRequest is the query for GET /api/v3/klines.
type CandleRequest struct {
	Symbol    string  `schema:"symbol" validate:"required"`
	Interval  string  `schema:"interval" validate:"required"`
	StartTime *int64  `schema:"startTime,omitempty"`
	EndTime   *int64  `schema:"endTime,omitempty"`
	TimeZone  *string `schema:"timeZone,omitempty"`
	Limit     int64   `schema:"limit,omitempty"`
}

func (r *CandleRequest) Validate() error {
	return validate.Struct(r)
}

func (r *CandleRequest) urlParams() (string, error) {
	return encodeQuery(r)
}

func encodeQuery(v any) (string, error) {
	form := url.Values{}
	if err := queryEncoder.Encode(v, form); err != nil {
		return "", err
	}
	return form.Encode(), nil
}
