package v3

import (
	"net/http"

	"github.com/ggicci/httpin"
	"github.com/go-playground/validator/v10"
)

const encodeBaseURL = "http://localhost/"

var validate = validator.New()

// SymbolRequest is the common symbol and time-range query parameters.
type SymbolRequest struct {
	Symbol    string `in:"query=symbol;required" validate:"required"`
	StartTime *int64 `in:"query=startTime;omitempty"`
	EndTime   *int64 `in:"query=endTime;omitempty"`
}

// AggTradeRequest is the query for GET /api/v3/aggTrades.
// Ref: https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
type AggTradeRequest struct {
	Base   SymbolRequest
	FromID *int64 `in:"query=fromId;omitempty"`
	Limit  int64  `in:"query=limit;omitempty"`
}

func (r *AggTradeRequest) Validate() error {
	return validate.Struct(r)
}

func (r *AggTradeRequest) urlParams() (string, error) {
	return encodeQuery(r)
}

// CandleRequest is the query for GET /api/v3/klines.
type CandleRequest struct {
	Base     SymbolRequest
	Interval string  `in:"query=interval;required" validate:"required"`
	TimeZone *string `in:"query=timeZone;omitempty"`
	Limit    int64   `in:"query=limit;omitempty"`
}

func (r *CandleRequest) Validate() error {
	return validate.Struct(r)
}

func (r *CandleRequest) urlParams() (string, error) {
	return encodeQuery(r)
}

func encodeQuery(req any) (string, error) {
	httpReq, err := httpin.NewRequest(http.MethodGet, encodeBaseURL, req)
	if err != nil {
		return "", err
	}
	return httpReq.URL.RawQuery, nil
}
