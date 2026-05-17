package v3

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

// CandleRequest is the query for GET /api/v3/klines.
type CandleRequest struct {
	Base     SymbolRequest
	Interval string  `in:"query=interval;required" validate:"required"`
	TimeZone *string `in:"query=timeZone;omitempty"`
	Limit    int64   `in:"query=limit;omitempty"`
}
