package v3

// AggTrade is an aggregated trade returned by GET /api/v3/aggTrades.
type AggTrade struct {
	AggTradeID       int64
	Price            float64
	Quantity         float64
	FirstTradeID     int64
	LastTradeID      int64
	Timestamp        int64
	IsBuyerMaker     bool
	IsBestPriceMatch bool
}

func aggTradeFromResponse(r aggTradeResponse) AggTrade {
	return AggTrade{
		AggTradeID:       r.AggTradeID,
		Price:            parseFloat(r.Price),
		Quantity:         parseFloat(r.Quantity),
		FirstTradeID:     r.FirstTradeID,
		LastTradeID:      r.LastTradeID,
		Timestamp:        r.Timestamp,
		IsBuyerMaker:     r.IsBuyerMaker,
		IsBestPriceMatch: r.BestPriceMatch,
	}
}
