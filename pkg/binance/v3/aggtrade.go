package v3

import "encoding/json"

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

func (a *AggTrade) UnmarshalJSON(data []byte) error {
	var wire struct {
		AggTradeID     int64  `json:"a"`
		Price          string `json:"p"`
		Quantity       string `json:"q"`
		FirstTradeID   int64  `json:"f"`
		LastTradeID    int64  `json:"l"`
		Timestamp      int64  `json:"T"`
		IsBuyerMaker   bool   `json:"m"`
		BestPriceMatch bool   `json:"M"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}

	a.AggTradeID = wire.AggTradeID
	a.Price = parseFloat(wire.Price)
	a.Quantity = parseFloat(wire.Quantity)
	a.FirstTradeID = wire.FirstTradeID
	a.LastTradeID = wire.LastTradeID
	a.Timestamp = wire.Timestamp
	a.IsBuyerMaker = wire.IsBuyerMaker
	a.IsBestPriceMatch = wire.BestPriceMatch
	return nil
}
