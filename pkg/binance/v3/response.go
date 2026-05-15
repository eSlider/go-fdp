package v3

// aggTradeResponse is the wire format for /api/v3/aggTrades entries.
type aggTradeResponse struct {
	AggTradeID     int64  `json:"a"`
	Price          string `json:"p"`
	Quantity       string `json:"q"`
	FirstTradeID   int64  `json:"f"`
	LastTradeID    int64  `json:"l"`
	Timestamp      int64  `json:"T"`
	IsBuyerMaker   bool   `json:"m"`
	BestPriceMatch bool   `json:"M"`
}
