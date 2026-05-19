package etl

// Source identifies a data provider (matches API ?exchange=).
type Source string

const (
	SourceBinance       Source = "binance"
	SourceBitfinex      Source = "bitfinex"
	SourcePolymarketAvg Source = "polymarket_avg"
)
