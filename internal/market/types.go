package market

import "github.com/eslider/go-binance-fdp/internal/query"

type MarketType = query.MarketType
type Indicator = query.Indicator
type Candle = query.Candle
type AggTrade = query.AggTrade
type Query = query.Query

const (
	Spot      = query.Spot
	Futures   = query.Futures
	Option    = query.Option
	Klines    = query.Klines
	Trades    = query.Trades
	AggTrades = query.AggTrades
)

var NewMarketType = query.NewMarketType
