package domain

import (
	"context"

	"sync-v3/pkg/binance"
)

type MarketRepository interface {
	GetCandles(ctx context.Context, req MarketDataRequest) ([]*Candle, error)
	GetAggTrades(ctx context.Context, req MarketDataRequest) ([]*AggTrade, error)
}

type HistoryConsumer interface {
	DownloadAndTransform(asset *binance.HistoryAsset) (chan *binance.AssetETLInfo, chan error)
}

type MarketService interface {
	GetMarketHistory(ctx context.Context, req MarketDataRequest) ([]*Candle, error)
	GetAggTrades(ctx context.Context, req MarketDataRequest) ([]*AggTrade, error)
	GetMarkets(ctx context.Context) ([]string, error)
	GetSymbols(ctx context.Context) ([]string, error)
}
