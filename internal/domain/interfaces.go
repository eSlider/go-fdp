package domain

import (
	"context"
)

type MarketRepository interface {
	GetCandles(ctx context.Context, req MarketDataRequest) ([]*Candle, error)
	// Add other methods as needed, e.g., for Trades
}

type MarketService interface {
	GetMarketHistory(ctx context.Context, req MarketDataRequest) ([]*Candle, error)
	GetMarkets(ctx context.Context) ([]string, error)
	GetSymbols(ctx context.Context) ([]string, error)
}
