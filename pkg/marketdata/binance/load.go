package binancemd

import (
	"context"
	"time"

	trade "github.com/eslider/go-trade"

	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/features"
)

// LoadCandles paginates Binance REST klines into go-trade candles for [from, to].
func LoadCandles(ctx context.Context, symbol, interval string, from, to time.Time) ([]trade.Candle, error) {
	const page = 1000
	var all []*binance.Kline
	cursor := from.UnixMilli()
	endMs := to.UnixMilli()

	for cursor < endMs {
		start := cursor
		klines, err := binance.FetchKlines(ctx, &binance.KlineRequest{
			Base: binance.SymbolRequest{
				Symbol:    symbol,
				StartTime: &start,
				EndTime:   &endMs,
			},
			Interval: interval,
			Limit:    page,
		})
		if err != nil {
			return nil, err
		}
		if len(klines) == 0 {
			break
		}
		all = append(all, klines...)
		last := klines[len(klines)-1]
		next := last.CloseTime + 1
		if next <= cursor {
			break
		}
		cursor = next
		if len(klines) < page {
			break
		}
	}
	return features.CandlesFromBinance(all), nil
}
