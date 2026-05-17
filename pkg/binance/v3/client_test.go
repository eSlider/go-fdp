package v3

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_GetCurrentAggTrades(t *testing.T) {
	ctx := t.Context()
	now := time.Now().UTC()
	start := now.Add(-10 * time.Minute)

	trades, err := AggTrades(ctx, &AggTradeRequest{
		Base: SymbolRequest{
			Symbol:    "BTCUSDT",
			StartTime: new(start.UnixMilli()),
			EndTime:   new(now.UnixMilli()),
		},
		Limit: 100,
	})
	require.NoError(t, err)
	require.NotEmpty(t, trades)
	for _, trade := range trades {
		assert.NotZero(t, trade.AggTradeID)
		assert.NotZero(t, trade.Price)
		assert.NotZero(t, trade.Timestamp)
		assert.LessOrEqual(t, trade.FirstTradeID, trade.LastTradeID)
		ts := time.UnixMilli(trade.Timestamp)
		assert.False(t, ts.Before(start))
		assert.False(t, ts.After(now))
	}

	candles, err := Klines(ctx, &KlineRequest{
		Base: SymbolRequest{
			Symbol:    "BTCUSDT",
			StartTime: new(start.UnixMilli()),
			EndTime:   new(now.UnixMilli()),
		},

		Interval: "1m",
		Limit:    10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, candles)
	for _, candle := range candles {
		assert.NotZero(t, candle.ClosePrice)
		assert.NotZero(t, candle.OpenTime)
	}
}
