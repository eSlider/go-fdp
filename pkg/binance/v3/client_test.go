package v3

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_GetCurrentAggTrades(t *testing.T) {

	now := time.Now().UTC()
	start := now.Add(-10 * time.Minute)

	raw, err := NewClient().AggTrades(&AggTradeRequest{
		Base: SymbolRequest{
			Symbol:    "BTCUSDT",
			StartTime: new(start.UnixMilli()),
			EndTime:   new(now.UnixMilli()),
		},
		Limit: 100,
	})
	require.NoError(t, err)
	trades := raw
	require.NotEmpty(t, trades)

	first := trades[0]
	assert.NotZero(t, first.AggTradeID)
	assert.NotZero(t, first.Price)
	assert.NotZero(t, first.Timestamp)
	assert.LessOrEqual(t, first.FirstTradeID, first.LastTradeID)

	for _, trade := range trades {
		ts := time.UnixMilli(trade.Timestamp)
		assert.False(t, ts.Before(start))
		assert.False(t, ts.After(now))
	}
}
