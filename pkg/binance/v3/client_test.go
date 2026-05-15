package v3

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAggTradeRequestValidate(t *testing.T) {
	err := (&AggTradeRequest{}).Validate()
	require.Error(t, err)

	err = (&AggTradeRequest{Symbol: "BTCUSDT"}).Validate()
	require.NoError(t, err)
}

func TestCandleRequestValidate(t *testing.T) {
	err := (&CandleRequest{Symbol: "BTCUSDT"}).Validate()
	require.Error(t, err)

	err = (&CandleRequest{Symbol: "BTCUSDT", Interval: "1m"}).Validate()
	require.NoError(t, err)
}

func TestRequestURLParams(t *testing.T) {
	start := int64(1000)
	req := &AggTradeRequest{
		Symbol:    "ETHUSDT",
		StartTime: &start,
		Limit:     100,
	}
	params, err := req.urlParams()
	require.NoError(t, err)
	assert.Contains(t, params, "symbol=ETHUSDT")
	assert.Contains(t, params, "startTime=1000")
}
