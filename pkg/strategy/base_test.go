package strategy

import (
	"testing"

	trade "github.com/eslider/go-trade"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func synthCandles(n int, start float64) []trade.Candle {
	out := make([]trade.Candle, n)
	p := start
	for i := 0; i < n; i++ {
		p += 10
		out[i] = trade.Candle{
			Open:  p - 5,
			High:  p + 5,
			Low:   p - 10,
			Close: p,
		}
	}
	return out
}

func TestBaseSignals_uptrend(t *testing.T) {
	candles := synthCandles(250, 50_000)
	sigs, feats, err := BaseSignals(candles, DefaultBaseConfig())
	require.NoError(t, err)
	require.NotEmpty(t, feats)
	// Uptrend may or may not trigger MACD cross; at least features computed.
	assert.GreaterOrEqual(t, len(feats), 250)
	_ = sigs
}

func TestMetaFilter_empty(t *testing.T) {
	out := MetaFilter(nil, nil, DefaultMetaConfig())
	assert.Nil(t, out)
}
