package features

import (
	"math"
	"testing"

	trade "github.com/eslider/go-trade"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRSI_flat(t *testing.T) {
	closes := []float64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100}
	rsi := RSI(closes, 14)
	require.Len(t, rsi, len(closes))
	assert.False(t, IsNaN(rsi[14]))
	assert.InDelta(t, 50, rsi[14], 0.01)
}

func TestMACDCrossUp(t *testing.T) {
	hist := []float64{-1, -0.5, 0.1}
	assert.False(t, MACDCrossUp(hist, 0))
	assert.True(t, MACDCrossUp(hist, 2))
}

func TestRealizedVol(t *testing.T) {
	closes := []float64{100, 101, 99, 102, 98}
	v := RealizedVol(closes, 4)
	assert.True(t, v > 0)
}

func TestOHLCFromCandles(t *testing.T) {
	candles := []trade.Candle{
		{Open: 1, High: 2, Low: 0.5, Close: 1.5},
		{Open: 1.5, High: 2.5, Low: 1, Close: 2},
	}
	ohlc := OHLCFromCandles(candles)
	assert.Equal(t, 2, ohlc.Len())
	assert.Equal(t, 2.0, ohlc.Close[1])
}

func TestSMA_period(t *testing.T) {
	v := []float64{1, 2, 3, 4, 5}
	sma := SMA(v, 3)
	assert.True(t, IsNaN(sma[1]))
	assert.InDelta(t, 2, sma[2], 1e-9)
	assert.InDelta(t, 4, sma[4], 1e-9)
}

func TestImpliedStrike(t *testing.T) {
	s := ImpliedStrike(100_000, 0.55, 0.02)
	assert.True(t, s > 100_000)
	assert.False(t, math.IsNaN(s))
}
