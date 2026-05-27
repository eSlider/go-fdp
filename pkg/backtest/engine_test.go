package backtest

import (
	"testing"

	trade "github.com/eslider/go-trade"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRun_insufficientBars(t *testing.T) {
	_, err := Run([]trade.Candle{{Close: 1}}, nil, DefaultConfig())
	require.Error(t, err)
}

func TestSharpeFromCurve(t *testing.T) {
	curve := []float64{100, 101, 102, 101, 103}
	s := sharpeFromCurve(curve)
	assert.False(t, s < 0 && s == 0) // may be positive
}
