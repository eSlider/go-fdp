package main

import (
	"testing"
	"time"

	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/stretchr/testify/assert"
)

func TestFrameHistory_cap(t *testing.T) {
	t.Parallel()
	h := make(frameHistory)
	for i := range maxHistoryPoints + 10 {
		h.appendPoint("5m", historyPoint{UpProbPct: float64(i)})
	}
	if len(h["5m"]) != maxHistoryPoints {
		t.Fatalf("len = %d want %d", len(h["5m"]), maxHistoryPoints)
	}
}

func TestCandleOHLC(t *testing.T) {
	t.Parallel()
	pts := []historyPoint{
		{At: time.Unix(0, 0), UpProbPct: 50},
		{At: time.Unix(60, 0), UpProbPct: 55},
	}
	o, h, l, c := candleOHLC(pts, metricUpProb)
	assert.Len(t, o, 2)
	assert.Len(t, h, 2)
	assert.Len(t, l, 2)
	assert.Len(t, c, 2)
	assert.Equal(t, 50.0, o[1].Value)
	assert.Equal(t, 55.0, c[1].Value)
	assert.Greater(t, h[1].Value, c[1].Value)
	assert.Less(t, l[1].Value, o[1].Value)
}

func TestMaxEmptyBarsInView_adjacent(t *testing.T) {
	t.Parallel()
	klines := []*binance.Kline{
		{OpenTime: 0},
		{OpenTime: 300_000},
	}
	assert.Equal(t, 0, maxEmptyBarsInView(klines, 0, 1e12, 300))
}

func TestMaxEmptyBarsInView_fiveEmpty(t *testing.T) {
	t.Parallel()
	// 6 bar periods apart on 5m frame => 5 empty slots between
	klines := []*binance.Kline{
		{OpenTime: 0},
		{OpenTime: 6 * 300_000},
	}
	assert.Equal(t, 5, maxEmptyBarsInView(klines, 0, 1e12, 300))
}
