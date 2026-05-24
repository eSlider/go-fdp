package main

import (
	"testing"

	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/stretchr/testify/assert"
)

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
