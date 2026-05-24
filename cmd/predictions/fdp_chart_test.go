package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFdpCandlesToKlines_sortsAscending(t *testing.T) {
	t.Parallel()
	candles := []fdpCandle{
		{OpenTime: time.Unix(200, 0), CloseTime: time.Unix(260, 0), Open: 2, High: 2, Low: 2, Close: 2},
		{OpenTime: time.Unix(100, 0), CloseTime: time.Unix(160, 0), Open: 1, High: 1, Low: 1, Close: 1},
	}
	klines := fdpCandlesToKlines(candles)
	require.Len(t, klines, 2)
	assert.Less(t, klines[0].OpenTime, klines[1].OpenTime)
	assert.Equal(t, int64(100000), klines[0].OpenTime)
}
