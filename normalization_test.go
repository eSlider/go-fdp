package main

import (
	"testing"
	"time"
)

// Test normalization by getting zip file
func TestNormalization(t *testing.T) {
	path := Link(&HistoryQuery{
		Market:    Spot,
		Frequency: Monthly,
		Date:      time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
		Interval:  OneMinute,
		Symbol:    "BTCUSDT",
	})

	t.Run("Link", func(t *testing.T) {
		if path != "data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2021-01.zip" {
			t.Errorf("unexpected path: %s", path)
		}
	})

	t.Run("Download", func(t *testing.T) {

	})
}
