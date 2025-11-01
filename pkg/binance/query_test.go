package binance

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestCandles(t *testing.T) {
	srv, err := NewHistoryConsumer(context.Background())
	if err != nil {
		t.Fatalf("could not initialize service: %s", err.Error())
	}
	if srv == nil {
		t.Fatal("service is nil")
	}

	registry, err := NewExchangeRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if len(registry.Symbols) < 1 {
		t.Fatal("No symbols found")
	}
	if len(registry.Markets) < 1 {
		t.Fatal("No markets found")
	}

	// Create asset configuration for a small dataset (2017-08 ETHUSDT 1m klines)
	// This date has existing data and we can verify the count
	asset := &HistoryAsset{
		MarketType: Spot,
		Frequency:  Daily,
		Frame:      OneMinute,
		Indicator:  Klines,
		Date:       time.Date(2020, 8, 2, 0, 0, 0, 0, time.UTC),
		Market:     "ETHUSDT",
	}

	start := asset.Date
	end := asset.Date.Add(time.Hour * 24 * 2)

	// Trace time between start and end every day
	for day := start; day.Before(end); day = day.AddDate(0, 0, 1) {

		fmt.Println(day)
	}
}
