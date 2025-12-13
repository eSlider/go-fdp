package binance

import (
	"context"
	"testing"
	"time"
)

// TestFetchHourAggTradesData tests fetching aggTrades for a specific hour
// Uses real Binance API - no mocking
func TestFetchHourAggTradesData(t *testing.T) {
	ctx := context.Background()

	consumer, err := NewHistoryConsumer(ctx)
	if err != nil {
		t.Fatalf("Failed to create history consumer: %v", err)
	}

	t.Run("Fetch aggTrades for last hour", func(t *testing.T) {
		now := time.Now().UTC()
		oneHourAgo := now.Add(-1 * time.Hour)

		asset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Indicator:  AggTrades,
			Market:     "BTCUSDT",
			Date:       now,
		}

		trades, err := consumer.fetchHourAggTradesData(asset, oneHourAgo, now)
		if err != nil {
			t.Fatalf("Failed to fetch hour aggTrades: %v", err)
		}

		if len(trades) == 0 {
			t.Fatal("Expected at least 1 aggTrade for last hour")
		}

		t.Logf("Fetched %d aggTrades for last hour", len(trades))

		// Validate trade data
		for i, trade := range trades[:min(5, len(trades))] {
			if trade.AggTradeID == 0 {
				t.Errorf("Trade %d: AggTradeID should not be zero", i)
			}
			if trade.Price == 0 {
				t.Errorf("Trade %d: Price should not be zero", i)
			}
			if trade.Timestamp == 0 {
				t.Errorf("Trade %d: Timestamp should not be zero", i)
			}

			// Validate timestamp is within expected range
			tradeTime := time.UnixMilli(trade.Timestamp)
			if tradeTime.Before(oneHourAgo) || tradeTime.After(now) {
				t.Errorf("Trade timestamp %v outside expected range [%v, %v]",
					tradeTime, oneHourAgo, now)
			}
		}
	})

	t.Run("Fetch aggTrades from trade ID", func(t *testing.T) {
		now := time.Now().UTC()
		fiveMinutesAgo := now.Add(-5 * time.Minute)

		asset := &HistoryAsset{
			MarketType: Spot,
			Frequency:  Daily,
			Indicator:  AggTrades,
			Market:     "BTCUSDT",
			Date:       now,
		}

		// First fetch to get a trade ID
		initialTrades, err := consumer.fetchHourAggTradesData(asset, fiveMinutesAgo, now)
		if err != nil {
			t.Fatalf("Failed to fetch initial aggTrades: %v", err)
		}

		if len(initialTrades) < 2 {
			t.Skip("Not enough trades to test fromID functionality")
		}

		// Get the first trade ID and fetch from there
		fromID := initialTrades[0].AggTradeID + 1

		trades, err := consumer.fetchAggTradesFromID(asset, fromID)
		if err != nil {
			t.Fatalf("Failed to fetch aggTrades from ID: %v", err)
		}

		if len(trades) == 0 {
			t.Fatal("Expected aggTrades when fetching from ID")
		}

		t.Logf("Fetched %d aggTrades starting from ID %d", len(trades), fromID)

		// Verify all trades have ID >= fromID
		for _, trade := range trades {
			if trade.AggTradeID < fromID {
				t.Errorf("Trade ID %d is less than fromID %d", trade.AggTradeID, fromID)
			}
		}
	})
}

// TestAggTradeParquetConversion tests the conversion between AggTrade and ParquetAggTrade
func TestAggTradeParquetConversion(t *testing.T) {
	t.Run("Convert AggTrade to ParquetAggTrade", func(t *testing.T) {
		now := time.Now().UTC()
		trade := &AggTrade{
			AggTradeID:   12345,
			Price:        100000.50,
			Quantity:     0.5,
			FirstTradeID: 1000,
			LastTradeID:  1005,
			Timestamp:    now.UnixMilli(),
			IsBuyerMaker: true,
		}

		parquet, err := trade.Parquet()
		if err != nil {
			t.Fatalf("Failed to convert to parquet: %v", err)
		}

		if parquet.AggTradeID != trade.AggTradeID {
			t.Errorf("AggTradeID mismatch: %d != %d", parquet.AggTradeID, trade.AggTradeID)
		}
		if parquet.Price != trade.Price {
			t.Errorf("Price mismatch: %f != %f", parquet.Price, trade.Price)
		}
		if parquet.Quantity != trade.Quantity {
			t.Errorf("Quantity mismatch: %f != %f", parquet.Quantity, trade.Quantity)
		}
		if parquet.IsBuyerMaker != trade.IsBuyerMaker {
			t.Errorf("IsBuyerMaker mismatch: %v != %v", parquet.IsBuyerMaker, trade.IsBuyerMaker)
		}
	})
}

// TestSortAggTradesByID tests the sorting function
func TestSortAggTradesByID(t *testing.T) {
	trades := []*AggTrade{
		{AggTradeID: 5},
		{AggTradeID: 2},
		{AggTradeID: 8},
		{AggTradeID: 1},
		{AggTradeID: 3},
	}

	sortAggTradesByID(trades)

	for i := 1; i < len(trades); i++ {
		if trades[i].AggTradeID < trades[i-1].AggTradeID {
			t.Errorf("Trades not sorted: %d should be after %d",
				trades[i].AggTradeID, trades[i-1].AggTradeID)
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

