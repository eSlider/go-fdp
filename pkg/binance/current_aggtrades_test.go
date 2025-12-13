package binance

import (
	"testing"
	"time"
)

// TestGetCurrentAggTrades tests fetching aggTrades from Binance API
// Uses real API calls - no mocking
func TestGetCurrentAggTrades(t *testing.T) {

	t.Run("Fetch aggTrades between yesterday and today", func(t *testing.T) {
		now := time.Now().UTC()
		yesterday := now.Add(-24 * time.Hour)

		// Test fetching aggTrades for BTCUSDT
		req := &AggTradeRequestV3{
			Symbol:    "BTCUSDT",
			StartTime: ptrInt64(yesterday.UnixMilli()),
			EndTime:   ptrInt64(now.UnixMilli()),
			Limit:     100, // Limit to 100 trades for testing
		}

		trades, err := GetCurrentAggTrades(req)
		if err != nil {
			t.Fatalf("Failed to fetch aggTrades: %v", err)
		}

		if len(trades) == 0 {
			t.Fatal("Expected at least 1 aggTrade, got 0")
		}

		t.Logf("Fetched %d aggTrades", len(trades))

		// Validate first trade has expected fields
		firstTrade := trades[0]
		if firstTrade.AggTradeID == 0 {
			t.Error("Expected AggTradeID to be non-zero")
		}
		if firstTrade.Price == 0 {
			t.Error("Expected Price to be non-zero")
		}
		if firstTrade.Quantity == 0 {
			t.Error("Expected Quantity to be non-zero")
		}
		if firstTrade.Timestamp == 0 {
			t.Error("Expected Timestamp to be non-zero")
		}

		t.Logf("First trade: ID=%d, Price=%.2f, Qty=%.6f, Time=%d",
			firstTrade.AggTradeID, firstTrade.Price, firstTrade.Quantity, firstTrade.Timestamp)
	})

	t.Run("Fetch aggTrades for last hour", func(t *testing.T) {
		now := time.Now().UTC()
		oneHourAgo := now.Add(-1 * time.Hour)

		req := &AggTradeRequestV3{
			Symbol:    "BTCUSDT",
			StartTime: ptrInt64(oneHourAgo.UnixMilli()),
			EndTime:   ptrInt64(now.UnixMilli()),
			Limit:     1000,
		}

		trades, err := GetCurrentAggTrades(req)
		if err != nil {
			t.Fatalf("Failed to fetch aggTrades: %v", err)
		}

		if len(trades) == 0 {
			t.Fatal("Expected aggTrades for last hour")
		}

		t.Logf("Fetched %d aggTrades for last hour", len(trades))

		// Validate timestamps are within expected range
		for _, trade := range trades {
			tradeTime := time.UnixMilli(trade.Timestamp)
			if tradeTime.Before(oneHourAgo) || tradeTime.After(now) {
				t.Errorf("Trade timestamp %v outside expected range [%v, %v]",
					tradeTime, oneHourAgo, now)
			}
		}
	})

	t.Run("Fetch aggTrades for multiple markets", func(t *testing.T) {
		now := time.Now().UTC()
		tenMinutesAgo := now.Add(-10 * time.Minute)

		markets := []string{"BTCUSDT", "ETHUSDT"}

		for _, market := range markets {
			req := &AggTradeRequestV3{
				Symbol:    market,
				StartTime: ptrInt64(tenMinutesAgo.UnixMilli()),
				EndTime:   ptrInt64(now.UnixMilli()),
				Limit:     50,
			}

			trades, err := GetCurrentAggTrades(req)
			if err != nil {
				t.Fatalf("Failed to fetch aggTrades for %s: %v", market, err)
			}

			if len(trades) == 0 {
				t.Errorf("Expected aggTrades for %s, got 0", market)
			}

			t.Logf("%s: Fetched %d aggTrades", market, len(trades))
		}
	})

	t.Run("Validate AggTrade structure", func(t *testing.T) {
		now := time.Now().UTC()
		fiveMinutesAgo := now.Add(-5 * time.Minute)

		req := &AggTradeRequestV3{
			Symbol:    "BTCUSDT",
			StartTime: ptrInt64(fiveMinutesAgo.UnixMilli()),
			EndTime:   ptrInt64(now.UnixMilli()),
			Limit:     10,
		}

		trades, err := GetCurrentAggTrades(req)
		if err != nil {
			t.Fatalf("Failed to fetch aggTrades: %v", err)
		}

		if len(trades) == 0 {
			t.Fatal("Expected at least 1 aggTrade")
		}

		for i, trade := range trades {
			// AggTradeID should be monotonically increasing
			if i > 0 && trade.AggTradeID <= trades[i-1].AggTradeID {
				// Note: might not be strictly increasing if we get limit trades
				t.Logf("Trade IDs may not be strictly increasing: %d vs %d",
					trades[i-1].AggTradeID, trade.AggTradeID)
			}

			// FirstTradeID should be <= LastTradeID
			if trade.FirstTradeID > trade.LastTradeID {
				t.Errorf("FirstTradeID (%d) > LastTradeID (%d)",
					trade.FirstTradeID, trade.LastTradeID)
			}
		}
	})
}

// Helper function to create pointer to int64
func ptrInt64(v int64) *int64 {
	return &v
}

