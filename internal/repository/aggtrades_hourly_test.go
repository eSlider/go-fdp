package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"sync-v3/internal/domain"
	"sync-v3/pkg/binance"
)

// TestAggTradesFromHourlyParquet tests reading aggTrades from hourly parquet files
// Uses real parquet files - no mocking
func TestAggTradesFromHourlyParquet(t *testing.T) {
	// First, ensure we have cached data by fetching from API
	ctx := context.Background()
	consumer, err := binance.NewHistoryConsumer(ctx)
	if err != nil {
		t.Fatalf("Failed to create history consumer: %v", err)
	}

	now := time.Now().UTC()
	asset := &binance.HistoryAsset{
		MarketType: binance.Spot,
		Frequency:  binance.Daily,
		Indicator:  binance.AggTrades,
		Market:     "BTCUSDT",
		Date:       now,
	}

	// Cache current day data
	trades, err := consumer.FetchAndCacheCurrentDayAggTrades(asset)
	if err != nil {
		t.Fatalf("Failed to cache aggTrades: %v", err)
	}
	t.Logf("Cached %d aggTrades to parquet files", len(trades))

	// Now test the repository reading from those parquet files
	repo, err := NewDuckDBRepository()
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	t.Run("Reads aggTrades from hourly parquet files", func(t *testing.T) {
		oneHourAgo := now.UTC().Add(-1 * time.Hour)

		req := domain.MarketDataRequest{
			From:       oneHourAgo,
			To:         now,
			Market:     "BTCUSDT",
			Exchange:   "binance",
			MarketType: domain.Spot,
			Indicator:  domain.AggTrades,
		}

		result, err := repo.aggTradesFromHourlyParquet(req)
		if err != nil {
			t.Fatalf("Failed to read aggTrades from hourly parquet: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected aggTrades from hourly parquet, got none")
		}

		t.Logf("Read %d aggTrades from hourly parquet files", len(result))

		// Verify first trade has expected fields
		trade := result[0]
		t.Logf("Trade fields: ID=%d, Price=%f, Qty=%f, FirstID=%d, LastID=%d",
			trade.ID, trade.Price, trade.Quantity, trade.FirstTradeID, trade.LastTradeID)
		if trade.ID == 0 {
			t.Error("Expected ID to be non-zero")
		}
		if trade.Price == 0 {
			t.Error("Expected Price to be non-zero")
		}
		if trade.Quantity == 0 {
			t.Error("Expected Quantity to be non-zero")
		}
		if trade.Time.IsZero() {
			t.Error("Expected Time to be non-zero")
		}
		fstr := fmt.Sprintf("First trade: ID=%d, Price=%.2f, Qty=%.6f, Time=%v, IsBuyerMaker=%v",
			trade.ID, trade.Price, trade.Quantity, trade.Time, trade.IsBuyerMaker)

		t.Log(fstr)
	})

	t.Run("GetAggTrades returns data for today", func(t *testing.T) {
		oneHourAgo := now.UTC().Add(-1 * time.Hour)

		req := domain.MarketDataRequest{
			From:       oneHourAgo,
			To:         now,
			Market:     "BTCUSDT",
			Exchange:   "binance",
			MarketType: domain.Spot,
			Indicator:  domain.AggTrades,
		}

		result, err := repo.GetAggTrades(ctx, req)
		if err != nil {
			t.Fatalf("GetAggTrades failed: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected aggTrades from GetAggTrades, got none")
		}

		t.Logf("GetAggTrades returned %d trades", len(result))
	})

	t.Run("Validates time filtering works", func(t *testing.T) {
		// Request for just the last 10 minutes
		tenMinutesAgo := now.UTC().Add(-10 * time.Minute)

		req := domain.MarketDataRequest{
			From:       tenMinutesAgo,
			To:         now,
			Market:     "BTCUSDT",
			Exchange:   "binance",
			MarketType: domain.Spot,
			Indicator:  domain.AggTrades,
		}

		result, err := repo.aggTradesFromHourlyParquet(req)
		if err != nil {
			t.Fatalf("Failed to read aggTrades: %v", err)
		}

		if len(result) == 0 {
			t.Fatal("Expected aggTrades, got none")
		}

		t.Logf("Last 10 minutes: %d trades", len(result))

		// Verify all trades are within the time range
		for _, trade := range result {
			if trade.Time.Before(tenMinutesAgo) || trade.Time.After(now.Add(1*time.Minute)) {
				t.Errorf("Trade time %v outside expected range [%v, %v]",
					trade.Time, tenMinutesAgo, now)
			}
		}
	})
}
