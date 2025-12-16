package service

import (
	"context"
	"testing"
	"time"

	"sync-v3/internal/domain"
	"sync-v3/pkg/binance"
)

// MockRepository for testing
type MockRepository struct{}

func (m *MockRepository) GetCandles(ctx context.Context, req domain.MarketDataRequest) ([]*domain.Candle, error) {
	return nil, nil
}

func (m *MockRepository) GetAggTrades(ctx context.Context, req domain.MarketDataRequest) ([]*domain.AggTrade, error) {
	return nil, nil
}

// MockHistoryConsumer for testing
type MockHistoryConsumer struct{}

func (m *MockHistoryConsumer) DownloadAndTransform(asset *binance.HistoryAsset) (chan *binance.AssetETLInfo, chan error) {
	infoCh := make(chan *binance.AssetETLInfo)
	errCh := make(chan error)
	go func() {
		close(infoCh)
		close(errCh)
	}()
	return infoCh, errCh
}

// TestGetAggTradesFromAPI tests fetching aggTrades directly from API for today's queries
func TestGetAggTradesFromAPI(t *testing.T) {
	service := NewMarketService(&MockRepository{}, &MockHistoryConsumer{})

	t.Run("Fetches today's aggTrades from API", func(t *testing.T) {
		now := time.Now().UTC()
		oneHourAgo := now.Add(-1 * time.Hour)

		req := domain.MarketDataRequest{
			From:       oneHourAgo,
			To:         now,
			Market:     "BTCUSDT",
			Exchange:   "binance",
			MarketType: domain.Spot,
			Indicator:  domain.AggTrades,
		}

		trades, err := service.GetAggTrades(context.Background(), req)
		if err != nil {
			t.Fatalf("Failed to get aggTrades: %v", err)
		}

		if len(trades) == 0 {
			t.Fatal("Expected aggTrades for today, got none")
		}

		t.Logf("Fetched %d aggTrades from API", len(trades))

		// Verify first trade has expected fields
		trade := trades[0]
		if trade.ID == 0 {
			t.Error("Expected ID to be non-zero")
		}
		if trade.Price == 0 {
			t.Error("Expected Price to be non-zero")
		}
		if trade.Time.IsZero() {
			t.Error("Expected Time to be non-zero")
		}

		t.Logf("First trade: ID=%d, Price=%.2f, Qty=%.6f, Time=%v",
			trade.ID, trade.Price, trade.Quantity, trade.Time)
	})

	t.Run("Matches Grafana request format (last hour)", func(t *testing.T) {
		// Simulate the Grafana request with dynamic timestamps (last hour)
		now := time.Now().UTC()
		oneHourAgo := now.Add(-1 * time.Hour)

		req := domain.MarketDataRequest{
			From:       oneHourAgo,
			To:         now,
			Market:     "BTCUSDT",
			Exchange:   "binance",
			MarketType: domain.Spot,
			Indicator:  domain.AggTrades,
		}

		trades, err := service.GetAggTrades(context.Background(), req)
		if err != nil {
			t.Fatalf("Failed to get aggTrades for Grafana request: %v", err)
		}

		if len(trades) == 0 {
			t.Fatal("Expected aggTrades for Grafana request, got none")
		}

		t.Logf("Grafana request returned %d aggTrades", len(trades))
	})
}
