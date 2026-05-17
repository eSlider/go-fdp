package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/eslider/go-binance-fdp/internal/repository"
	"github.com/eslider/go-binance-fdp/internal/service"
	"github.com/eslider/go-binance-fdp/pkg/binance"
)

// TestAggTradesHandler tests the /v1/aggtrades endpoint
func TestAggTradesHandler(t *testing.T) {
	// Create real repository
	repo, err := repository.NewDuckDBRepository()
	if err != nil {
		t.Fatalf("Failed to create repository: %v", err)
	}
	defer repo.Close()

	// Create real history consumer
	ctx := context.Background()
	consumer, err := binance.NewHistoryConsumer(ctx)
	if err != nil {
		t.Fatalf("Failed to create history consumer: %v", err)
	}

	// Create real service
	svc := service.NewMarketService(repo, consumer)

	// Create handler with real service
	handler := NewMarketHandler(svc)

	t.Run("Returns correct JSON structure for Grafana", func(t *testing.T) {
		now := time.Now().UTC()
		oneHourAgo := now.Add(-1 * time.Hour)

		req, err := http.NewRequest("GET",
			"/v1/aggtrades?market=BTCUSDT&from="+
				formatMs(oneHourAgo)+"&to="+formatMs(now),
			nil)
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		http.HandlerFunc(handler.GetAggTrades).ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Errorf("Expected status 200, got %d. Body: %s", rr.Code, rr.Body.String())
		}

		// Parse JSON response
		var response []map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
			t.Fatalf("Failed to parse JSON response: %v. Body: %s", err, rr.Body.String())
		}

		if len(response) == 0 {
			t.Fatal("Expected at least 1 trade in response")
		}

		// Verify Grafana-expected fields exist
		trade := response[0]

		requiredFields := []string{"id", "price", "quantity", "time", "isBuyerMaker"}
		for _, field := range requiredFields {
			if _, ok := trade[field]; !ok {
				t.Errorf("Missing required field for Grafana: %s", field)
			}
		}

		// Verify field types
		if _, ok := trade["id"].(float64); !ok {
			t.Error("id should be a number")
		}
		if _, ok := trade["price"].(float64); !ok {
			t.Error("price should be a number")
		}
		if _, ok := trade["quantity"].(float64); !ok {
			t.Error("quantity should be a number")
		}
		if _, ok := trade["time"].(string); !ok {
			t.Error("time should be a string (ISO timestamp)")
		}
		if _, ok := trade["isBuyerMaker"].(bool); !ok {
			t.Error("isBuyerMaker should be a boolean")
		}

		t.Logf("Response structure is Grafana-compatible: %+v", trade)
	})

	t.Run("Validates required parameters", func(t *testing.T) {
		// Missing 'from' parameter
		req, _ := http.NewRequest("GET", "/v1/aggtrades?market=BTCUSDT&to=1702468800000", nil)
		rr := httptest.NewRecorder()
		http.HandlerFunc(handler.GetAggTrades).ServeHTTP(rr, req)

		if rr.Code != http.StatusBadRequest {
			t.Errorf("Expected status 400 for missing 'from', got %d", rr.Code)
		}
	})
}

func formatMs(t time.Time) string {
	return fmt.Sprintf("%d", t.UnixMilli())
}
