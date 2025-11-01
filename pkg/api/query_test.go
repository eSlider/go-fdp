package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"sync-v3/pkg/binance"
	"testing"
	"time"
)

func TestCandles(t *testing.T) {
	// Create a test server
	server, err := NewServer()
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	// Create a test POST request without gzip support
	req := httptest.NewRequest("POST", "/v1/sql", bytes.NewReader([]byte(`{"query": "SELECT 1 as test"}`)))

	// Create a response recorder
	w := httptest.NewRecorder()

	// Call ServeHTTP
	server.ServeHTTP(w, req)

	exec, err := server.db.Exec("SELECT 1 as test")
	if err != nil {
		t.Errorf("Failed to execute query: %v", err)
	}

	count, err := exec.RowsAffected()
	if err != nil {
		t.Errorf("Failed to get affected rows: %v", err)
	}
	if count != 1 {
		t.Errorf("Expected 1, got %d", count)
	}

	var result []struct{ Test int }

	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Errorf("Response data is not valid JSON: %v, body: %s", err, string(w.Body.Bytes()))
	}

	if result[0].Test != 1 {
		t.Errorf("Expected 1, got %d", result[0].Test)
	}

	// Create asset configuration for a small dataset (2017-08 ETHUSDT 1m klines)
	// This date has existing data and we can verify the count
	asset := &binance.HistoryAsset{
		MarketType: binance.Spot,
		Frequency:  binance.Daily,
		Frame:      binance.OneMinute,
		Indicator:  binance.Klines,
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
