package api

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"testing"
	"time"
)

func QueryServer(t *testing.T, method, target string, body []byte) *httptest.ResponseRecorder {
	// Create a test server
	server, err := NewServer()
	if err != nil {
		t.Errorf("failed to create server: %v", err)
	}
	defer server.Close()

	// Create a test POST request without gzip support "/v1/sql"
	req := httptest.NewRequest(method, target, bytes.NewReader(body))

	// Create a response recorder
	w := httptest.NewRecorder()

	// Call ServeHTTP
	server.ServeHTTP(w, req)
	return w
}

func HandleServerResponse[T any](w *httptest.ResponseRecorder) (r *T, err error) {
	body := w.Body
	if w.Code != http.StatusOK {
		r, err := data.JsonDecode[Error](body)
		if err != nil {
			return nil, err
		}
		return nil, r
	}
	return data.JsonDecode[T](body)
}

// TestCandles - test candles endpoint
func TestMarkets(t *testing.T) {

	w := QueryServer(t, "GET", "/v1/markets", nil)
	r, err := data.JsonDecode[[]binance.Market](w.Body)

	if err != nil {
		t.Errorf("Failed to decode response: %v", err)
	}

	if r == nil {
		t.Errorf("Response is nil")
	}

	for _, m := range *r {
		if len(m.Symbols) < 2 {
			t.Logf("Symbol count is less than 2: %s", m.Name)
		}
	}

}

// TestCandles - test candles endpoint
func TestSQL(t *testing.T) {
	w := QueryServer(t, http.MethodPost, "/v1/sql", []byte(`{"query": "SELECT 1 as test"}`))
	result, err := data.JsonDecode[[]struct{ Test int }](w.Body)
	if err != nil {
		t.Errorf("Failed to decode response: %v", err)
	}
	if result == nil {
		t.Errorf("Response is nil")
	}

	if (*result)[0].Test != 1 {
		t.Errorf("Expected 1, got %d", (*result)[0].Test)
	}

}
func TestCandles(t *testing.T) {
	now := time.Now()
	end := now.AddDate(0, 0, -1)

	// Trace time between start and end every day
	// for day := now.AddDate(0, 0, -2); day.Before(end) || day.Equal(end); day = day.AddDate(0, 0, 1) {
	fmt.Println(end)
	q, err := (&AssetRequest{
		Exchange:   "binance",
		MarketType: string(binance.Spot),
		Frame:      binance.OneMinute,
		Indicator:  string(binance.Klines),
		Market:     "ZECUSDT",
		From:       end.UnixMicro(),
		To:         now.UnixMicro(),
	}).MarshalJSON()

	if err != nil {
		t.Errorf("Failed to marshal query: %v", err)
	}

	// params :=

	r, err := HandleServerResponse[[]*CandleResponse](QueryServer(t, http.MethodGet, "/v1/data", q))

	if err != nil {
		t.Errorf("Failed to decode response: %v", err)
	}

	if r == nil {
		t.Errorf("Response is nil")
	}

	// }
}
