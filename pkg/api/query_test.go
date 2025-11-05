package api

import (
	"bytes"
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
func TestCandles(t *testing.T) {

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
		q, err := (&AssetRequest{
			Exchange:   "binance",
			MarketType: string(binance.Spot),
			Frame:      binance.OneMinute,
			Indicator:  string(binance.Klines),
			Market:     "ETHUSDT",
			From:       day.UnixMicro(),
			To:         end.UnixMicro(),
		}).MarshalJSON()

		if err != nil {
			t.Errorf("Failed to marshal query: %v", err)
		}

		// params :=

		w := QueryServer(t, http.MethodGet, "/v1/data", q)
		r, err := HandleServerResponse[[]binance.ParquetKline](w)

		if err != nil {
			t.Errorf("Failed to decode response: %v", err)
		}

		if r == nil {
			t.Errorf("Response is nil")
		}

	}
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
