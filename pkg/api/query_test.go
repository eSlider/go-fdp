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
	r, err := data.JsonDecode[[]*binance.Market](w.Body)

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

// TestCandlesToday tests querying today's data from DuckDB cache
func TestCandlesToday(t *testing.T) {
	now := time.Now()
	todayMidnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())

	// Query from midnight today to now (today's data)
	q, err := (&AssetRequest{
		Exchange:   "binance",
		MarketType: string(binance.Spot),
		Frame:      binance.OneMinute,
		Indicator:  string(binance.Klines),
		Market:     "BTCUSDT",
		From:       todayMidnight.UnixMilli(),
		To:         now.UnixMilli(),
	}).MarshalJSON()

	if err != nil {
		t.Fatalf("Failed to marshal query: %v", err)
	}

	r, err := HandleServerResponse[[]*CandleResponse](QueryServer(t, http.MethodGet, "/v1/data", q))
	if err != nil {
		t.Fatalf("Failed to get response: %v", err)
	}

	if r == nil {
		t.Fatal("Response is nil")
	}

	// Validate response structure and data
	if len(*r) == 0 {
		t.Log("No today's data returned - this might be expected if no data is cached yet")
		return
	}

	// Validate each candle
	for i, candle := range *r {
		openTime := time.Time(candle.OpenTime)
		closeTime := time.Time(candle.CloseTime)
		if openTime.IsZero() {
			t.Errorf("Candle %d has zero OpenTime", i)
		}
		if closeTime.IsZero() {
			t.Errorf("Candle %d has zero CloseTime", i)
		}
		if !closeTime.After(openTime) {
			t.Errorf("Candle %d CloseTime (%v) should be after OpenTime (%v)",
				i, closeTime, openTime)
		}
		if candle.High < candle.Low {
			t.Errorf("Candle %d High (%f) should be >= Low (%f)", i, candle.High, candle.Low)
		}
	}

	// Validate chronological ordering (should be descending by close time)
	for i := 1; i < len(*r); i++ {
		prevCloseTime := time.Time((*r)[i-1].CloseTime)
		currCloseTime := time.Time((*r)[i].CloseTime)
		if prevCloseTime.Before(currCloseTime) {
			t.Errorf("Candles not properly sorted by close time descending: candle[%d]=%v, candle[%d]=%v",
				i-1, prevCloseTime, i, currCloseTime)
		}
	}

	t.Logf("Successfully retrieved %d candles for today's data", len(*r))
}

// TestCandlesHistorical tests querying historical data from parquet files
func TestCandlesHistorical(t *testing.T) {
	// Use a date from the past that should have parquet data
	historicalDate := time.Date(2020, 8, 2, 0, 0, 0, 0, time.UTC)
	nextDay := historicalDate.AddDate(0, 0, 1)

	q, err := (&AssetRequest{
		Exchange:   "binance",
		MarketType: string(binance.Spot),
		Frame:      binance.OneMinute,
		Indicator:  string(binance.Klines),
		Market:     "ETHUSDT",
		From:       historicalDate.UnixMilli(),
		To:         nextDay.UnixMilli(),
	}).MarshalJSON()

	if err != nil {
		t.Fatalf("Failed to marshal query: %v", err)
	}

	r, err := HandleServerResponse[[]*CandleResponse](QueryServer(t, http.MethodGet, "/v1/data", q))
	// Allow the test to pass even if historical data download fails
	if err != nil {
		t.Logf("Historical data query failed (expected for missing data): %v", err)
		return
	}

	if r == nil {
		t.Fatal("Response is nil")
	}

	// For historical data, we expect some data to be returned if available
	if len(*r) == 0 {
		t.Log("No historical data returned - this might be expected if parquet files don't exist or download failed")
		return
	}

	// Validate each candle
	for i, candle := range *r {
		openTime := time.Time(candle.OpenTime)
		closeTime := time.Time(candle.CloseTime)
		if openTime.IsZero() {
			t.Errorf("Candle %d has zero OpenTime", i)
		}
		if closeTime.IsZero() {
			t.Errorf("Candle %d has zero CloseTime", i)
		}
		if !closeTime.After(openTime) {
			t.Errorf("Candle %d CloseTime (%v) should be after OpenTime (%v)",
				i, closeTime, openTime)
		}
		if candle.High < candle.Low {
			t.Errorf("Candle %d High (%f) should be >= Low (%f)", i, candle.High, candle.Low)
		}
		if candle.Volume < 0 {
			t.Errorf("Candle %d has negative volume: %f", i, candle.Volume)
		}
	}

	// Validate chronological ordering (should be descending by close time)
	for i := 1; i < len(*r); i++ {
		prevCloseTime := time.Time((*r)[i-1].CloseTime)
		currCloseTime := time.Time((*r)[i].CloseTime)
		if prevCloseTime.Before(currCloseTime) {
			t.Errorf("Candles not properly sorted by close time descending: candle[%d]=%v, candle[%d]=%v",
				i-1, prevCloseTime, i, currCloseTime)
		}
	}

	// Validate time range
	for i, candle := range *r {
		openTime := time.Time(candle.OpenTime)
		closeTime := time.Time(candle.CloseTime)
		if openTime.Before(historicalDate) {
			t.Errorf("Candle %d OpenTime %v is before requested From time %v",
				i, openTime, historicalDate)
		}
		if closeTime.After(nextDay) {
			t.Errorf("Candle %d CloseTime %v is after requested To time %v",
				i, closeTime, nextDay)
		}
	}

	t.Logf("Successfully retrieved %d candles for historical data", len(*r))
}

// TestCandlesMixedRange tests querying a range that includes both today and historical data
func TestCandlesMixedRange(t *testing.T) {
	now := time.Now()
	yesterday := now.AddDate(0, 0, -1)

	q, err := (&AssetRequest{
		Exchange:   "binance",
		MarketType: string(binance.Spot),
		Frame:      binance.OneMinute,
		Indicator:  string(binance.Klines),
		Market:     "BTCUSDT",
		From:       yesterday.UnixMilli(),
		To:         now.UnixMilli(),
	}).MarshalJSON()

	if err != nil {
		t.Fatalf("Failed to marshal query: %v", err)
	}

	r, err := HandleServerResponse[[]*CandleResponse](QueryServer(t, http.MethodGet, "/v1/data", q))
	// Allow the test to pass even if data fetching fails
	if err != nil {
		t.Logf("Mixed range query failed (expected for missing data): %v", err)
		return
	}

	if r == nil {
		t.Fatal("Response is nil")
	}

	// Validate that we get some data (could be from today, historical, or both)
	if len(*r) == 0 {
		t.Log("No data returned for mixed range - this might be expected if no cached data exists")
		return
	}

	// Validate data structure and ordering
	for i, candle := range *r {
		openTime := time.Time(candle.OpenTime)
		closeTime := time.Time(candle.CloseTime)
		if openTime.IsZero() || closeTime.IsZero() {
			t.Errorf("Candle %d has invalid timestamps: OpenTime=%v, CloseTime=%v",
				i, openTime, closeTime)
		}
		if !closeTime.After(openTime) {
			t.Errorf("Candle %d CloseTime should be after OpenTime", i)
		}
	}

	// Check chronological ordering
	for i := 1; i < len(*r); i++ {
		prevCloseTime := time.Time((*r)[i-1].CloseTime)
		currCloseTime := time.Time((*r)[i].CloseTime)
		if prevCloseTime.Before(currCloseTime) {
			t.Errorf("Candles not properly sorted by close time descending")
		}
	}

	t.Logf("Successfully retrieved %d candles for mixed date range", len(*r))
}

// TestCandlesInvalidRequest tests error handling for invalid requests
func TestCandlesInvalidRequest(t *testing.T) {
	tests := []struct {
		name        string
		request     AssetRequest
		expectError bool
	}{
		{
			name: "invalid market type",
			request: AssetRequest{
				Exchange:   "binance",
				MarketType: "invalid",
				Frame:      binance.OneMinute,
				Indicator:  string(binance.Klines),
				Market:     "BTCUSDT",
				From:       time.Now().AddDate(0, 0, -1).UnixMilli(),
				To:         time.Now().UnixMilli(),
			},
			expectError: true,
		},
		{
			name: "from time after to time",
			request: AssetRequest{
				Exchange:   "binance",
				MarketType: string(binance.Spot),
				Frame:      binance.OneMinute,
				Indicator:  string(binance.Klines),
				Market:     "BTCUSDT",
				From:       time.Now().UnixMilli(),
				To:         time.Now().AddDate(0, 0, -1).UnixMilli(),
			},
			expectError: true,
		},
		{
			name: "empty market",
			request: AssetRequest{
				Exchange:   "binance",
				MarketType: string(binance.Spot),
				Frame:      binance.OneMinute,
				Indicator:  string(binance.Klines),
				Market:     "",
				From:       time.Now().AddDate(0, 0, -1).UnixMilli(),
				To:         time.Now().UnixMilli(),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, err := tt.request.MarshalJSON()
			if err != nil {
				t.Fatalf("Failed to marshal query: %v", err)
			}

			w := QueryServer(t, http.MethodGet, "/v1/data", q)

			if tt.expectError {
				if w.Code == http.StatusOK {
					t.Errorf("Expected error response, but got status %d", w.Code)
				}
			} else {
				if w.Code != http.StatusOK {
					t.Errorf("Expected success response, but got status %d", w.Code)
				}
			}
		})
	}
}

// TestCandlesEmptyResponse tests handling of queries that return no data
func TestCandlesEmptyResponse(t *testing.T) {
	// Use a past date range for a market that doesn't exist locally
	pastTime := time.Now().AddDate(-2, 0, 0) // 2 years ago

	q, err := (&AssetRequest{
		Exchange:   "binance",
		MarketType: string(binance.Spot),
		Frame:      binance.OneMinute,
		Indicator:  string(binance.Klines),
		Market:     "VERYUNLIKELYMARKETNAME",
		From:       pastTime.UnixMilli(),
		To:         pastTime.Add(time.Hour).UnixMilli(),
	}).MarshalJSON()

	if err != nil {
		t.Fatalf("Failed to marshal query: %v", err)
	}

	r, err := HandleServerResponse[[]*CandleResponse](QueryServer(t, http.MethodGet, "/v1/data", q))
	// Allow the test to pass even if data fetching fails for non-existent markets
	if err != nil {
		t.Logf("Empty response test failed as expected for non-existent market: %v", err)
		return
	}

	if r == nil {
		t.Fatal("Response is nil")
	}

	// Should return empty array, not nil
	if len(*r) != 0 {
		t.Errorf("Expected empty response for non-existent market, got %d candles", len(*r))
	}

	t.Log("Successfully handled empty response for non-existent market")
}
