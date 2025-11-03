package binance

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
)

func getCurrentCandle(symbol, interval string) ([]interface{}, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/klines?symbol=%s&interval=%s&limit=1", symbol, interval)

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var data [][]interface{}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("no kline data returned")
	}

	latest := data[0]

	return latest, nil
}

// Test normalization by getting zip file
func TestCurrentCandles(t *testing.T) {
	t.Run("Parsing and creation history asset links", func(t *testing.T) {
		for _, market := range []string{"BTCUSDT", "ETHUSDT"} {
			for _, frame := range []string{"1m", "1h"} {
				for _, indicator := range []Indicator{Klines} {

					fmt.Println(market, frame, indicator)

					kandles, err := getCurrentCandle(market, frame)
					if err != nil {
						t.Fatal(err)
					}
					if len(kandles) == 0 {
						t.Fatal("no kline data returned")
					}

				}
			}
		}
	})
}
