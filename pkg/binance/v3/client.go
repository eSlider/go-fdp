package v3

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

const defaultBaseURL = "https://api.binance.com"

// Client calls the Binance Spot REST API v3.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient returns a Client with the default HTTP client and base URL.
func NewClient() *Client {
	return &Client{
		baseURL:    defaultBaseURL,
		httpClient: http.DefaultClient,
	}
}

// NewClientWithHTTP returns a Client that uses the given HTTP client.
func NewClientWithHTTP(httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:    defaultBaseURL,
		httpClient: httpClient,
	}
}

// AggTrades fetches compressed aggregate trades.
func (c *Client) AggTrades(req *AggTradeRequest) ([]AggTrade, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v3/aggTrades?%s", c.baseURL, req.urlParams())
	body, err := c.get(url)
	if err != nil {
		return nil, err
	}

	var responses []aggTradeResponse
	if err := json.Unmarshal(body, &responses); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	trades := make([]AggTrade, len(responses))
	for i, r := range responses {
		trades[i] = aggTradeFromResponse(r)
	}
	return trades, nil
}

// Candles fetches kline/candlestick data.
func (c *Client) Candles(req *CandleRequest) ([]Kline, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v3/klines?%s", c.baseURL, req.urlParams())
	body, err := c.get(url)
	if err != nil {
		return nil, err
	}

	var klines []Kline
	if err := json.Unmarshal(body, &klines); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}
	return klines, nil
}

func (c *Client) get(url string) ([]byte, error) {
	resp, err := c.httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}
