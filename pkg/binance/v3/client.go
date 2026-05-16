package v3

import (
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

type Option func(*Client)

func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) {
		if httpClient == nil {
			httpClient = http.DefaultClient
		}
		c.httpClient = httpClient
	}
}

// NewClient returns a Client with the default HTTP client and base URL.
func NewClient(option ...Option) *Client {
	c := &Client{
		baseURL:    defaultBaseURL,
		httpClient: http.DefaultClient,
	}
	for _, opt := range option {
		opt(c)
	}
	return c
}

// AggTrades fetches compressed aggregate trades.
func (c *Client) AggTrades(req *AggTradeRequest) ([]AggTrade, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	params, err := req.urlParams()
	if err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v3/aggTrades?%s", c.baseURL, params)
	body, err := c.get(url)
	if err != nil {
		return nil, err
	}

	return NewAggTrades(WithJson[AggTrade](body))
}

// Candles fetches kline/candlestick data.
func (c *Client) Candles(req *CandleRequest) ([]Kline, error) {
	if err := req.Validate(); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	params, err := req.urlParams()
	if err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	url := fmt.Sprintf("%s/api/v3/klines?%s", c.baseURL, params)
	body, err := c.get(url)
	if err != nil {
		return nil, err
	}

	return NewKlines(WithJson[Kline](body))
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
