package fdp

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	trade "github.com/eslider/go-trade"
	"github.com/goccy/go-json"
)

// Client reads market data from a go-fdp HTTP server.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewClient returns a client with default HTTP transport.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		HTTPClient: http.DefaultClient,
	}
}

type candleDTO struct {
	OpenTime  time.Time `json:"openTime"`
	CloseTime time.Time `json:"closeTime"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
}

// FetchCandles returns go-trade candles for [from, to].
func (c *Client) FetchCandles(ctx context.Context, market, frame string, from, to time.Time) ([]trade.Candle, error) {
	u, err := url.Parse(c.BaseURL + "/v1/data")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("market", market)
	q.Set("exchange", "binance")
	q.Set("marketType", "spot")
	q.Set("frame", frame)
	q.Set("from", fmt.Sprintf("%d", from.UTC().UnixMilli()))
	q.Set("to", fmt.Sprintf("%d", to.UTC().UnixMilli()))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fdp candles %s: %s", resp.Status, truncate(string(body), 200))
	}

	var dtos []candleDTO
	if err := json.Unmarshal(body, &dtos); err != nil {
		return nil, fmt.Errorf("decode candles: %w", err)
	}
	return candlesFromDTO(dtos), nil
}

type predictionDTO struct {
	Time        time.Time `json:"time"`
	UpPrice     float64   `json:"upPrice"`
	DownPrice   float64   `json:"downPrice"`
	Frame       string    `json:"frame"`
	Market      string    `json:"market"`
	WindowStart time.Time `json:"windowStart"`
	WindowEnd   time.Time `json:"windowEnd"`
}

// Prediction is a Polymarket implied-probability snapshot.
type Prediction struct {
	Time        time.Time
	UpPrice     float64
	DownPrice   float64
	Frame       string
	Market      string
	WindowStart time.Time
	WindowEnd   time.Time
}

// FetchPredictions returns Polymarket history for the range.
func (c *Client) FetchPredictions(ctx context.Context, market, frame string, from, to time.Time) ([]Prediction, error) {
	u, err := url.Parse(c.BaseURL + "/v1/predictions")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("market", market)
	q.Set("exchange", "polymarket")
	q.Set("frame", frame)
	q.Set("from", fmt.Sprintf("%d", from.UTC().UnixMilli()))
	q.Set("to", fmt.Sprintf("%d", to.UTC().UnixMilli()))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fdp predictions %s: %s", resp.Status, truncate(string(body), 200))
	}

	var dtos []predictionDTO
	if err := json.Unmarshal(body, &dtos); err != nil {
		return nil, fmt.Errorf("decode predictions: %w", err)
	}
	out := make([]Prediction, len(dtos))
	for i, d := range dtos {
		out[i] = Prediction(d)
	}
	return out, nil
}

func candlesFromDTO(dtos []candleDTO) []trade.Candle {
	out := make([]trade.Candle, 0, len(dtos))
	for _, d := range dtos {
		out = append(out, trade.Candle{
			TimeOpen:  d.OpenTime.UTC(),
			TimeClose: d.CloseTime.UTC(),
			Open:      d.Open,
			High:      d.High,
			Low:       d.Low,
			Close:     d.Close,
			Volume:    d.Volume,
		})
	}
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
