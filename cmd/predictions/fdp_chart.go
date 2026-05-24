package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/goccy/go-json"
)

const defaultFDPURL = "http://127.0.0.1:8082"

type fdpCandle struct {
	OpenTime  time.Time `json:"openTime"`
	CloseTime time.Time `json:"closeTime"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
}

func fetchChartKlines(ctx context.Context, cfg appConfig, symbol string, frame data.Frame) ([]*binance.Kline, error) {
	if cfg.fdpURL != "" {
		klines, err := fetchKlinesFromFDP(ctx, cfg.fdpURL, cfg.timeout, symbol, frame)
		if err == nil && len(klines) > 0 {
			return klines, nil
		}
	}
	return fetchKlinesFromBinance(ctx, cfg.timeout, symbol, frame)
}

func fetchKlinesFromBinance(ctx context.Context, timeout time.Duration, symbol string, frame data.Frame) ([]*binance.Kline, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return binance.FetchKlines(ctx, &binance.KlineRequest{
		Base:     binance.SymbolRequest{Symbol: symbol},
		Interval: frame.String(),
		Limit:    500,
	})
}

func fetchKlinesFromFDP(ctx context.Context, baseURL string, timeout time.Duration, symbol string, frame data.Frame) ([]*binance.Kline, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	to := time.Now().UTC()
	from := to.Add(-7 * 24 * time.Hour)
	u, err := url.Parse(strings.TrimRight(baseURL, "/") + "/v1/data")
	if err != nil {
		return nil, err
	}
	q := u.Query()
	q.Set("market", symbol)
	q.Set("exchange", "binance")
	q.Set("marketType", "spot")
	q.Set("frame", frame.String())
	q.Set("from", fmt.Sprintf("%d", from.UnixMilli()))
	q.Set("to", fmt.Sprintf("%d", to.UnixMilli()))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fdp %s: %s", resp.Status, truncate(string(body), 120))
	}

	var candles []fdpCandle
	if err := json.Unmarshal(body, &candles); err != nil {
		return nil, fmt.Errorf("decode fdp candles: %w", err)
	}
	return fdpCandlesToKlines(candles), nil
}

func fdpCandlesToKlines(candles []fdpCandle) []*binance.Kline {
	out := make([]*binance.Kline, 0, len(candles))
	for _, c := range candles {
		out = append(out, &binance.Kline{
			OpenTime:   c.OpenTime.UTC().UnixMilli(),
			CloseTime:  c.CloseTime.UTC().UnixMilli(),
			OpenPrice:  c.Open,
			HighPrice:  c.High,
			LowPrice:   c.Low,
			ClosePrice: c.Close,
			Volume:     c.Volume,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].OpenTime < out[j].OpenTime
	})
	return out
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
