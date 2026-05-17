package v3

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"sync-v3/pkg/data"
	"time"

	"github.com/goccy/go-json"
)

//Data is returned in ascending order. Oldest first, newest last.
//All time and timestamp related fields are in milliseconds.

const defaultBaseURL = "https://api.binance.com"

// The base endpoint https://data-api.binance.vision can be used to access the following API endpoints that have NONE as security type:
// GET /api/v3/aggTrades
// GET /api/v3/avgPrice
// GET /api/v3/depth
// GET /api/v3/exchangeInfo
// GET /api/v3/klines
// GET /api/v3/ping
// GET /api/v3/ticker
// GET /api/v3/ticker/24hr
// GET /api/v3/ticker/bookTicker
// GET /api/v3/ticker/price
// GET /api/v3/time
// GET /api/v3/trades
// GET /api/v3/uiKlines
const dataApiBaseURL = "https://data-api.binance.vision"

// https://developers.binance.com/docs/derivatives/portfolio-margin-pro/general-info#general-api-information
var BaseUrls = []string{
	defaultBaseURL,

	// The last 4 endpoints in the point above (api1-api4) might give better performance but have less stability. Please use whichever works best for your setup.
	"https://api1.binance.com",
	"https://api2.binance.com",
	"https://api3.binance.com",
	"https://api4.binance.com",
}

var ctx = context.Background()

// SymbolRequest is the common symbol and time-range query parameters.
type SymbolRequest struct {
	// Exclude context.Context to avoid circular dependency
	Context context.Context `json:"-" validate:"-"`

	Symbol    string `in:"query=symbol;required" validate:"required"`
	StartTime *int64 `in:"query=startTime;omitempty"`
	EndTime   *int64 `in:"query=endTime;omitempty"`
}

// AggTradeRequest is the query for GET /api/v3/aggTrades.
// Ref: https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
type AggTradeRequest struct {
	Base   SymbolRequest
	FromID *int64 `in:"query=fromId;omitempty"`
	Limit  int64  `in:"query=limit;omitempty"`
}

// Use a configured HTTP client with a timeout to prevent hanging
// Binance API rate limits are primarily governed by two categories: Request Weight limits, which apply per IP address, and Order limits, which apply per API key.  The current hard limits for the Spot API are 6,000 request weight per minute, 100 orders per 10 seconds, and 200,000 orders per 24 hours.
// Request Weight: The limit is 6,000 per minute (updated in August 2023).  Different endpoints consume different amounts of weight; for example, a single symbol query costs 1 weight, while fetching all symbols may cost significantly more.
// Order Limits: Users are restricted to 100 new orders every 10 seconds and 200,000 orders every 24 hours.  These limits are specific to the API key used.
// WebSocket Limits: There is a limit of 300 connections per attempt every 5 minutes per IP address.
// Machine Learning (ML) Limits: Binance employs ML to detect abusive trading behavior, such as front-running or excessive order cancellation. Violations can result in bans ranging from 5 minutes to 3 days.
// Web Application Firewall (WAF): Excessive requests or malicious patterns can trigger HTTP 403 errors, typically resulting in a 5-minute ban per IP, though longer bans may apply for severe violations.
// If you exceed these limits, you will receive an HTTP 429 status code.  To manage limits, you can query the current status using the /api/v3/exchangeInfo endpoint, which returns the rateLimits array containing the current usage count and limits for each interval.
var client = &http.Client{
	// Let insecure, skip TLS verification
	Transport: &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
	}},
	Timeout: 10 * time.Second,
}

type ErrorResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func GetCast[T any](path string, req any) (l []*T, err error) {
	params, err := data.Params(req)
	if err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	url := fmt.Sprintf("%s/%s?%s", defaultBaseURL, path, params)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)

	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {

		// Rewind the body to allow for retry, and get the error message
		//resx, err := io.ReadAll(resp.Body)
		//if err != nil {
		//	return nil, fmt.Errorf("failed to read response body: %w", err)
		//}
		var er ErrorResponse
		if err = json.NewDecoder(resp.Body).Decode(&er); err != nil {
			return nil, fmt.Errorf("failed to unmarshal response: %w", err)
		}

		return nil, fmt.Errorf("API error %d, %d, %s", resp.StatusCode, er.Code, er.Msg)
	}

	if err = json.NewDecoder(resp.Body).Decode(&l); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return l, nil
}

// AggTrades fetches compressed aggregate trades.
func AggTrades(req *AggTradeRequest) ([]*AggTrade, error) {
	return GetCast[AggTrade]("api/v3/aggTrades", req)
}

// Klines fetches kline/candlestick data.
func Klines(req *KlineRequest) ([]*Kline, error) {
	return GetCast[Kline]("api/v3/klines", req)
}
