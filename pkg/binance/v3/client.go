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

// SymbolRequest is the common symbol and time-range query parameters.
type SymbolRequest struct {
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

// Use a configured HTTP client with a timeout to prevent hanging.
// IP limits: 6000 request weight per minute; 429 on violation; 418 when banned after repeated 429s.
// See: https://developers.binance.com/docs/derivatives/portfolio-margin-pro/general-info#ip-limits
var client = &http.Client{
	Transport: &http.Transport{TLSClientConfig: &tls.Config{
		InsecureSkipVerify: true,
	}},
	Timeout: 30 * time.Second,
}

type ErrorResponse struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func GetCast[T any](ctx context.Context, path string, req any) (l []*T, err error) {
	params, err := data.Params(req)
	if err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	url := fmt.Sprintf("%s/%s?%s", dataApiBaseURL, path, params)

	var lastLimitErr *RateLimitError
	for attempt := 0; attempt < maxRequestAttempts; attempt++ {
		if err := defaultIPLimiter.waitIfHeavy(ctx); err != nil {
			return nil, err
		}

		httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		resp, err := client.Do(httpReq)
		if err != nil {
			return nil, fmt.Errorf("failed to make request: %w", err)
		}

		defaultIPLimiter.updateFromHeaders(resp.Header)
		if resp.StatusCode == http.StatusOK {
			if err = json.NewDecoder(resp.Body).Decode(&l); err != nil {
				resp.Body.Close()
				return nil, fmt.Errorf("failed to unmarshal response: %w", err)
			}
			resp.Body.Close()
			return l, nil
		}

		errResp, decodeErr := DecodeAPIError(resp)
		resp.Body.Close()

		// Retry after rate limit
		if isRetryableStatus(resp.StatusCode) && attempt < maxRequestAttempts-1 {
			wait := retryAfterDuration(resp.Header, resp.StatusCode, attempt)
			lastLimitErr = &RateLimitError{
				StatusCode: resp.StatusCode,
				RetryAfter: wait,
				API:        errResp,
			}
			if err := sleepContext(ctx, wait); err != nil {
				return nil, err
			}
			continue
		}

		if decodeErr != nil {
			return nil, fmt.Errorf("http %d: %w", resp.StatusCode, decodeErr)
		}

		// IP limits exceeded
		if isRetryableStatus(resp.StatusCode) {
			return nil, &RateLimitError{
				StatusCode: resp.StatusCode,
				RetryAfter: retryAfterDuration(resp.Header, resp.StatusCode, attempt),
				API:        errResp,
			}
		}
		return nil, fmt.Errorf("API error %d, %d, %s", resp.StatusCode, errResp.Code, errResp.Msg)
	}

	if lastLimitErr != nil {
		return nil, lastLimitErr
	}
	return nil, fmt.Errorf("binance request failed after %d attempts", maxRequestAttempts)
}

// DecodeAPIError decodes the API error response from Binance.
func DecodeAPIError(resp *http.Response) (er *ErrorResponse, err error) {
	er = new(ErrorResponse)
	if err := json.NewDecoder(resp.Body).Decode(er); err != nil {
		return nil, fmt.Errorf("unmarshal error body: %w", err)
	}
	return er, nil
}

// AggTrades fetches compressed aggregate trades.
func AggTrades(ctx context.Context, req *AggTradeRequest) ([]*AggTrade, error) {
	return GetCast[AggTrade](ctx, "api/v3/aggTrades", req)
}

// Klines fetches kline/candlestick data.
func Klines(ctx context.Context, req *KlineRequest) ([]*Kline, error) {
	return GetCast[Kline](ctx, "api/v3/klines", req)
}
