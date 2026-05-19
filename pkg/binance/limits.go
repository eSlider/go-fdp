package binance

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Spot REST IP request-weight limit per minute (see Binance general info — IP limits).
const weightLimitPerMinute = 6000

const (
	maxRequestAttempts  = 6
	mbxUsedWeightPrefix = "X-Mbx-Used-Weight-"
)

var (
	// ErrRateLimited is returned when Binance responds with HTTP 429 after retries are exhausted.
	ErrRateLimited = errors.New("binance rate limited")
	// ErrIPBanned is returned when Binance responds with HTTP 418 after retries are exhausted.
	ErrIPBanned = errors.New("binance ip banned")
)

// RateLimitError carries HTTP status, optional Retry-After, and the API error body.
type RateLimitError struct {
	StatusCode int
	RetryAfter time.Duration
	API        *ErrorResponse
}

func (e *RateLimitError) Error() string {
	if e.API.Msg != "" {
		return fmt.Sprintf("binance http %d: %s (code %d)", e.StatusCode, e.API.Msg, e.API.Code)
	}
	return fmt.Sprintf("binance http %d", e.StatusCode)
}

func (e *RateLimitError) Unwrap() error {
	switch e.StatusCode {
	case http.StatusTeapot:
		return ErrIPBanned
	case http.StatusTooManyRequests:
		return ErrRateLimited
	default:
		return nil
	}
}

var defaultIPLimiter ipLimiter

type ipLimiter struct {
	mu         sync.Mutex
	usedWeight int
}

func (l *ipLimiter) updateFromHeaders(h http.Header) {
	if w, ok := parseMBXUsedWeight(h); ok {
		l.mu.Lock()
		l.usedWeight = w
		l.mu.Unlock()
	}
}

// waitIfHeavy applies proactive backoff when the IP is close to the per-minute weight cap.
func (l *ipLimiter) waitIfHeavy(ctx context.Context) error {
	l.mu.Lock()
	w := l.usedWeight
	l.mu.Unlock()

	var delay time.Duration
	switch {
	case w >= weightLimitPerMinute-50:
		delay = 15 * time.Second
	case w >= weightLimitPerMinute-200:
		delay = 5 * time.Second
	case w >= weightLimitPerMinute-500:
		delay = time.Second
	default:
		return nil
	}

	return sleepContext(ctx, delay)
}

func parseMBXUsedWeight(h http.Header) (int, bool) {
	for k, vals := range h {
		if !strings.HasPrefix(k, mbxUsedWeightPrefix) || len(vals) == 0 {
			continue
		}
		w, err := strconv.Atoi(vals[0])
		if err != nil {
			continue
		}
		return w, true
	}
	return 0, false
}

// retryAfterDuration returns the duration to wait before retrying a request.
func retryAfterDuration(h http.Header, statusCode int, attempt int) time.Duration {
	if s := h.Get("Retry-After"); s != "" {
		if secs, err := strconv.Atoi(s); err == nil && secs > 0 {
			return time.Duration(secs) * time.Second
		}
	}
	switch statusCode {
	case http.StatusTeapot:
		return 60 * time.Second
	case http.StatusTooManyRequests:
		backoff := time.Duration(1<<attempt) * time.Second
		if backoff > 30*time.Second {
			return 30 * time.Second
		}
		return backoff
	case http.StatusForbidden:
		return 5 * time.Minute
	default:
		return time.Second
	}
}

func isRetryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusTeapot, http.StatusForbidden:
		return true
	default:
		return false
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
