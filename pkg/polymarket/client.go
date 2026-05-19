package polymarket

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/goccy/go-json"
)

const (
	gammaBaseURL = "https://gamma-api.polymarket.com"
	clobBaseURL  = "https://clob.polymarket.com"
)

// Client calls Polymarket Gamma and CLOB HTTP APIs.
type Client struct {
	http      *http.Client
	gamma     string
	clob      string
	gammaRL   time.Duration
	clobRL    time.Duration
	lastGamma time.Time
	lastClob  time.Time
}

func NewClient() *Client {
	return &Client{
		http:    &http.Client{Timeout: 30 * time.Second},
		gamma:   gammaBaseURL,
		clob:    clobBaseURL,
		gammaRL: 500 * time.Millisecond,
		clobRL:  200 * time.Millisecond,
	}
}

func (c *Client) getJSON(ctx context.Context, base, path string, query url.Values, dest any) error {
	if err := c.throttle(ctx, base == c.gamma); err != nil {
		return err
	}
	u, err := url.Parse(base + path)
	if err != nil {
		return err
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		return ErrNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("polymarket %s %s: status %d: %s", base, path, resp.StatusCode, truncate(body, 200))
	}
	if err := json.Unmarshal(body, dest); err != nil {
		return fmt.Errorf("decode %s: %w", path, err)
	}
	return nil
}

func (c *Client) throttle(ctx context.Context, gamma bool) error {
	var wait time.Duration
	now := time.Now()
	if gamma {
		if !c.lastGamma.IsZero() {
			wait = c.gammaRL - now.Sub(c.lastGamma)
		}
		if wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
		c.lastGamma = time.Now()
	} else {
		if !c.lastClob.IsZero() {
			wait = c.clobRL - now.Sub(c.lastClob)
		}
		if wait > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}
		c.lastClob = time.Now()
	}
	return nil
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n])
}
