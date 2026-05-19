//go:build integration

package polymarket

import (
	"context"
	"testing"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/stretchr/testify/require"
)

func TestIntegration_FetchEvent5m(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	c := NewClient()
	now := time.Now().UTC()
	slug := SlugForWindow(data.FiveMinute, now)
	ev, err := c.FetchEventBySlug(context.Background(), slug)
	if err != nil {
		t.Skipf("no live market: %v", err)
	}
	require.NotEmpty(t, ev.UpTokenID)
}
