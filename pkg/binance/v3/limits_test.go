package v3

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseMBXUsedWeight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		headers http.Header
		want    int
		ok      bool
	}{
		{
			name: "1m interval",
			headers: http.Header{
				"X-Mbx-Used-Weight-1m": []string{"42"},
			},
			want: 42,
			ok:   true,
		},
		{
			name:    "missing",
			headers: http.Header{},
			ok:      false,
		},
		{
			name: "invalid value skipped",
			headers: http.Header{
				"X-Mbx-Used-Weight-1m": []string{"nope"},
			},
			ok: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := parseMBXUsedWeight(tt.headers)
			assert.Equal(t, tt.ok, ok)
			if tt.ok {
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestRetryAfterDuration(t *testing.T) {
	t.Parallel()

	t.Run("uses Retry-After header", func(t *testing.T) {
		t.Parallel()
		h := http.Header{"Retry-After": []string{"7"}}
		assert.Equal(t, 7*time.Second, retryAfterDuration(h, http.StatusTooManyRequests, 0))
	})

	t.Run("429 exponential fallback", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, time.Second, retryAfterDuration(http.Header{}, http.StatusTooManyRequests, 0))
		assert.Equal(t, 4*time.Second, retryAfterDuration(http.Header{}, http.StatusTooManyRequests, 2))
	})

	t.Run("418 default", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 60*time.Second, retryAfterDuration(http.Header{}, http.StatusTeapot, 0))
	})
}

func TestIPLimiter_waitIfHeavy(t *testing.T) {
	t.Parallel()

	var l ipLimiter
	ctx := t.Context()

	require.NoError(t, l.waitIfHeavy(ctx))

	l.usedWeight = weightLimitPerMinute - 100
	start := time.Now()
	require.NoError(t, l.waitIfHeavy(ctx))
	assert.GreaterOrEqual(t, time.Since(start), time.Second)
}
