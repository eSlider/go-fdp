package polymarket

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveCurrentEvent_fallsBackTo5m(t *testing.T) {
	t.Parallel()
	var calls []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slug := r.URL.Query().Get("slug")
		calls = append(calls, slug)
		if !strings.HasPrefix(slug, "btc-updown-5m-") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		_, _ = w.Write([]byte(`[{"slug":"` + slug + `","markets":[{"clobTokenIds":"[\"up\",\"down\"]","outcomes":"[\"Up\",\"Down\"]","outcomePrices":"[\"0.6\",\"0.4\"]"}]}]`))
	}))
	t.Cleanup(srv.Close)

	c := NewClient()
	c.gamma = srv.URL
	c.gammaRL = 0
	col := NewCollector(c, NewStore(""))

	now := time.Date(2026, 5, 27, 12, 3, 0, 0, time.UTC)
	ws := AlignWindowStart(now, 4*data.Hour)
	ev, fallback, err := col.resolveCurrentEvent(context.Background(), 4*data.Hour, ws, now)
	require.NoError(t, err)
	require.True(t, fallback)
	require.NotNil(t, ev)
	assert.GreaterOrEqual(t, len(calls), 2)
	assert.True(t, strings.HasPrefix(calls[0], "btc-updown-4h-"))
	assert.True(t, strings.HasPrefix(calls[len(calls)-1], "btc-updown-5m-"))
}
