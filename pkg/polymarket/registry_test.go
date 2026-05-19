package polymarket

import (
	"testing"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/stretchr/testify/assert"
)

func TestAlignWindowStart_5m(t *testing.T) {
	tm := time.Date(2026, 4, 3, 1, 52, 30, 0, time.UTC)
	aligned := AlignWindowStart(tm, data.FiveMinute)
	assert.Equal(t, time.Date(2026, 4, 3, 1, 50, 0, 0, time.UTC), aligned)
}

func TestSlugForWindow_5m(t *testing.T) {
	ws := time.Unix(1775181000, 0).UTC()
	slug := SlugForWindow(data.FiveMinute, ws)
	assert.Equal(t, "btc-updown-5m-1775181000", slug)
}

func TestSlugForWindow_15m(t *testing.T) {
	ws := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	slug := SlugForWindow(data.FifteenMin, ws)
	assert.Contains(t, slug, "btc-updown-15m-")
}

func TestResampleLast(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snaps := []Snapshot{
		{Time: base.Add(1 * time.Minute), UpPrice: 0.4},
		{Time: base.Add(4 * time.Minute), UpPrice: 0.6},
		{Time: base.Add(6 * time.Minute), UpPrice: 0.7},
	}
	out := ResampleLast(snaps, data.FiveMinute)
	assert.Len(t, out, 2)
}

func TestDedupeSnapshots(t *testing.T) {
	ts := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snaps := []Snapshot{
		{Time: ts, EventSlug: "a", UpPrice: 0.1},
		{Time: ts, EventSlug: "a", UpPrice: 0.9},
	}
	out := DedupeSnapshots(snaps)
	assert.Len(t, out, 1)
	assert.Equal(t, 0.9, out[0].UpPrice)
}
