package main

import (
	"errors"
	"testing"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/stretchr/testify/assert"
)

func TestPredictionsStatus(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	frames := []data.Frame{data.FiveMinute, data.FifteenMin, 4 * data.Hour}

	assert.Equal(t, "no native frames", predictionsStatus(nil, nil, at, nil))
	assert.Equal(t, "no live data", predictionsStatus(frames, nil, at, nil))
	assert.Equal(t, "error: boom", predictionsStatus(frames, nil, at, errors.New("boom")))
	assert.Equal(t, "updated 12:00:00 UTC", predictionsStatus(frames, []row{{Frame: "5m"}}, at, nil))
}

func TestDisplayRows_placeholders(t *testing.T) {
	t.Parallel()
	frames := []data.Frame{data.FiveMinute, data.FifteenMin}
	out := displayRows(frames, nil)
	assert.Len(t, out, 2)
	assert.Equal(t, "5m", out[0].Frame)
	assert.Equal(t, "15m", out[1].Frame)
}
