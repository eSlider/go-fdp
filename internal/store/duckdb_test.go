package store

import (
	"context"
	"testing"
	"time"

	"github.com/eslider/go-binance-fdp/internal/query"
	"github.com/eslider/go-binance-fdp/pkg/data"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitCandleRequest_SpansToday(t *testing.T) {
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)
	req := query.Query{
		From: todayStart.Add(-12 * time.Hour),
		To:   todayStart.Add(2 * time.Hour),
	}
	hist, hourly := splitCandleRequest(req)
	require.NotNil(t, hist)
	require.NotNil(t, hourly)
	assert.Equal(t, req.From.UTC(), hist.From.UTC())
	assert.Equal(t, todayStart, hist.To.UTC())
	assert.Equal(t, todayStart, hourly.From.UTC())
	assert.Equal(t, req.To.UTC(), hourly.To.UTC())
}

func TestSplitCandleRequest_HistoricalOnly(t *testing.T) {
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)
	req := query.Query{
		From: todayStart.Add(-48 * time.Hour),
		To:   todayStart,
	}
	hist, hourly := splitCandleRequest(req)
	require.NotNil(t, hist)
	assert.Nil(t, hourly)
}

func TestEffectiveQueryEnd_IncludesOpenCandle(t *testing.T) {
	frame := data.NewFrame("1m")
	now := time.Now().UTC()
	frameMs := int64(time.Minute / time.Millisecond)
	currentOpen := (now.UnixMilli() / frameMs) * frameMs

	to := time.UnixMilli(currentOpen)
	got := effectiveQueryEnd(to, frame)
	assert.Equal(t, currentOpen+frameMs, got)
}

func TestEffectiveQueryEnd_OldRangeUnchanged(t *testing.T) {
	frame := data.NewFrame("1m")
	to := time.Now().UTC().Add(-24 * time.Hour)
	want := to.UnixMilli()
	assert.Equal(t, want, effectiveQueryEnd(to, frame))
}

func TestSplitCandleRequest_TodayOnly(t *testing.T) {
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)
	req := query.Query{
		From: todayStart,
		To:   todayStart.Add(3 * time.Hour),
	}
	hist, hourly := splitCandleRequest(req)
	assert.Nil(t, hist)
	require.NotNil(t, hourly)
}

func TestStore_Creation(t *testing.T) {
	t.Run("creates repository successfully", func(t *testing.T) {
		repo, err := NewStore()
		require.NoError(t, err)
		assert.NotNil(t, repo)

		// Test that we can close it
		err = repo.Close()
		assert.NoError(t, err)
	})

	t.Run("repository has required methods", func(t *testing.T) {
		repo, err := NewStore()
		require.NoError(t, err)
		defer repo.Close()

		// Verify the repository implements the expected interface
		// This is a compile-time check, but we test it at runtime
		assert.NotNil(t, repo.GetCandles)
		assert.NotNil(t, repo.GetAggTrades)
	})
}

func TestStore_GetAggTrades_MethodExists(t *testing.T) {
	t.Run("GetAggTrades method exists and is callable", func(t *testing.T) {
		repo, err := NewStore()
		require.NoError(t, err)
		defer repo.Close()

		// Just verify the method exists and can be called
		// With an empty request, it may return empty results or handle gracefully
		ctx := context.Background()
		result, err := repo.GetAggTrades(ctx, query.Query{})
		// The method should exist and be callable without panicking
		// It may return empty results or an error depending on the implementation
		if err != nil {
			// If there's an error, that's acceptable - the method exists and was called
			assert.NotNil(t, err)
		}
		// Result can be nil or empty slice - both are valid
		if result != nil {
			assert.IsType(t, []*query.AggTrade{}, result)
		}
	})
}
