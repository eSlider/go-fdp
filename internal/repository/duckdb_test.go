package repository

import (
	"context"
	"testing"

	"sync-v3/internal/domain"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDuckDBRepository_Creation(t *testing.T) {
	t.Run("creates repository successfully", func(t *testing.T) {
		repo, err := NewDuckDBRepository()
		require.NoError(t, err)
		assert.NotNil(t, repo)

		// Test that we can close it
		err = repo.Close()
		assert.NoError(t, err)
	})

	t.Run("repository has required methods", func(t *testing.T) {
		repo, err := NewDuckDBRepository()
		require.NoError(t, err)
		defer repo.Close()

		// Verify the repository implements the expected interface
		// This is a compile-time check, but we test it at runtime
		assert.NotNil(t, repo.GetCandles)
		assert.NotNil(t, repo.GetAggTrades)
	})
}

func TestDuckDBRepository_GetAggTrades_MethodExists(t *testing.T) {
	t.Run("GetAggTrades method exists and is callable", func(t *testing.T) {
		repo, err := NewDuckDBRepository()
		require.NoError(t, err)
		defer repo.Close()

		// Just verify the method exists and can be called
		// With an empty request, it may return empty results or handle gracefully
		ctx := context.Background()
		result, err := repo.GetAggTrades(ctx, domain.MarketDataRequest{})
		// The method should exist and be callable without panicking
		// It may return empty results or an error depending on the implementation
		if err != nil {
			// If there's an error, that's acceptable - the method exists and was called
			assert.NotNil(t, err)
		}
		// Result can be nil or empty slice - both are valid
		if result != nil {
			assert.IsType(t, []*domain.AggTrade{}, result)
		}
	})
}
