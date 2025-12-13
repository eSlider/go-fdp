package repository

import (
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

		// Just verify the method exists and can be called (will return empty results)
		// Note: This will fail due to no parquet files, but that's expected behavior
		result, err := repo.GetAggTrades(nil, domain.MarketDataRequest{})
		// We expect an error due to no files found, but the method should exist and be callable
		assert.Error(t, err)
		assert.Nil(t, result)
		assert.Contains(t, err.Error(), "No files found")
	})
}
