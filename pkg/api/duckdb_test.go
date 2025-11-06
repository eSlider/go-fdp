package api

import (
	"database/sql"
	"sync-v3/pkg/fs"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestDuckDBParquetReading(t *testing.T) {
	t.Run("reads klines parquet file directly", func(t *testing.T) {
		// Open DuckDB connection (same as handler)
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		defer db.Close()

		// Get absolute path to parquet file
		parquetPath := fs.GetModuleRelativePath("data/spot/daily/klines/*/1m/*/*/*.parquet")

		// Test reading a specific klines parquet file (same pattern as handler)
		query := `SELECT COUNT(*) FROM read_parquet(?)`
		var count int
		err = db.QueryRow(query, parquetPath).Scan(&count)
		if err != nil {
			t.Fatalf("failed to read klines parquet: %v", err)
		}

		if count <= 0 {
			t.Errorf("expected positive count from klines parquet, got %d", count)
		}

		t.Logf("Successfully read %d records from klines parquet file", count)
	})

	t.Run("handles non-existent parquet file", func(t *testing.T) {
		// Open DuckDB connection
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		defer db.Close()

		// Try to read non-existent file
		query := `SELECT COUNT(*) FROM read_parquet('non_existent_file.parquet')`
		var count int
		err = db.QueryRow(query).Scan(&count)

		// Should expect an error for non-existent file
		if err == nil {
			t.Error("expected error when reading non-existent parquet file, but got none")
		}

		t.Logf("Correctly handled error for non-existent file: %v", err)
	})
}
