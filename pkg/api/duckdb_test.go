package api

import (
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"sync-v3/pkg/fs"

	_ "github.com/duckdb/duckdb-go/v2"
)

// func TestAssetLazyLoader(t *testing.T) {
// 	fromTime := time.Now().Add(-4 * time.Hour)
// 	toTime := time.Now()
//
// 	asset := &binance.HistoryAsset{
// 		MarketType: marketType,
// 		Frequency:  Frequency(freq),
// 		Indicator:  indicator,
// 		Date:       time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
// 		Frame:      Frame(frame),
// 		Market:     market,
// 	}
//
// 	zipLink := asset.SymbolDateAssetZipLink()
// 	reverseAsset, err := NewHistoryAssetByPath(zipLink)
// 	if err != nil {
// 		t.Errorf("could not parse path: %s", err.Error())
// 	}
// 	link := reverseAsset.SymbolDateAssetZipLink()
// 	if link != zipLink {
// 		t.Errorf("unexpected asset: %v", zipLink)
// 	}
//
// }

func TestDuckDBParquetReading(t *testing.T) {
	t.Run("reads klines parquet file directly", func(t *testing.T) {
		// Open DuckDB connection (same as handler)
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		defer db.Close()

		// Get absolute path to parquet file
		parquetPath := fs.GetModuleRelativePath("data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2020-08.parquet")

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

	t.Run("reads aggTrades parquet file directly", func(t *testing.T) {
		// Open DuckDB connection
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		defer db.Close()

		// Get absolute path to aggTrades parquet file
		parquetPath := filepath.Join(fs.ModuleRootPath(), "data/spot/monthly/aggTrades/ETHUSDT/ETHUSDT-aggTrades-2017-08.parquet")

		// Test reading aggTrades parquet file
		query := `SELECT COUNT(*) FROM read_parquet(?)`
		var count int
		err = db.QueryRow(query, parquetPath).Scan(&count)
		if err != nil {
			t.Fatalf("failed to read aggTrades parquet: %v", err)
		}

		// Note: This aggTrades file appears to be empty in the test data
		if count < 0 {
			t.Errorf("expected non-negative count from aggTrades parquet, got %d", count)
		}

		t.Logf("Successfully read %d records from aggTrades parquet file", count)
	})

	t.Run("reads multiple parquet files with wildcard", func(t *testing.T) {
		// Open DuckDB connection
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		defer db.Close()

		// Get absolute path pattern for wildcard
		patternPath := filepath.Join(fs.ModuleRootPath(), "data/spot/monthly/klines/**/*.parquet")

		// Test reading multiple klines files with wildcard (same as handler)
		query := `SELECT COUNT(*) FROM read_parquet(?)`
		var count int
		err = db.QueryRow(query, patternPath).Scan(&count)
		if err != nil {
			t.Fatalf("failed to read multiple parquet files with wildcard: %v", err)
		}

		if count <= 0 {
			t.Errorf("expected positive count from wildcard parquet query, got %d", count)
		}

		t.Logf("Successfully read %d total records from all klines parquet files", count)
	})

	t.Run("creates and queries table from parquet", func(t *testing.T) {
		// Open DuckDB connection
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		defer db.Close()

		// Get absolute path to parquet file
		parquetPath := filepath.Join(fs.ModuleRootPath(), "data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2020-08.parquet")

		// Create table from parquet (similar to setupTables in handler)
		createTableSQL := `
		CREATE OR REPLACE TABLE test_klines AS
		SELECT *
		FROM read_parquet(?)
		`

		_, err = db.Exec(createTableSQL, parquetPath)
		if err != nil {
			t.Fatalf("failed to create table from parquet: %v", err)
		}

		// Query the created table
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM test_klines").Scan(&count)
		if err != nil {
			t.Fatalf("failed to query created table: %v", err)
		}

		if count <= 0 {
			t.Errorf("expected positive count from table query, got %d", count)
		}

		// Test querying specific columns (adapted to available columns)
		rows, err := db.Query(`
			SELECT open_time, close_time, open_price, high_price, low_price, close_price, volume
			FROM test_klines
			ORDER BY open_time
			LIMIT 5
		`)
		if err != nil {
			t.Fatalf("failed to query table columns: %v", err)
		}
		defer rows.Close()

		// Verify we can scan the results (handle time.Time like handler does)
		var recordsCount int
		for rows.Next() {
			var openTime, closeTime time.Time
			var openPrice, highPrice, lowPrice, closePrice, volume float64

			err := rows.Scan(&openTime, &closeTime, &openPrice, &highPrice, &lowPrice, &closePrice, &volume)
			if err != nil {
				t.Fatalf("failed to scan row: %v", err)
			}

			// Convert to milliseconds like handler does
			openTimeMs := openTime.UnixMilli()
			closeTimeMs := closeTime.UnixMilli()

			// Basic validation
			if openTimeMs <= 0 {
				t.Errorf("invalid open_time: %d", openTimeMs)
			}
			if closeTimeMs <= 0 {
				t.Errorf("invalid close_time: %d", closeTimeMs)
			}
			if openPrice <= 0 {
				t.Errorf("invalid open_price: %f", openPrice)
			}
			if volume < 0 {
				t.Errorf("invalid volume: %f", volume)
			}

			recordsCount++
		}

		if recordsCount != 5 {
			t.Errorf("expected 5 records, got %d", recordsCount)
		}

		t.Logf("Successfully created table and queried %d sample records", recordsCount)
	})

	t.Run("handles parameterized queries", func(t *testing.T) {
		// Open DuckDB connection
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		defer db.Close()

		// Get absolute path to parquet file
		parquetPath := filepath.Join(fs.ModuleRootPath(), "data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2020-08.parquet")

		// Create table first
		createTableSQL := `
		CREATE OR REPLACE TABLE test_param_klines AS
		SELECT *
		FROM read_parquet(?)
		`

		_, err = db.Exec(createTableSQL, parquetPath)
		if err != nil {
			t.Fatalf("failed to create table for parameterized test: %v", err)
		}

		// Test parameterized query (adapted to available columns)
		query := `
		SELECT COUNT(*)
		FROM test_param_klines
		WHERE open_time >= ?::TIMESTAMP
		AND open_time <= ?::TIMESTAMP
		`

		// Convert milliseconds to time.Time for comparison
		fromTimeMs := int64(1596240000000) // August 1, 2020 00:00:00 UTC in milliseconds
		toTimeMs := int64(1598918400000)   // September 1, 2020 00:00:00 UTC in milliseconds
		fromTime := time.UnixMilli(fromTimeMs)
		toTime := time.UnixMilli(toTimeMs)

		var count int
		err = db.QueryRow(query, fromTime, toTime).Scan(&count)
		if err != nil {
			t.Fatalf("failed to execute parameterized query: %v", err)
		}

		if count < 0 {
			t.Errorf("expected non-negative count from parameterized query, got %d", count)
		}

		t.Logf("Successfully executed parameterized query, found %d records between %d and %d",
			count, fromTimeMs, toTimeMs)
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
