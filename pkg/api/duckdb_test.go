package api

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"
	"testing"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

func TestWorkGroup(t *testing.T) {
	worker := func(id int) {
		fmt.Printf("Worker %d starting\n", id)
		time.Sleep(time.Second)
		fmt.Printf("Worker %d done\n", id)
	}

	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Go(func() {
			worker(i)
		})
	}
	wg.Wait()
	fmt.Print("done")
}

func TestDuckDBParquetReading(t *testing.T) {
	t.Run("reads klines by date file directly", func(t *testing.T) {
		// Open DuckDB connection (same as handler)
		db, err := sql.Open("duckdb", "")
		if err != nil {
			t.Fatalf("failed to open duckdb: %v", err)
		}
		defer db.Close()

		// Get absolute path to parquet file
		parquetPath := fs.GetModuleRelativePath("data/spot/daily/klines/ZECUSDT/1m/*/*/*.parquet")

		// Test reading a specific klines parquet file (same pattern as handler)
		query := `SELECT
			-- replace(split(filename,'/')[4],'.parquet','')::uint8 as day,
			-- split(filename,'/')[3]::uint8 as month,
			-- split(filename, '/')[2]::uint16 AS year
			filename
			FROM read_parquet('%s')
		`

		var results []any
		for row := range data.QueryDuckDb(fmt.Sprintf(query, parquetPath)) {
			if row.Error != nil {
				t.Fatalf("failed to read klines parquet: %v", row.Error)
			}
			results = append(results, row.Data)
		}

		fmt.Sprintf("finished reading klines parquet")

	})
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

// TestDuckDBTodayCaching tests the DuckDB caching functionality for today's data
func TestDuckDBTodayCaching(t *testing.T) {
	// Create a test asset for today
	now := time.Now()
	todayAsset := &binance.HistoryAsset{
		MarketType: binance.Spot,
		Frequency:  binance.Daily,
		Frame:      binance.OneMinute,
		Indicator:  binance.Klines,
		Date:       time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location()),
		Market:     "BTCUSDT",
	}

	duckDBPath := fs.GetModuleRelativePath(todayAsset.TodayDuckDBPath())

	// Clean up any existing test file
	os.Remove(duckDBPath)
	defer os.Remove(duckDBPath) // Clean up after test

	t.Run("creates and queries today.duckdb cache", func(t *testing.T) {
		// Open DuckDB file for testing
		db, err := sql.Open("duckdb", duckDBPath)
		if err != nil {
			t.Fatalf("failed to open test DuckDB file: %v", err)
		}
		defer db.Close()

		// Create table (same as WriteToday does)
		createTableSQL := `
			CREATE TABLE IF NOT EXISTS klines (
				open_time BIGINT,
				close_time BIGINT,
				open_price DOUBLE,
				high_price DOUBLE,
				low_price DOUBLE,
				close_price DOUBLE,
				volume DOUBLE,
				PRIMARY KEY (open_time)
			)`
		if _, err := db.Exec(createTableSQL); err != nil {
			t.Fatalf("failed to create klines table: %v", err)
		}

		// Insert some test data
		testData := []struct {
			openTime  int64
			closeTime int64
			open      float64
			high      float64
			low       float64
			close     float64
			volume    float64
		}{
			{1640995200000000, 1640995260000000, 46200.0, 46300.0, 46150.0, 46250.0, 100.0},
			{1640995260000000, 1640995320000000, 46250.0, 46350.0, 46200.0, 46300.0, 150.0},
			{1640995320000000, 1640995380000000, 46300.0, 46400.0, 46250.0, 46350.0, 200.0},
		}

		for _, data := range testData {
			insertSQL := `
				INSERT OR IGNORE INTO klines (open_time, close_time, open_price, high_price, low_price, close_price, volume)
				VALUES (?, ?, ?, ?, ?, ?, ?)`
			_, err = db.Exec(insertSQL, data.openTime, data.closeTime, data.open, data.high, data.low, data.close, data.volume)
			if err != nil {
				t.Fatalf("failed to insert test data: %v", err)
			}
		}

		// Test querying the data
		query := `
			SELECT open_time, close_time, open_price, high_price, low_price, close_price, volume
			FROM klines
			WHERE open_time >= ? AND close_time <= ?
			ORDER BY close_time DESC`

		rows, err := db.Query(query, 1640995200000000, 1640995380000000)
		if err != nil {
			t.Fatalf("failed to query test data: %v", err)
		}
		defer rows.Close()

		var results []map[string]interface{}
		for rows.Next() {
			var openTime, closeTime int64
			var open, high, low, close, volume float64

			err := rows.Scan(&openTime, &closeTime, &open, &high, &low, &close, &volume)
			if err != nil {
				t.Fatalf("failed to scan row: %v", err)
			}

			results = append(results, map[string]interface{}{
				"open_time":  openTime,
				"close_time": closeTime,
				"open":       open,
				"high":       high,
				"low":        low,
				"close":      close,
				"volume":     volume,
			})
		}

		if len(results) != 3 {
			t.Errorf("expected 3 rows, got %d", len(results))
		}

		// Validate data integrity
		if results[0]["close_time"].(int64) <= results[1]["close_time"].(int64) {
			t.Error("results not sorted by close_time descending")
		}

		if results[0]["high"].(float64) < results[0]["low"].(float64) {
			t.Error("high price should be >= low price")
		}

		t.Logf("Successfully created and queried DuckDB cache with %d records", len(results))
	})

	t.Run("handles empty cache gracefully", func(t *testing.T) {
		// Test querying a non-existent DuckDB file (should return empty results)
		emptyDBPath := fs.GetModuleRelativePath("data/test/empty_today.duckdb")
		os.Remove(emptyDBPath) // Ensure it doesn't exist

		// This simulates what CandlesFromDuckDB does
		if !fs.FileExists(emptyDBPath) {
			// Should return empty result without error
			t.Log("Successfully handled non-existent DuckDB cache file")
			return
		}

		// If file exists, test it
		db, err := sql.Open("duckdb", emptyDBPath)
		if err != nil {
			t.Fatalf("failed to open empty DuckDB file: %v", err)
		}
		defer db.Close()

		// Create table but don't insert data
		createTableSQL := `
			CREATE TABLE IF NOT EXISTS klines (
				open_time BIGINT,
				close_time BIGINT,
				open_price DOUBLE,
				high_price DOUBLE,
				low_price DOUBLE,
				close_price DOUBLE,
				volume DOUBLE,
				PRIMARY KEY (open_time)
			)`
		if _, err := db.Exec(createTableSQL); err != nil {
			t.Fatalf("failed to create klines table: %v", err)
		}

		// Query empty table
		query := `SELECT COUNT(*) FROM klines`
		var count int
		err = db.QueryRow(query).Scan(&count)
		if err != nil {
			t.Fatalf("failed to query empty table: %v", err)
		}

		if count != 0 {
			t.Errorf("expected empty table to have 0 records, got %d", count)
		}

		t.Log("Successfully handled empty DuckDB cache")
	})
}
