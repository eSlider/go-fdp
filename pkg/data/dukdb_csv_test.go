package data

import (
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"
	// _ "github.com/marcboeker/go-duckdb"
)

func TestReadCSVChan(t *testing.T) {
	// Open an in-memory DuckDB instance
	db, err := sql.Open("duckdb", "")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Sample CSV data in a buffer
	csvData := strings.NewReader(`Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago`)
	type Person struct {
		Name string
		Age  int
		City string
	}

	csvBuf := &Buffer{
		data: []byte(`Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago`),
	}

	// Read CSV and write to parquet
	var results []*Person
	for row := range ReadHeaderlessCSV[Person](csvBuf) {
		if row.Error != nil {
			t.Errorf("error reading csv: %v", row.Error)
			continue
		}
		results = append(results, row.Value)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// Rewind csvData
	csvData.Seek(0, 0)
	results = []*Person{}

	// Read CSV and write to parquet
	resCh, errCh := ReadHeaderlessCSVChan[Person](csvData)
CSVLoop:
	for {
		select {
		case row, ok := <-resCh:
			if ok {
				results = append(results, row)
			} else {
				break CSVLoop
			}
		case err, ok := <-errCh:
			if ok {
				t.Errorf("error reading csv: %v", err)
			}
			break CSVLoop
		}
	}
	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// Rewind csvData
	csvData.Seek(0, 0)
	results = []*Person{}

	// Read CSV and write to parquet
	var closed bool
	for resCh, errCh := ReadHeaderlessCSVChan[Person](csvData); true; closed = false {
		// Handle channel closure
		select {
		case row, ok := <-resCh:
			if ok {
				results = append(results, row)
			} else {
				closed = true
			}
		case err, ok := <-errCh:
			if ok {
				t.Errorf("error reading csv: %v", err)
			}
			closed = true
		}

		if closed {
			break
		}
	}

	if len(results) != 3 {
		t.Errorf("expected 3 results, got %d", len(results))
	}

	// Register the buffer as a virtual file
	// Note: Direct buffer support may require DuckDB v0.8.0+ and proper driver support
	// Alternative: Write to a temporary file or use HTTPFS extension with in-memory server

	// Query the CSV directly (if file-based)
	// For true buffer support, consider using a temporary in-memory solution or check latest DuckDB Go driver capabilities

	// rows, err := db.Query(`SELECT * FROM read_csv_auto(?)`, csvData)
	// if err != nil {
	// 	t.Errorf("failed to query CSV: %v", err)
	// }
	// defer rows.Close()
	//
	// // Print results
	// for rows.Next() {
	// 	var name string
	// 	var age int
	// 	var city string
	// 	if err := rows.Scan(&name, &age, &city); err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("Name: %s, Age: %d, City: %s\n", name, age, city)
	// }
}
