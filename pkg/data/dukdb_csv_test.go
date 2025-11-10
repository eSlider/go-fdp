package data

import (
	"database/sql"
	"fmt"
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
	csvData := strings.NewReader(`name,age,city
Alice,30,New York
Bob,25,Los Angeles
Charlie,35,Chicago`)

	// Register the buffer as a virtual file
	// Note: Direct buffer support may require DuckDB v0.8.0+ and proper driver support
	// Alternative: Write to a temporary file or use HTTPFS extension with in-memory server

	// Query the CSV directly (if file-based)
	// For true buffer support, consider using a temporary in-memory solution or check latest DuckDB Go driver capabilities

	rows, err := db.Query(`SELECT * FROM read_csv_auto(?)`, csvData)
	if err != nil {
		t.Errorf("failed to query CSV: %v", err)
	}
	defer rows.Close()

	// Print results
	for rows.Next() {
		var name string
		var age int
		var city string
		if err := rows.Scan(&name, &age, &city); err != nil {
			panic(err)
		}
		fmt.Printf("Name: %s, Age: %d, City: %s\n", name, age, city)
	}
}
