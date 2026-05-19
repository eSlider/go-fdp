package integrity

import (
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/duckdb/duckdb-go/v2"
)

// OpenDB opens an in-memory DuckDB connection for parquet audits.
func OpenDB() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}
	return db, nil
}

func scanIssues(rows *sql.Rows) ([]Issue, error) {
	var issues []Issue
	for rows.Next() {
		var code, severity, detail string
		var openTime int64
		if err := rows.Scan(&code, &severity, &openTime, &detail); err != nil {
			return nil, err
		}
		issues = append(issues, Issue{
			Code:     Code(code),
			Severity: Severity(severity),
			OpenTime: openTime,
			Detail:   detail,
		})
	}
	return issues, rows.Err()
}

func sqlQuotePath(path string) string {
	return "'" + strings.ReplaceAll(path, "'", "''") + "'"
}
