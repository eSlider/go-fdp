package data

import (
	"database/sql"
	"fmt"

	_ "github.com/duckdb/duckdb-go/v2"
)

type DuckDbRow struct {
	Data  map[string]any
	Error error
}

// QueryDuckDb queries DuckDB and returns the number of rows.
func QueryDuckDb(query string) (rowCh chan *DuckDbRow) {
	rowCh = make(chan *DuckDbRow)

	go func() {
		db, err := sql.Open("duckdb", "")
		if err != nil {
			rowCh <- &DuckDbRow{Error: fmt.Errorf("failed to open DuckDB: %v", err)}
			return
		}
		defer db.Close()

		rows, err := db.Query(query)
		if err != nil {
			rowCh <- &DuckDbRow{Error: fmt.Errorf("failed to execute query: %v", err)}
		}

		columns, err := rows.Columns()
		if err != nil {
			rowCh <- &DuckDbRow{Error: fmt.Errorf("failed to get columns: %v", err)}
		}

		data := make([]any, len(columns))
		for i := range data {
			data[i] = new(sql.RawBytes)
		}

		for rows.Next() {
			err = rows.Scan(data...)
			if err != nil {
				rowCh <- &DuckDbRow{Error: fmt.Errorf("failed to scan row: %v", err)}
			}
			var entry = make(map[string]any)
			for i, column := range columns {
				switch v := data[i].(type) {
				case *sql.RawBytes:
					entry[column] = string(*v)
				case []byte:
					entry[column] = string(v)
				case nil:
					entry[column] = nil
				default:
					entry[column] = v
				}
			}
			rowCh <- &DuckDbRow{Data: entry}
		}
		close(rowCh)
	}()

	return
}
