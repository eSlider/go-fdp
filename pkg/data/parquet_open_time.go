package data

import "fmt"

// ParquetOpenTimeMSExpr returns a DuckDB expression for open_time column → epoch milliseconds.
// midnight is UTC midnight of the parquet file's calendar day.
func ParquetOpenTimeMSExpr(year, month, day int) string {
	return fmt.Sprintf(`CASE
		WHEN typeof(open_time) = 'TIME' THEN epoch_ms(make_timestamp(%d, %d, %d, 0, 0, 0) + (date_part('epoch', open_time) * interval '1 second'))::BIGINT
		WHEN try_cast(open_time AS BIGINT) < 86400000 THEN epoch_ms(make_timestamp(%d, %d, %d, 0, 0, 0) + (try_cast(open_time AS BIGINT) * interval '1 millisecond'))::BIGINT
		ELSE try_cast(open_time AS BIGINT)
	END`, year, month, day, year, month, day)
}
