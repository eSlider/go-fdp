# Data storage

To store internal financial data, duckdb and hive parquet partitions are used.

It's allows to use SQL queries to analyze data like without backend, duckdb is used as a query engine.


## Klines query

```sql
SELECT
    make_timestamp(year, month, day,
            date_part('hour', open_time),
            date_part('minute', open_time),
            date_part('second', open_time)) as openTime,
    openTime + interval '1' minute - interval '1' millisecond AS closeTime,
    mtype, indicator, market, frame,

    open_price as open,
    close_price as close,
    high_price as high,
    low_price as low,

    volume as volume

FROM read_parquet('data/*/*/*/*/*/*/*/*.parquet')

WHERE mtype = 'spot'
  AND indicator = 'klines'
  AND market = 'BTCUSDT'
  AND frame = '1m'

    --AND open_time
	AND(openTime BETWEEN epoch_ms(1754179200000) AND epoch_ms(1754265600000))
```
