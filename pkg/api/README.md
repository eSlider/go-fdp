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
  AND openTime BETWEEN '2025-08-03 00:00:00'::TIMESTAMP AND '2025-08-03 00:10:00'::TIMESTAMP
```

Result

```json
[
  {
    "openTime": "2025-08-03 00:10:00",
    "closeTime": "2025-08-03 00:10:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112655.02,
    "close": 112653.73,
    "high": 112660,
    "low": 112642,
    "volume": 3.19551
  },
  {
    "openTime": "2025-08-03 00:09:00",
    "closeTime": "2025-08-03 00:09:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112622.44,
    "close": 112655.01,
    "high": 112655.02,
    "low": 112622.43,
    "volume": 1.29583
  },
  {
    "openTime": "2025-08-03 00:08:00",
    "closeTime": "2025-08-03 00:08:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112647.61,
    "close": 112622.44,
    "high": 112647.61,
    "low": 112613.5,
    "volume": 1.68788
  },
  {
    "openTime": "2025-08-03 00:07:00",
    "closeTime": "2025-08-03 00:07:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112612.12,
    "close": 112647.61,
    "high": 112647.61,
    "low": 112608.65,
    "volume": 3.98763
  },
  {
    "openTime": "2025-08-03 00:06:00",
    "closeTime": "2025-08-03 00:06:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112575.31,
    "close": 112612.12,
    "high": 112622.5,
    "low": 112575.31,
    "volume": 5.82082
  },
  {
    "openTime": "2025-08-03 00:05:00",
    "closeTime": "2025-08-03 00:05:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112640.4,
    "close": 112575.31,
    "high": 112655.35,
    "low": 112575.3,
    "volume": 1.94464
  },
  {
    "openTime": "2025-08-03 00:04:00",
    "closeTime": "2025-08-03 00:04:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112633.67,
    "close": 112640.4,
    "high": 112646.28,
    "low": 112627.16,
    "volume": 8.66881
  },
  {
    "openTime": "2025-08-03 00:03:00",
    "closeTime": "2025-08-03 00:03:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112611.02,
    "close": 112633.67,
    "high": 112633.67,
    "low": 112568.63,
    "volume": 14.24115
  },
  {
    "openTime": "2025-08-03 00:02:00",
    "closeTime": "2025-08-03 00:02:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112712.38,
    "close": 112611.03,
    "high": 112712.38,
    "low": 112611.02,
    "volume": 12.68106
  },
  {
    "openTime": "2025-08-03 00:01:00",
    "closeTime": "2025-08-03 00:01:59",
    "mtype": "spot",
    "indicator": "klines",
    "market": "BTCUSDT",
    "frame": "1m",
    "open": 112533.57,
    "close": 112712.37,
    "high": 112722.63,
    "low": 112533.57,
    "volume": 12.39997
  }
]
```
