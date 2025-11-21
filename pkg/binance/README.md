# Current candle

The issue of binance candles is, that there is no current candle available for download from S3 storage as a ZIP file.

There is not this day data at all.

To fulfill this requirement, we need to use API and store candles into  duckdb using `access_mode=READ_WRITE` to 
let read and write access in parallel.

## Duckdb storage

```sql

-- Crate candles table
create
or replace table candles
(
    -- unique identifier
    openTime timestamp primary key,
    --closeTime TIMESTAMP GENERATED ALWAYS AS (openTime + INTERVAL '1 minute' + INTERVAL '-1 microsecond') ,

    open double NOT NULL,
    high double NOT NULL,
    low double NOT NULL,
    close double NOT NULL,

    volume double NOT NULL
);
```

### Insert data

```sql
-- Insert data into duckdb table
INSERT INTO candles (openTime, open, high, low, close, volume)
VALUES
    ('2025-11-18 14:35:00', 67234.5, 67280.0, 67190.0, 67210.3, 145.678);

```
