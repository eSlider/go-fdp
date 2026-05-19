# pkg/binance

Binance spot market data: S3 historical ETL, live hourly parquet, and REST client.

## Layout

| File | Role |
| --- | --- |
| `bulk.go` | S3 list/download, ZIP → CSV → daily parquet |
| `live.go` | Today hourly klines and aggTrades seal/refresh |
| `client.go` | REST (`FetchKlines`, `FetchAggTrades`, …) |
| `source.go` | `etl.BulkLoader` + `etl.LiveSeries` adapter |
| `kline_hourly.go` | Per-hour seal/load/audit helpers |
| `repair.go` | Issue-driven gap repair (delegates to `pkg/integrity`) |

## ETL source

```go
consumer, _ := binance.NewHistoryConsumer(ctx)
src := binance.NewSource(consumer)
// Register with etl.NewRouter as BulkLoader and LiveSeries for etl.SourceBinance
```

## REST

```go
klines, err := binance.FetchKlines(ctx, &binance.KlineRequest{ /* … */ })
```

## Current day

Historical ZIPs exclude today. Live paths write under `data/.../current/*.parquet`; `hourplan.PlanHours` drives seal vs refresh for the open hour.
