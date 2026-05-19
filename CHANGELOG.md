# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Package `doc.go` files and README links for [pkg.go.dev](https://pkg.go.dev/github.com/eslider/go-fdp)

### Changed

- Renamed module and GitHub repository from `go-binance-fdp` to `go-fdp` (`github.com/eslider/go-fdp`) to reflect multi-source ETL

## [0.2.0] - 2026-05-19

### Added

- REST API for klines, aggregate trades, symbols, and markets
- ETL pipeline from [Binance public data](https://data.binance.vision/) (anonymous S3)
- DuckDB queries over Hive-partitioned Parquet cache
- `pkg/etl` router with multi-source stubs (`bitfinex`, `polymarket`)
- `pkg/gapfill` lazy gap repair on API read (count-first audit via `pkg/integrity`)
- `pkg/integrity/run` shared audit runner for `cmd/audit`
- `cmd/fdp` HTTP entrypoint; `cmd/audit` integrity CLI

### Changed

- Merged `pkg/binance/v3` into `pkg/binance` (REST client, `FetchKlines`, `KlineSeries`)
- Dissolved `internal/domain`, `internal/service`, `internal/repository` into `internal/market`, `internal/store`, `internal/query`
- Docker and CI build `./cmd/fdp` instead of root `main.go`
- Split `HistoryConsumer` into `bulk.go` and `live.go`

### Removed

- Root `main.go` (use `go run ./cmd/fdp`)
- `pkg/binance/v3` subpackage

### Added (earlier)

- Docker Compose stack with Grafana, Loki, and Promtail
