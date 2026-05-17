# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- REST API for klines, aggregate trades, symbols, and markets
- ETL pipeline from [Binance public data](https://data.binance.vision/) (anonymous S3)
- DuckDB queries over Hive-partitioned Parquet cache
- Live Binance REST client (`pkg/binance/v3`) for current-day candles and aggTrades
- Docker Compose stack with Grafana, Loki, and Promtail
