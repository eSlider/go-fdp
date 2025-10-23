# Finance Data Proxy

## Goals

- External data proxy for finance data (crypto/fiat)
- Caching data by request into parquet files using Hive Partitioning and duckdb

## Binance Data Proxy (prototype)

Binance exposes historical market data via a public S3 bucket (data.binance.vision) as zipped CSV files. Working directly with zipped CSVs is inconvenient for analytics and ad‑hoc queries.

This repository explores a proxy approach: discover, download, decompress, and prepare Binance data for local querying with DuckDB. The current code focuses on listing and fetching files, unzipping in memory, and sketching ingestion into DuckDB.

## Goals

- API to request historical data by SQL queries
- Lazy loading of data by user queries and caching
- Hive partitioning with Parquet files
- Clean database schema structure
- ZSTD compression for Parquet files

Note: These are target goals; not all are implemented yet. See TODOs below.

## Current status (what works now)

- Go CLI prototype with a single entry point (main.go)
- AWS SDK v2 client configured for Binance public S3 (anonymous) in ap-northeast-1
- Utilities to:
  - Construct normalized S3 paths for Binance datasets (Link function)
  - List S3 objects under a prefix
  - Download an object into memory and decompress ZIP to CSV text
- DuckDB database file initialization (data.duckdb) and example SQL scaffolding

## Stack

- Language: Go (modules)
  - go.mod declares: go 1.24.3 (a Go 1.24+ toolchain is required)
- Libraries:
  - AWS SDK for Go v2 (S3 + Downloader)
  - DuckDB Go driver (github.com/duckdb/duckdb-go/v2)
- Storage:
  - Local DuckDB database file: data.duckdb
  - Public S3 bucket: s3://data.binance.vision/

## Project structure

- main.go — entry point; S3 listing, downloading, in‑memory unzip, and DuckDB scaffolding
- normalization_test.go — tests for path normalization (Link)
- go.mod / go.sum — Go modules
- data/ — local working data directory (created/populated by you)
- binance.duckdb, data.duckdb — DuckDB database files (created by the app)

## Requirements

- Go 1.24+ installed (matching or newer than the version in go.mod)
- Network access to AWS S3 (public bucket data.binance.vision)
- No AWS credentials are required for public access; the client is configured for anonymous S3 reads

## Setup

1. Ensure Go is installed and on PATH. Verify:
   - go version
2. Download dependencies:
   - go mod download

## Build and run

- Run directly:
  - go run ./

- Build a binary:
  - go build -o binance-sync
  - ./binance-sync

What the program currently does:
- Creates/opens data.duckdb
- Lists monthly spot klines under the prefix data/spot/monthly/klines/
- For each object (skipping directories and CHECKSUM files):
  - Downloads the ZIP into memory
  - Decompresses the first file from the ZIP to CSV text
  - Prepares to load CSV into DuckDB (ingestion SQL is a work in progress)

Notes:
- The DuckDB ingestion step in main.go is not finalized; it shows an example using read_csv_auto and needs adjustments to pass CSV content correctly to DuckDB in this context.

## Scripts and useful commands

- Dependency management: go mod tidy
- Lint/format: go fmt ./...
- Tests: go test ./...
- Run with race detector (if applicable): go test -race ./...

## Environment variables

- None required for basic read‑only access to data.binance.vision via S3.
- The S3 region/endpoint is hardcoded to ap-northeast-1 for the Binance bucket.
- If you need to customize networking (proxy, etc.), use standard AWS SDK environment variables; however, credentials are not required for this public bucket.

## Tests

- Unit tests:
  - normalization_test.go verifies S3 path normalization (Link)
- Run all tests:
  - go test ./...

## Data and storage

- Local database file: data.duckdb (created automatically)
- Public data source: s3://data.binance.vision/
- Sample path for one symbol/interval/month (built by Link):
  - data/spot/monthly/klines/BTCUSDT/1m/BTCUSDT-1m-2021-01.zip

## TODOs / Roadmap

- Implement a proper ingestion path from CSV text to DuckDB tables
  - Define schemas and create tables
  - Parse CSV columns and types; convert to Parquet with partitioning if desired
- Add a persistent cache layer and eviction policy
- Expose an HTTP API for SQL queries over DuckDB
- Add configuration (flags/env) for market, symbol list, intervals, and date ranges
- Expand tests to cover S3 listing, downloading, and ingestion
- Provide reproducible examples and datasets

## License

- TODO: Add a LICENSE file and specify the project license.
