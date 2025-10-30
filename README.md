# Finance Data Proxy


This is a Go-based finance data proxy for crypto/fiat data, using DuckDB for caching and Parquet files with Hive
partitioning. The project syncs Binance historical market data from S3.

## Goals

- External data proxy for finance data (crypto/fiat)
- Caching data by request into parquet files using Hive Partitioning and duckdb/posgresql

## Features

- API to request historical data by SQL queries
- Lazy loading of data by user queries and caching
- Hive partitioning with Parquet files
- Clean database schema structure
- ZSTD compression for Parquet files


### Binance Data Proxy (prototype)

Binance exposes historical market data via a public S3 bucket (data.binance.vision) as zipped CSV files. Working directly with zipped CSVs is inconvenient for analytics and ad‑hoc queries.

This repository explores a proxy approach: discover, download, decompress, and prepare Binance data for local querying with DuckDB. The current code focuses on listing and fetching files, unzipping in memory, and sketching ingestion into DuckDB.




> Note: These are target goals; not all are implemented yet. See TODOs below.

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
- go.mod / go.sum — Go modules
- data/ — local working data directory (created/populated by you)

## Requirements

- Go 1.24+
- Network access to AWS S3 (data.binance.vision)
- Build tag: `no_duckdb_arrow` (required to disable Apache Arrow dependencies)

## Installation

```bash
go mod download
```

## Usage

Run directly:
```bash
go run -tags no_duckdb_arrow ./
```

Build binary:
```bash
go build -tags no_duckdb_arrow -o binance-sync
./binance-sync
```

## Current Features

- S3 client for Binance public bucket (anonymous access)
- List and download monthly spot klines
- In-memory ZIP decompression to CSV
- DuckDB database initialization and scaffolding

## Architecture

- **Language**: Go 1.24+ with modules
- **Libraries**:
  - AWS SDK v2 (S3 operations)
  - DuckDB Go driver
- **Storage**:
  - Local: data.duckdb
  - Remote: s3://data.binance.vision/

## Project Structure

```
├── main.go                    # Entry point
├── main_test.go              # Integration tests
├── pkg/
│   ├── binance/              # Binance-specific logic
│   │   ├── model.go          # Data models
│   │   ├── service.go        # S3 operations
│   │   └── normalization_test.go
│   ├── data/                 # Data processing
│   │   ├── reader.go         # CSV reader
│   │   ├── buffer.go         # Data buffering
│   │   └── decompressor.go   # ZIP handling
│   └── fs/                   # File system operations
│       ├── parquet.go        # Parquet processing
│       ├── parquet_test.go   # Parquet tests
│       ├── file.go           # File utilities
│       └── zip.go            # ZIP utilities
├── go.mod/go.sum             # Dependencies
└── data/                     # Working directory
```

## Development

### Commands
```bash
go mod tidy      # Clean dependencies
go fmt ./...     # Format code
go test -tags no_duckdb_arrow ./...    # Run tests
go test -tags no_duckdb_arrow -race ./...  # Run tests with race detection
```

### Testing
- Unit tests: `go test -tags no_duckdb_arrow ./...`
- Integration tests: `go test -tags no_duckdb_arrow,integration ./...`

## Roadmap

- [ ] Implement CSV to DuckDB ingestion pipeline
- [ ] Add Parquet conversion with Hive partitioning
- [ ] Implement persistent caching layer
- [ ] Add HTTP API for SQL queries
- [ ] Configuration system (flags/env vars)
- [ ] Extended test coverage
- [ ] Documentation and examples
