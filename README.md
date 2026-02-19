# Finance Data Proxy

This is a Go-based finance data proxy for crypto/fiat data, using DuckDB for caching and Parquet files with Hive partitioning. The project syncs Binance historical market data from S3.

## Architecture

The project follows a **Three-tier architecture** with **Domain-Driven Design (DDD)** principles:

- **Presentation Layer (`internal/handler`)**: HTTP handlers exposing REST API endpoints.
- **Service Layer (`internal/service`)**: Orchestrates business logic, such as ensuring data availability (ETL) and coordinating with the repository.
- **Repository Layer (`internal/repository`)**: Handles data access using DuckDB and Parquet files.
- **Domain Layer (`internal/domain`)**: Defines core entities (`Candle`, `Trade`) and interfaces.
- **Infrastructure (`pkg/binance`, `pkg/data`)**: External integrations (Binance S3) and low-level data utilities.

## Features

- **Historical Data**: Request historical candles (klines) for any range.
- **ETL on Demand**: Automatically downloads and transforms data from Binance S3 if missing.
- **Caching**: Stores data in Parquet files with Hive partitioning for efficient querying.
- **Current Day Data**: Caches current day's data in hourly Parquet files to provide up-to-date information.
- **API**: RESTful API for easy integration.

## Project Structure

```
├── cmd/
│   └── server/               # (Optional) Alternative entry point
├── internal/
│   ├── domain/               # Domain entities and interfaces
│   ├── service/              # Business logic
│   ├── repository/           # Data access (DuckDB/Parquet)
│   └── handler/              # HTTP handlers
├── pkg/
│   ├── binance/              # Binance S3 client and ETL logic
│   ├── data/                 # Shared data utilities (Parquet, CSV, Time)
│   └── fs/                   # File system helpers
├── main.go                   # Application entry point
├── go.mod/go.sum             # Dependencies
└── data/                     # Data storage (Parquet files)
```

## Requirements

- Go 1.24+
- Network access to AWS S3 (data.binance.vision)
- Build tag: `no_duckdb_arrow` (required to disable Apache Arrow dependencies)

## Installation

```bash
go mod download
```

## Usage

Run the server:
```bash
go run -tags no_duckdb_arrow main.go
```

The server listens on port 8082 by default.

### API Endpoints

- **Get Historical Data**:
  ```
  GET /v1/data?from={ms}&to={ms}&market={symbol}&exchange=binance&marketType=spot&frame=1m&indicator=klines
  ```

- **Get Markets**:
  ```
  GET /v1/markets
  ```

- **Get Symbols**:
  ```
  GET /v1/symbols
  ```

## Development

### Commands
```bash
go mod tidy      # Clean dependencies
go fmt ./...     # Format code
go test -tags no_duckdb_arrow ./...    # Run tests
```

`
## TODO:

* Analyze OrderBook indicators project:
  * https://github.dev/empenoso/MOEX-OrderBook-DeepZoom
  * https://habr.com/ru/articles/975106/
  * HQ data: https://www.youtube.com/watch?v=qFEPJJ7uUUo&t=3006s
* ATAS | VDS Manager
  * Analyze :
    * Virtualization/KASM:
    * https://git.markets-platform.com/TradePlatform/managment-task-report/issues/233
    * https://docs.google.com/spreadsheets/d/12Z0IAXDVcBBeepIqwkT8xkjvdlf7IYcHCsBZUEo3YA8/edit?gid=0#gid=0
* Agent-Builder
