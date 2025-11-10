package binance

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync-v3/pkg/data"
	"time"
)

const (
	defaultBaseURL = "https://api.binance.com"
	defaultDataURL = "https://data.binance.vision/data"
)

type JobStatus struct {
	Symbol string `json:"symbol"`
	Year   int    `json:"year"`
	Month  int    `json:"month"`
	Status string `json:"status"` // e.g., "downloading", "extracting", "transforming", "completed", "failed"
	Error  string `json:"error,omitempty"`
}

// JobStatusChannel is used to send job status updates.
var JobStatusChannel = make(chan JobStatus, 100)

func GetDownloadURL(tradingType, dataType, interval, symbol string, year, month int) string {
	var filename string
	var path string

	// Handle different trading types
	var basePath string
	if tradingType == "futures" {
		// Futures data uses "um" (USD-M) path
		basePath = "um"
	} else {
		basePath = tradingType
	}

	if interval == "monthly" {
		filename = fmt.Sprintf("%s-%s-%d-%02d.zip", symbol, dataType, year, month)
		path = fmt.Sprintf("%s/%s/monthly/%s/%s", defaultDataURL, basePath, dataType, symbol)
	} else if dataType == "klines" {
		filename = fmt.Sprintf("%s-%s-%d-%02d-01.zip", symbol, interval, year, month)
		path = fmt.Sprintf("%s/%s/daily/%s/%s/%s", defaultDataURL, basePath, dataType, symbol, interval)
	} else {
		// Default to daily for other data types if not monthly or klines
		filename = fmt.Sprintf("%s-%s-%d-%02d-01.zip", symbol, dataType, year, month)
		path = fmt.Sprintf("%s/%s/daily/%s/%s", defaultDataURL, basePath, dataType, symbol)
	}

	return fmt.Sprintf("%s/%s", path, filename)
}

// DownloadData downloads any type of data for a given symbol and month.
func DownloadData(tradingType, dataType, interval, symbol string, year, month int, destinationFolder string) error {
	job := JobStatus{Symbol: symbol, Year: year, Month: month}

	// 1. Download the zip file
	job.Status = "downloading"
	JobStatusChannel <- job

	zipURL := GetDownloadURL(tradingType, dataType, interval, symbol, year, month)
	zipBuf, err := data.DownloadFile(zipURL)
	if err != nil {
		job.Status = "failed"
		job.Error = err.Error()
		JobStatusChannel <- job
		return fmt.Errorf("failed to download %s zip: %w", dataType, err)
	}

	// 2. Extract the zip file
	job.Status = "extracting"
	csvBuf, err := zipBuf.Decompress()
	if err != nil {
		job.Status = "failed"
		job.Error = err.Error()
		JobStatusChannel <- job
		return fmt.Errorf("failed to extract %s zip: %w", dataType, err)
	}

	// 3. Transform CSV to Parquet
	job.Status = "transforming"
	JobStatusChannel <- job

	// Set up the parquet file path first
	var parquetFilePath string
	parquetFileName := fmt.Sprintf("%s-%s-%d-%02d.parquet", symbol, dataType, year, month)
	if dataType == "klines" && interval != "monthly" {
		parquetFileName = fmt.Sprintf("%s-%s-%d-%02d.parquet", symbol, interval, year, month)
		parquetFilePath = filepath.Join(destinationFolder, tradingType, dataType, symbol, interval, parquetFileName)
	} else {
		parquetFilePath = filepath.Join(destinationFolder, tradingType, dataType, symbol, parquetFileName)
	}

	// Ensure destination directory exists for parquet
	err = os.MkdirAll(filepath.Dir(parquetFilePath), os.ModePerm)
	if err != nil {
		job.Status = "failed"
		job.Error = err.Error()
		JobStatusChannel <- job
		return fmt.Errorf("failed to create destination directory for parquet: %w", err)
	}

	go func(parquetFilePath string, job JobStatus) {
		var err error
		switch dataType {
		case "aggTrades":
			err = TransformAggTradesToParquet(csvBuf, parquetFilePath)
		case "trades":
			err = TransformTradesToParquet(csvBuf, parquetFilePath)
		case "klines":
			err = TransformKlinesToParquet(csvBuf, parquetFilePath, interval)
		default:
			err = fmt.Errorf("unsupported data type: %s", dataType)
		}

		if err != nil {
			job.Status = "failed"
			job.Error = err.Error()
			fmt.Printf("[ASYNC] Transformation failed for %s-%d-%02d: %v\n", job.Symbol, job.Year, job.Month, err)
			JobStatusChannel <- job
			return
		}

		// Validation step
		db, err := sql.Open("duckdb", "")
		if err != nil {
			job.Status = "failed"
			job.Error = "DuckDB open error during validation: " + err.Error()
			fmt.Printf("[ASYNC] Validation failed for %s-%d-%02d: %v\n", job.Symbol, job.Year, job.Month, err)
			JobStatusChannel <- job
			return
		}
		defer db.Close()

		row := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, parquetFilePath))
		var count int
		err = row.Scan(&count)
		if err != nil || count == 0 {
			job.Status = "failed"
			job.Error = "Validation failed: row count is zero or unavailable"
			fmt.Printf("[ASYNC] Validation failed for %s-%d-%02d: row count is zero or unavailable\n", job.Symbol, job.Year, job.Month)
			JobStatusChannel <- job
			return
		}

		job.Status = "completed"
		fmt.Printf("[ASYNC] Transformation and validation completed for %s-%d-%02d (rows: %d)\n", job.Symbol, job.Year, job.Month, count)
		JobStatusChannel <- job

	}(parquetFilePath, job)

	return nil
}

// DownloadAggTrades downloads aggregated trade data for a given symbol and month.
func DownloadAggTrades(tradingType, symbol string, year, month int, destinationFolder string) error {
	return DownloadData(tradingType, "aggTrades", "monthly", symbol, year, month, destinationFolder)
}

// TransformAggTradesToParquet transforms aggTrades CSV to Parquet format.
func TransformAggTradesToParquet(csvBuffer *data.Buffer, parquetFilePath string) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	fmt.Printf("Transforming aggTrades CSV to Parquet...\n  Input: %s\n  Output: %s\n", csvBuffer, parquetFilePath)

	query := fmt.Sprintf(`
		COPY (
			SELECT
			CAST(column0 AS BIGINT) AS agg_trade_id,
			CAST(column1 AS DOUBLE) AS price,
			CAST(column2 AS DOUBLE) AS quantity,
			CAST(column3 AS BIGINT) AS first_trade_id,
			CAST(column4 AS BIGINT) AS last_trade_id,
			CAST(column5 AS BIGINT) AS timestamp,
			CAST(column6 AS BOOLEAN) AS is_buyer_maker,
			CAST(column7 AS BOOLEAN) AS is_best_price_match
			FROM read_csv_auto('%s', HEADER=FALSE)
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)
	`, csvBuffer, parquetFilePath)

	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to transform aggTrades CSV to Parquet: %w", err)
	}

	row := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, parquetFilePath))
	var count int
	err = row.Scan(&count)
	if err == nil {
		fmt.Printf("Transformation complete. Rows written: %d\n", count)
	} else {
		fmt.Printf("Transformation complete. (Row count unavailable: %v)\n", err)
	}
	return nil
}

// TransformTradesToParquet transforms trades CSV to Parquet format.
func TransformTradesToParquet(csvBuffer *data.Buffer, parquetFilePath string) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf(`
		COPY (
			SELECT
			CAST(column0 AS BIGINT) AS trade_id,
			CAST(column1 AS DOUBLE) AS price,
			CAST(column2 AS DOUBLE) AS quantity,
			CAST(column3 AS BIGINT) AS quote_quantity,
			CAST(column4 AS BIGINT) AS timestamp,
			CAST(column5 AS BOOLEAN) AS is_buyer_maker,
			CAST(column6 AS BOOLEAN) AS is_best_match
			FROM read_csv_auto('%s', HEADER=FALSE)
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)
	`, csvBuffer, parquetFilePath)

	if _, err = db.Exec(query); err != nil {
		return fmt.Errorf("failed to transform trades CSV to Parquet: %w", err)
	}

	var count int
	if err = db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, parquetFilePath)).Scan(&count); err != nil {
		return fmt.Errorf("Transformation complete. (Row count unavailable: %v)\n", err)
	}

	fmt.Printf("Transformation complete. Rows written: %d\n", count)
	return nil
}

// TransformKlinesToParquet transforms klines CSV to Parquet format.
func TransformKlinesToParquet(csvFilePath *data.Buffer, parquetFilePath, interval string) error {
	start := time.Now()
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	fmt.Printf("Transforming klines CSV to Parquet...\n  Input: %s\n  Output: %s\n", csvFilePath, parquetFilePath)

	query := fmt.Sprintf(`
		COPY (
			SELECT
			CAST(column0 AS BIGINT) AS open_time,
			CAST(column1 AS DOUBLE) AS open,
			CAST(column2 AS DOUBLE) AS high,
			CAST(column3 AS DOUBLE) AS low,
			CAST(column4 AS DOUBLE) AS close,
			CAST(column5 AS DOUBLE) AS volume,
			CAST(column6 AS BIGINT) AS close_time,
			CAST(column7 AS DOUBLE) AS quote_asset_volume,
			CAST(column8 AS INTEGER) AS number_of_trades,
			CAST(column9 AS DOUBLE) AS taker_buy_base_asset_volume,
			CAST(column10 AS DOUBLE) AS taker_buy_quote_asset_volume
			FROM read_csv_auto('%s', HEADER=FALSE)
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)
	`, csvFilePath, parquetFilePath)

	fmt.Printf("[Transform] Started at %s\n", start.Format(time.RFC3339))
	_, err = db.Exec(query)
	if err != nil {
		return fmt.Errorf("failed to transform klines CSV to Parquet: %w", err)
	}
	elapsed := time.Since(start)

	row := db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, parquetFilePath))
	var count int
	err = row.Scan(&count)
	if err == nil {
		fmt.Printf("Transformation complete. Rows written: %d\n", count)
	} else {
		fmt.Printf("Transformation complete. (Row count unavailable: %v)\n", err)
	}
	fmt.Printf("[Transform] Finished in %s\n", elapsed)
	return nil
}

// GetAllSymbols retrieves all trading symbols from the Binance API.
func GetAllSymbols(tradingType string) ([]string, error) {
	var exchangeInfoURL string
	if tradingType == "spot" {
		exchangeInfoURL = fmt.Sprintf("%s/api/v3/exchangeInfo", defaultBaseURL)
	} else if tradingType == "futures" {
		exchangeInfoURL = fmt.Sprintf("%s/fapi/v1/exchangeInfo", defaultBaseURL)
	} else {
		return nil, fmt.Errorf("unsupported trading type: %s", tradingType)
	}

	resp, err := http.Get(exchangeInfoURL)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch exchange info: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("bad status when fetching exchange info: %s", resp.Status)
	}

	var data struct {
		Symbols []struct {
			Symbol string `json:"symbol"`
		} `json:"symbols"`
	}

	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode exchange info: %w", err)
	}

	var symbols []string
	for _, s := range data.Symbols {
		symbols = append(symbols, s.Symbol)
	}

	return symbols, nil
}

// RunDownloader is the main entry point for the Binance downloader.
func RunDownloader() {
	fs := flag.NewFlagSet("binance-downloader", flag.ExitOnError)

	tradingType := fs.String("type", "spot", "Trading type (spot or futures)")
	symbol := fs.String("symbol", "", "Symbol to download (e.g., BTCUSDT)")
	startYear := fs.Int("startYear", 0, "Start year for download")
	startMonth := fs.Int("startMonth", 0, "Start month for download")
	endYear := fs.Int("endYear", 0, "End year for download")
	endMonth := fs.Int("endMonth", 0, "End month for download")
	folder := fs.String("folder", ".", "Destination folder for downloads")
	fs.Parse(os.Args[1:])

	if *startYear == 0 || *startMonth == 0 || *endYear == 0 || *endMonth == 0 {
		fmt.Println("Usage: go run main.go --type <spot/futures> --startYear <YEAR> --startMonth <MONTH> --endYear <YEAR> --endMonth <MONTH> [--symbol <SYMBOL>] --folder <FOLDER>")
		return
	}

	var symbolsToDownload []string
	if *symbol != "" {
		symbolsToDownload = []string{*symbol}
	} else {
		// If no symbol is provided, get all symbols
		var err error
		symbolsToDownload, err = GetAllSymbols(*tradingType)
		if err != nil {
			fmt.Printf("Error getting all symbols: %v\n", err)
			return
		}
	}

	for currentYear := *startYear; currentYear <= *endYear; currentYear++ {
		startM := 1
		endM := 12
		if currentYear == *startYear {
			startM = *startMonth
		}
		if currentYear == *endYear {
			endM = *endMonth
		}

		for currentMonth := startM; currentMonth <= endM; currentMonth++ {
			for _, s := range symbolsToDownload {
				err := DownloadAggTrades(*tradingType, s, currentYear, currentMonth, *folder)
				if err != nil {
					fmt.Printf("Error downloading aggTrades for %s-%d-%02d: %v\n", s, currentYear, currentMonth, err)
				}
			}
		}
	}
}
