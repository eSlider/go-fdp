package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-playground/validator/v10"
	"github.com/go-viper/mapstructure/v2"
	"github.com/gorilla/mux"
)

// Server represents the API server
type Server struct {
	db       *sql.DB
	router   *mux.Router
	validate *validator.Validate
}

// NewServer creates a new API server
func NewServer() (*Server, error) {
	// Connect to DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	// Create tables pointing to parquet files
	if err := setupTables(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to setup tables: %w", err)
	}

	s := &Server{
		db:       db,
		router:   mux.NewRouter(),
		validate: validator.New(),
	}

	s.setupRoutes()
	return s, nil
}

// setupTables creates DuckDB tables that point to parquet files
func setupTables(db *sql.DB) error {
	// First test if we can read parquet directly
	testSQL := `SELECT COUNT(*) FROM read_parquet('data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2020-08.parquet')`
	var count int
	if err := db.QueryRow(testSQL).Scan(&count); err != nil {
		log.Printf("Warning: cannot read klines parquet: %v", err)
	} else {
		log.Printf("Successfully read %d records from klines parquet", count)
	}

	// Create table for klines (candles)
	klinesSQL := `
	CREATE OR REPLACE TABLE klines AS
	SELECT *
	FROM read_parquet('data/spot/monthly/klines/**/*.parquet')
	`

	if _, err := db.Exec(klinesSQL); err != nil {
		log.Printf("Warning: failed to create klines table: %v", err)
	} else {
		log.Printf("Successfully created klines table")
	}

	// Create table for agg_trades
	aggTradesSQL := `
	CREATE OR REPLACE TABLE agg_trades AS
	SELECT *
	FROM read_parquet('data/spot/monthly/aggTrades/**/*.parquet')
	`

	if _, err := db.Exec(aggTradesSQL); err != nil {
		log.Printf("Warning: failed to create agg_trades table: %v", err)
	} else {
		log.Printf("Successfully created agg_trades table")
	}

	return nil
}

// setupRoutes configures the API routes
func (s *Server) setupRoutes() {
	s.router.HandleFunc("/v1/data", s.handleData).Methods("GET")
	s.router.HandleFunc("/v1/sql", s.handleSQL).Methods("POST")
}

// DataQuery represents query parameters for /v1/data with validation rules
// All time values are milliseconds since epoch.
type DataQuery struct {
	From       int64  `validate:"required"`              // Millisecond timestamp
	To         int64  `validate:"required,gtfield=From"` // Millisecond timestamp
	Market     string `validate:"required"`
	Exchange   string `validate:"omitempty,oneof=binance"`
	MarketType string `validate:"omitempty,oneof=spot futures"`
	Frame      string `validate:"omitempty,oneof=1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M"`
}

func (dq *DataQuery) FromTime() *time.Time {
	return data.AnyTimestampToTime(dq.From)
}
func (dq *DataQuery) ToTime() *time.Time {
	return data.AnyTimestampToTime(dq.To)
}

type FieldError struct {
	Field string `json:"field"`
	Tag   string `json:"tag"`
	Param string `json:"param"`
}
type ErrorsResponse struct {
	Message string       `json:"message"`
	Errors  []FieldError `json:"errors"`
}

// handleData handles the /v1/data endpoint for candle data
func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {

	dq := &DataQuery{
		Exchange:   "binance",
		MarketType: "spot",
		Frame:      "1m",
	}
	mapstructure.WeakDecode(r.URL.Query(), dq)

	if err := s.Validate(&dq, w); err != nil {
		log.Fatalf("Validation error: %v", err)
		return
	}

	// Basic presence checks for required numeric fields
	if dq.Market == "" {
		http.Error(w, "Missing required parameters: from, to, market", http.StatusBadRequest)
		return
	}

	context := r.Context()
	srv, err := binance.NewHistoryConsumer(context)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	// Loop from start to end by every year and month

	// Map frame and market type from query
	frame := binance.Frame(dq.Frame)
	if frame == "" {
		frame = binance.OneMinute
	}
	var mtype binance.MarketType
	switch dq.MarketType {
	case "", string(binance.Spot):
		mtype = binance.Spot
	case string(binance.Futures):
		mtype = binance.Futures
	default:
		mtype = binance.Spot
	}

	fromTime := dq.FromTime()
	toTime := dq.ToTime()
	start := time.Date(fromTime.Year(), fromTime.Month(), 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(toTime.Year(), toTime.Month(), 1, 0, 0, 0, 0, time.UTC)

	// Collect results
	var infos []*binance.AssetETLInfo
	var errs []error

	for cur := start; !cur.After(end); cur = cur.AddDate(0, 1, 0) {
		asset := &binance.HistoryAsset{
			MarketType: mtype,
			Frequency:  binance.Monthly,
			Frame:      frame,
			Indicator:  binance.Klines,
			Date:       cur,
			Market:     dq.Market,
		}

		// Download and transform (this should create or reuse parquet file)
		infoCh, errCh := srv.DownloadAndTransform(asset)

		// Drain channels for this month
		doneInfo := false
		doneErr := false
		for !(doneInfo && doneErr) {
			select {
			case info, ok := <-infoCh:
				if ok {
					infos = append(infos, info)
				} else {
					doneInfo = true
				}
			case err, ok := <-errCh:
				if ok {
					errs = append(errs, err)
				} else {
					doneErr = true
				}
			}
		}
	}

	// Query klines data
	query := `
	SELECT
		open_time,
		close_time,
		open_price,
		high_price,
		low_price,
		close_price,
		volume,
		quote_volume,
		number_of_trades,
		taker_buy_volume,
		taker_buy_quote
	FROM klines
	WHERE symbol = ?
		AND open_time >= ?
		AND open_time <= ?
	ORDER BY open_time
	`

	log.Printf("Executing query: %s with params: %s, %d, %d", query, dq.Market, dq.From, dq.To)
	rows, err := s.db.Query(query, dq.Market, dq.From, dq.To)
	if err != nil {
		log.Printf("Query error: %v", err)
		http.Error(w, "Database query failed", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Convert to JSON
	var candles []map[string]interface{}
	for rows.Next() {
		var openTime, closeTime, numberOfTrades int64
		var openPrice, highPrice, lowPrice, closePrice, volume, quoteVolume, takerBuyVolume, takerBuyQuote float64

		err := rows.Scan(&openTime, &closeTime, &openPrice, &highPrice, &lowPrice, &closePrice,
			&volume, &quoteVolume, &numberOfTrades, &takerBuyVolume, &takerBuyQuote)
		if err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}

		candle := map[string]interface{}{
			"open_time":        openTime,
			"close_time":       closeTime,
			"open_price":       openPrice,
			"high_price":       highPrice,
			"low_price":        lowPrice,
			"close_price":      closePrice,
			"volume":           volume,
			"quote_volume":     quoteVolume,
			"number_of_trades": numberOfTrades,
			"taker_buy_volume": takerBuyVolume,
			"taker_buy_quote":  takerBuyQuote,
		}
		candles = append(candles, candle)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(candles)
}

// handleSQL handles the /v1/sql endpoint for raw SQL queries
func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("JSON decode error: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	log.Printf("Received SQL query: %s", req.Query)

	if req.Query == "" {
		http.Error(w, "Query is required", http.StatusBadRequest)
		return
	}

	// Execute the query
	rows, err := s.db.Query(req.Query)
	if err != nil {
		log.Printf("SQL query error: %v", err)
		http.Error(w, "Query execution failed", http.StatusBadRequest)
		return
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Printf("Columns error: %v", err)
		http.Error(w, "Failed to get columns", http.StatusInternalServerError)
		return
	}

	// Convert to JSON array
	var results []map[string]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			// Handle different types
			switch v := values[i].(type) {
			case []byte:
				row[col] = string(v)
			case time.Time:
				row[col] = v.UnixMilli()
			default:
				row[col] = v
			}
		}
		results = append(results, row)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

// ServeHTTP implements the http.Handler interface
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.router.ServeHTTP(w, r)
}

// Close closes the database connection
func (s *Server) Close() error {
	return s.db.Close()
}

// Validate and respond with validation errors
func (s *Server) Validate(dq any, w http.ResponseWriter) error {
	if err := s.validate.Struct(dq); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)

		resp := ErrorsResponse{Message: "Validation failed"}
		var ve validator.ValidationErrors
		if errors.As(err, &ve) {
			for _, e := range ve {
				resp.Errors = append(resp.Errors, FieldError{Field: e.Field(), Tag: e.Tag(), Param: e.Param()})
			}
		}
		_ = json.NewEncoder(w).Encode(resp)
		if err != nil {
			return err
		}
	}
	return nil
}
