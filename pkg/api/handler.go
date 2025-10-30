package api

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/go-playground/validator/v10"
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
	testSQL := `SELECT COUNT(*) FROM read_parquet('data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2017-08.parquet')`
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
	From       int64  `validate:"required"`
	To         int64  `validate:"required,gtfield=From"`
	Market     string `validate:"required"`
	Exchange   string `validate:"omitempty,oneof=binance"`
	MarketType string `validate:"omitempty,oneof=spot futures"`
	Frame      string `validate:"omitempty,oneof=1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M"`
}

// handleData handles the /v1/data endpoint for candle data
func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Fetch all query parameters at once
	fromStr := q.Get("from")
	toStr := q.Get("to")
	market := q.Get("market")
	exchange := q.Get("exchange")
	marketType := q.Get("marketType")
	frame := q.Get("frame")

	// Basic presence checks for required numeric fields
	if fromStr == "" || toStr == "" || market == "" {
		http.Error(w, "Missing required parameters: from, to, market", http.StatusBadRequest)
		return
	}

	from, err := strconv.ParseInt(fromStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid 'from' timestamp", http.StatusBadRequest)
		return
	}
	to, err := strconv.ParseInt(toStr, 10, 64)
	if err != nil {
		http.Error(w, "Invalid 'to' timestamp", http.StatusBadRequest)
		return
	}

	// Apply defaults
	if exchange == "" {
		exchange = "binance"
	}
	if marketType == "" {
		marketType = "spot"
	}
	if frame == "" {
		frame = "1m"
	}

	// Build DTO and validate
	dq := DataQuery{
		From:       from,
		To:         to,
		Market:     market,
		Exchange:   exchange,
		MarketType: marketType,
		Frame:      frame,
	}

	if err := s.validate.Struct(dq); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		// Translate validation errors to a simple list
		type verr struct {
			Field string `json:"field"`
			Tag   string `json:"tag"`
			Param string `json:"param"`
		}
		resp := struct {
			Message string `json:"message"`
			Errors  []verr `json:"errors"`
		}{Message: "Validation failed"}
		if ve, ok := err.(validator.ValidationErrors); ok {
			for _, e := range ve {
				resp.Errors = append(resp.Errors, verr{Field: e.Field(), Tag: e.Tag(), Param: e.Param()})
			}
		} else {
			resp.Errors = append(resp.Errors, verr{Field: "", Tag: "error", Param: err.Error()})
		}
		_ = json.NewEncoder(w).Encode(resp)
		return
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
