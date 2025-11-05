package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"time"

	"github.com/chonla/format"
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
	// // First test if we can read parquet directly
	// testSQL := `SELECT COUNT(*) FROM read_parquet('data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2020-08.parquet')`
	// var count int
	// if err := db.QueryRow(testSQL).Scan(&count); err != nil {
	// 	log.Printf("Warning: cannot read klines parquet: %v", err)
	// } else {
	// 	log.Printf("Successfully read %d records from klines parquet", count)
	// }
	//
	// // Create table for klines (candles)
	// klinesSQL := `
	// CREATE OR REPLACE TABLE klines AS
	// SELECT *
	// FROM read_parquet('data/spot/monthly/klines/**/*.parquet')
	// `
	//
	// if _, err := db.Exec(klinesSQL); err != nil {
	// 	log.Printf("Warning: failed to create klines table: %v", err)
	// } else {
	// 	log.Printf("Successfully created klines table")
	// }
	//
	// // Create table for agg_trades
	// aggTradesSQL := `
	// CREATE OR REPLACE TABLE agg_trades AS
	// SELECT *
	// FROM read_parquet('data/spot/monthly/aggTrades/**/*.parquet')
	// `
	//
	// if _, err := db.Exec(aggTradesSQL); err != nil {
	// 	log.Printf("Warning: failed to create agg_trades table: %v", err)
	// } else {
	// 	log.Printf("Successfully created agg_trades table")
	// }

	return nil
}

// setupRoutes configures the API routes
func (s *Server) setupRoutes() {
	s.router.HandleFunc("/v1/data", s.GetMarketHistory).Methods("GET")
	s.router.HandleFunc("/v1/symbols", s.GetSymbols).Methods("GET")
	s.router.HandleFunc("/v1/markets", s.GetMarkets).Methods("GET")
	s.router.HandleFunc("/v1/sql", s.handleSQL).Methods("POST")
}

// AssetRequest represents query parameters for /v1/data with validation rules
// All time values are milliseconds since epoch.
type AssetRequest struct {
	From       int64  `validate:"required"`              // Millisecond timestamp
	To         int64  `validate:"required,gtfield=From"` // Millisecond timestamp
	Market     string `validate:"required"`
	Exchange   string `validate:"omitempty"`
	MarketType string `validate:"omitempty,oneof=spot futures options"`
	Frame      string `validate:"omitempty,oneof=1s 1m 3m 5m 15m 30m 1h 2h 4h 6h 8h 12h 1d 3d 1w 1M"`
	Indicator  string `validate:"omitempty,oneof=klines aggTrades"`
}

// UnmarshalJSON implements the json.Unmarshaler interface
func (dq *AssetRequest) UnmarshalJSON(b []byte) error {
	type Alias AssetRequest
	var a Alias
	if err := json.Unmarshal(b, &a); err != nil {
		return err
	}
	*dq = AssetRequest(a)
	return nil
}

func (dq *AssetRequest) FromTime() *time.Time {
	return data.AnyTimestampToTime(dq.From)
}
func (dq *AssetRequest) ToTime() *time.Time {
	return data.AnyTimestampToTime(dq.To)
}

func (dq *AssetRequest) MarshalJSON() ([]byte, error) {
	// Marshaling JSON dosntr work with time.Time, so we need to convert to int64

	var m = make(map[string]any)
	m["from"] = dq.From
	m["to"] = dq.To
	m["market"] = dq.Market
	m["exchange"] = dq.Exchange
	m["marketType"] = dq.MarketType
	m["frame"] = dq.Frame
	m["indicator"] = dq.Indicator
	marshal, err := json.Marshal(m)
	return marshal, err
}

func (dq *AssetRequest) GetMarketType() binance.MarketType {
	return binance.NewMarketType(dq.MarketType)
}

func (dq *AssetRequest) GetFrame() binance.Frame {
	return binance.NewFrame(dq.Frame)
}

type FieldError struct {
	Field   string `json:"field,omitempty"`
	Value   any    `json:"-"`
	Tag     string `json:"-"`
	Param   string `json:"-"`
	Message string `json:"message,omitempty"`
	// Err     error  `json:"error,omitempty"`
}

func (f FieldError) Error() string {
	return fmt.Sprintf("%s: %s", f.Field, f.Message)
}

type Error struct {
	Message string       `json:"message"`
	Errors  []FieldError `json:"errors"`
}

func (f Error) Error() string {
	var l []error
	for _, e := range f.Errors {
		l = append(l, e)
	}
	return fmt.Sprintf("%s: %v", f.Message, errors.Join(l...))
}

// GetMarketHistory handles the /v1/data endpoint for candle data
func (s *Server) GetMarketHistory(w http.ResponseWriter, r *http.Request) {
	q := &AssetRequest{
		Exchange:   "binance",
		MarketType: string(binance.Spot),
		Frame:      binance.OneMinute,
		Indicator:  string(binance.Klines),
	}

	if err := s.Validate(q, r); err != nil {
		s.WriteError(w, err, http.StatusBadRequest)
		return
	}

	srv, err := binance.NewHistoryConsumer(r.Context())
	if err != nil {
		s.WriteError(w, err)
		return
	}

	// Loop from start to end by every year and month

	// Map frame and market type from query

	// Collect results
	var infos []*binance.AssetETLInfo
	var errs []FieldError

	// Download and transform (this should create or reuse a parquet file)
	fromTime := *q.FromTime()
	toTime := *q.ToTime()
	for cur := fromTime; !cur.After(toTime); cur = cur.AddDate(0, 0, 1) {
		asset := &binance.HistoryAsset{
			MarketType: q.GetMarketType(),
			Frequency:  binance.Daily,
			Frame:      q.GetFrame(),
			Indicator:  binance.Klines,
			Date:       cur,
			Market:     q.Market,
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
					errs = append(errs, FieldError{
						Message: err.Error(),
					})
				} else {
					doneErr = true
				}
			}
		}
	}
	if len(errs) > 0 {
		s.WriteError(w, Error{
			Message: "Failed to download and transform data",
			Errors:  errs,
		})
		return
	}

	// Query klines data
	result, err := s.CandlesFromParquet(CandleParquetQuery{
		Market:     q.Market,
		Frame:      string(q.GetFrame()),
		Indicator:  q.Indicator,
		MarketType: q.MarketType,
		From:       q.FromTime().UnixMilli(),
		To:         q.ToTime().UnixMilli(),
	})

	if err != nil {
		s.WriteError(w, err)
		return
	}

	s.WriteJson(w, result)
}

// handleSQL handles the /v1/sql endpoint for raw SQL queries
func (s *Server) handleSQL(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Query string `json:"query"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.WriteError(w,
			fmt.Errorf("failed to decode request body: %w", err),
			http.StatusBadRequest)
		return
	}

	log.Printf("Received SQL query: %s", req.Query)
	if req.Query == "" {
		s.WriteError(w, errors.New("query is empty"), http.StatusBadRequest)
		return
	}

	// Execute the query
	results, err := s.QueryMap(req.Query)
	if err != nil {
		s.WriteError(w, err)
		return
	}

	s.WriteJson(w, results)
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
func (s *Server) Validate(dq any, r *http.Request) error {
	values := r.URL.Query()

	// Collect all values
	m := make(map[string]any)
	for k, v := range values {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}

	// Read query parameters from request body
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
			return err
		}
	}

	// Decode query parameters
	if err := mapstructure.WeakDecode(m, dq); err != nil {
		return err
	}

	// Validate
	if err := s.validate.Struct(dq); err != nil {
		return err
	}

	return nil
}

func (s *Server) QueryDatabase(
	query string,
	dq any,
) (result []any, err error) {

	// Validate query parameters
	if err := s.validate.Struct(dq); err != nil {
		return nil, err
	}

	// Format query
	q := format.Sprintf(query, dq)

	// If query has "%<", means not all values are filled, so we need to replace them
	if strings.Contains(q, "%<") {
		return nil, fmt.Errorf("query is not complete: %s", q)
	}

	log.Printf("Executing query: %s", q)

	rows, err := s.db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}
	log.Printf("Columns: %v", columns)

	// Convert to JSON
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		valuePtrs := make([]any, len(columns))
		for i, name := range columns {
			// if string contain time or timestamp or date, then it should be int
			if strings.Contains(name, "time") ||
				strings.Contains(name, "timestamp") ||
				strings.Contains(name, "date") {
				valuePtrs[i] = new(ResponseDate)
			} else if strings.Contains(name, "volume") ||
				strings.Contains(name, "float") ||
				strings.Contains(name, "price") {
				valuePtrs[i] = new(float64)
			} else {
				valuePtrs[i] = new(string)
			}
		}

		// Scan into the slice
		err := rows.Scan(valuePtrs...)
		if err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Create map
		valueMap := make(map[string]any)
		for i, name := range columns {
			valueMap[name] = valuePtrs[i]
		}

		result = append(result, valueMap)
	}
	return result, nil
}

// WriteError writes an error response to the client
func (s *Server) WriteError(w http.ResponseWriter, err error, codeOption ...int) {
	code := http.StatusInternalServerError
	if len(codeOption) > 0 {
		code = codeOption[0]
	}

	w.WriteHeader(code)

	var res *Error
	switch typeErr := err.(type) {
	case validator.ValidationErrors:
		res = &Error{
			Message: "Validation failed",
		}
		for _, e := range typeErr {
			res.Errors = append(res.Errors, FieldError{
				Field:   e.Field(),
				Value:   e.Value(),
				Tag:     e.Tag(),
				Param:   e.Param(),
				Message: e.Error(),
			})
		}

	case FieldError:
		res = &Error{
			Message: "Field error",
			Errors:  []FieldError{typeErr},
		}
	case *FieldError:
		res = &Error{
			Message: "Field error",
			Errors:  []FieldError{*typeErr},
		}
	case *Error:
		res = typeErr
	default:
		res = &Error{
			Message: err.Error(),
		}
	}

	s.WriteJson(w, res)
}

func (s *Server) WriteJson(w http.ResponseWriter, result any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// GetMarkets returns a list of markets for the given exchange
func (s *Server) GetMarkets(w http.ResponseWriter, r *http.Request) {
	registry, err := binance.NewExchangeRegistry()
	if err != nil {
		s.WriteError(w, err)
	}
	s.WriteJson(w, registry.Markets)
}

// GetSymbols returns a list of symbols for the given exchange
func (s *Server) GetSymbols(w http.ResponseWriter, r *http.Request) {
	registry, err := binance.NewExchangeRegistry()
	if err != nil {
		s.WriteError(w, err)
	}
	s.WriteJson(w, registry.Symbols)
}

type CandleParquetQuery struct {
	Market     string `validate:"required"`
	Frame      string `validate:"required"`
	Indicator  string `validate:"required"`
	MarketType string `validate:"required"`
	From       int64  `validate:"required"`
	To         int64  `validate:"required"`
}

// CandlesFromParquet returns candles from parquet files
func (s *Server) CandlesFromParquet(q CandleParquetQuery) (result []any, err error) {

	return s.QueryDatabase(`
		SELECT *
		FROM read_parquet('data/%<MarketType>s/daily/%<Indicator>s/%<Market>s/%<Frame>s/*/*/*.parquet',
hive_partitioning=true)
		WHERE	open_time 	> epoch_ms(%<From>d)::TIMESTAMP
		AND		close_time <= epoch_ms(%<To>d)::TIMESTAMP
		ORDER BY close_time DESC
	`, q)
}

func (s *Server) QueryMap(q string) (any, error) {
	rows, err := s.db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	// Convert to JSON array
	var results []map[string]any
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		row := make(map[string]any, len(columns))
		for i, col := range columns {
			// Handle different types
			switch v := values[i].(type) {
			case []byte:
				row[col] = string(v)
			case *time.Time:
				row[col] = v.UnixMilli()
			case time.Time:
				row[col] = v.UnixMilli()
			default:
				row[col] = v
			}
		}
		results = append(results, row)
	}
	return results, nil
}

type ResponseDate time.Time

// MarshalJSON implements the json.Marshaler interface
func (r ResponseDate) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(r).UnixMilli())
}

type CandleResponse struct {
	OpenTime  ResponseDate `json:"openTime"`
	CloseTime ResponseDate `json:"closeTime"`
	Open      float64      `json:"open"`
	High      float64      `json:"high"`
	Low       float64      `json:"low"`
	Close     float64      `json:"close"`
	Volume    float64      `json:"volume"`
}
