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
	s.router.HandleFunc("/v1/data", s.handleData).Methods("GET")
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

type FieldError struct {
	Field   string `json:"field,omitempty"`
	Value   any    `json:"value,omitempty"`
	Tag     string `json:"tag,omitempty"`
	Param   string `json:"param,omitempty"`
	Message string `json:"message,omitempty"`
	Err     error  `json:"error,omitempty"`
}

func (f FieldError) Error() string {
	return fmt.Sprintf("%s: %s: %v", f.Field, f.Message, f.Err)
}

type Error struct {
	Message string  `json:"message"`
	Errors  []error `json:"errors"`
}

func (f Error) Error() string {
	return fmt.Sprintf("%s: %v", f.Message, errors.Join(f.Errors...))
}

// handleData handles the /v1/data endpoint for candle data
func (s *Server) handleData(w http.ResponseWriter, r *http.Request) {
	q := &AssetRequest{
		Exchange:   "binance",
		MarketType: string(binance.Spot),
		Frame:      binance.OneMinute,
		Indicator:  string(binance.Klines),
	}

	if err := s.Validate(q, w, r); err != nil {
		s.WriteError(w, err, http.StatusBadRequest)
		return
	}

	// Basic presence checks for required numeric fields
	if q.Market == "" {
		s.WriteError(w, Error{"Missing required field", []error{FieldError{Field: "market"}}}, http.StatusBadRequest)
		return
	}

	context := r.Context()
	srv, err := binance.NewHistoryConsumer(context)
	if err != nil {
		s.WriteError(w, err)
		return
	}

	// Loop from start to end by every year and month

	// Map frame and market type from query
	frame := binance.Frame(q.Frame)
	if frame == "" {
		frame = binance.OneMinute
	}
	var mtype binance.MarketType
	switch q.MarketType {
	case "", string(binance.Spot):
		mtype = binance.Spot
	case string(binance.Futures):
		mtype = binance.Futures
	default:
		mtype = binance.Spot
	}

	fromTime := q.FromTime()
	toTime := q.ToTime()
	// start := time.Date(fromTime.Year(), fromTime.Month(), fromTime.Day(), 0, 0, 0, 0, time.UTC)
	// end := time.Date(toTime.Year(), toTime.Month(), toTime.Day(), 0, 0, 0, 0, time.UTC)
	start := *fromTime
	end := *toTime

	// Collect results
	var infos []*binance.AssetETLInfo
	var errs []error

	for cur := start; !cur.After(end); cur = cur.AddDate(0, 0, 1) {
		asset := &binance.HistoryAsset{
			MarketType: mtype,
			Frequency:  binance.Daily,
			Frame:      frame,
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
					errs = append(errs, err)
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
	result, err := s.DbQuery(`SELECT
	    *
	FROM
		read_parquet(
			'data/%<mtype>s/daily/%<indicator>s/%<market>s/%<frame>s/*/*/*.parquet',
			hive_partitioning = true
		)
	where
		open_time > epoch_ms(%<from>d)::TIMESTAMP
	AND
		close_time <= epoch_ms(%<to>d)::TIMESTAMP
	order by
		close_time desc
	`, map[string]any{
		"market":    q.Market,
		"frame":     frame,
		"indicator": q.Indicator,
		"mtype":     q.MarketType,
		"from":      fromTime.UnixMilli(),
		"to":        toTime.UnixMilli(),
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
	rows, err := s.db.Query(req.Query)
	if err != nil {
		s.WriteError(w,
			fmt.Errorf("failed to execute query: %w", err),
			http.StatusBadRequest)
		return
	}
	defer rows.Close()

	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		s.WriteError(w,
			fmt.Errorf("failed to get columns: %w", err),
			http.StatusInternalServerError)
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
			s.WriteError(w, fmt.Errorf("failed to scan row: %w", err), http.StatusInternalServerError)
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
func (s *Server) Validate(dq any, w http.ResponseWriter, r *http.Request) error {
	values := r.URL.Query()

	// Collect all values
	m := make(map[string]any)
	for k, v := range values {
		if len(v) > 0 {
			m[k] = v[0]
		}
	}

	// Decode query parameters
	if err := mapstructure.WeakDecode(m, dq); err != nil {
		return err
	}

	// Validate
	if err := s.validate.Struct(dq); err != nil {
		// Return validation errors
		resp := Error{Message: "Validation failed"}
		var ve validator.ValidationErrors
		if errors.As(err, &ve) {
			for _, e := range ve {
				resp.Errors = append(resp.Errors, FieldError{
					Field:   e.Field(),
					Value:   e.Value(),
					Tag:     e.Tag(),
					Param:   e.Param(),
					Message: e.Error(),
				})
			}
		}
		return err
	}

	return nil
}

func (s *Server) DbQuery(
	query string,
	dq any,
) (result []any, err error) {
	q := format.Sprintf(query, dq)
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

		// v := new(CandleResponse)
		// err = mapstructure.WeakDecode(valuePtrs, v)
		// if err != nil {
		// 	return nil, fmt.Errorf("failed to decode row: %w", err)
		// }

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
	switch err.(type) {
	case *FieldError:
		res = &Error{
			Message: "Field error",
			Errors:  []error{err},
		}
	case *Error:
		errors.As(err, &res)
	default:
		res = &Error{
			Message: err.Error(),
			Errors:  []error{err},
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

type ResponseDate time.Time

// MarshalJSON implements the json.Marshaler interface
func (r ResponseDate) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(r).UnixMilli())
}

type CandleResponse struct {
	OpenTime  time.Time `json:"openTime"`
	CloseTime time.Time `json:"closeTime"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
}
