package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"

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

func (dq *AssetRequest) IsToday() bool {
	utc := dq.ToTime().UTC()
	if utc.IsZero() {
		return false
	}
	return data.IsToday(utc)
}

func (dq AssetRequest) MarshalJson() ([]byte, error) {
	// Marshaling JSON dosntr work with time.Time, so we need to convert to int64

	marshal, err := json.Marshal(dq)
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
		Frame:      binance.Minute.String(),
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

	var result []*CandleResponse
	var wg sync.WaitGroup
	start := time.Now()

	// Loop between dates - download/transform but don't fail on errors
	for cur := fromTime; !cur.After(toTime); cur = cur.AddDate(0, 0, 1) {
		asset := &binance.HistoryAsset{
			MarketType: q.GetMarketType(),
			Frequency:  binance.Daily,
			Frame:      q.GetFrame(),
			Indicator:  binance.Klines,
			Date:       cur,
			Market:     q.Market,
		}

		wg.Add(1)
		go func(asset *binance.HistoryAsset) {
			defer wg.Done()
			// Download and transform (this should create or reuse a parquet file)

			startDownloadTime := time.Now()
			infoCh, errCh := srv.DownloadAndTransform(asset)
			slog.Info("Download and transform completed", "date", asset.Date.Format("2006-01-02"), "elapsed", time.Since(startDownloadTime))
			for done := false; !done; {
				select {
				case info, ok := <-infoCh:
					if ok {
						infos = append(infos, info)
						fmt.Printf("Downloaded %s\n", cur.Format("2006-01-02"))
						// fmt.Printf("Path:%s", 9info.Path)
					} else {
						done = true
					}
				case err, ok := <-errCh:
					if ok {
						errs = append(errs, FieldError{
							Message: fmt.Sprintf("Date %s: %s", cur.Format("2006-01-02"), err.Error()),
						})
					} else {
						done = true
					}
				}
			}
		}(asset)
	}
	wg.Wait()
	slog.Info("ETL and caching completed", "elapsed", time.Since(start))

	// Query klines data - try to get as much data as possible even if some downloads failed

	// Check if the query includes today's date
	// midnightToday := time.Now().UTC().Truncate(24 * time.Hour)
	// isQueryingToday := q.FromTime().Before(midnightToday) && q.ToTime().After(midnightToday)
	asset := &binance.HistoryAsset{
		MarketType: q.GetMarketType(),
		Frequency:  binance.Daily,
		Frame:      q.GetFrame(),
		Indicator:  binance.Klines,
		Date:       *q.FromTime(),
		Market:     q.Market,
	}

	if asset.IsToday() {
		// Query today's data from hourly parquet files
		// Use absolute path for DuckDB queries (relative paths break with -trimpath builds)
		absDataPath, _ := filepath.Abs(fs.GetModuleRelativePath("data"))
		todayResult, err := s.CandlesFromHourlyParquet(CandleParquetQuery{
			Market:     q.Market,
			Frame:      string(q.GetFrame().String()),
			Indicator:  q.Indicator,
			MarketType: q.MarketType,
			From:       q.FromTime().UnixMilli(),
			To:         q.ToTime().UnixMilli(),
			DataPath:   absDataPath,
		})

		if err != nil {
			slog.Error("Failed to query today's hourly data", "error", err)
			errs = append(errs, FieldError{
				Message: fmt.Sprintf("Failed to query today's hourly data: %v", err),
			})
		} else {
			result = append(result, todayResult...)
		}
	}

	// Query historical data from parquet files
	historicalResult, err := s.CandlesFromParquet(CandleParquetQuery{
		Market:     q.Market,
		Frame:      string(q.GetFrame().String()),
		Indicator:  q.Indicator,
		MarketType: q.MarketType,
		From:       q.FromTime().UnixMilli(),
		To:         q.ToTime().UnixMilli(),
	})

	if err != nil {
		slog.Error("Failed to query historical data", "error", err)
		errs = append(errs, FieldError{
			Message: fmt.Sprintf("Failed to query historical data: %v", err),
		})
	} else {
		result = append(result, historicalResult...)
	}

	// If we have some data, return it even if there were some errors
	if len(result) > 0 {
		if len(errs) > 0 {
			slog.Warn("Partial data returned", "errorCount", len(errs), "errors", errs)
		}
		s.WriteJson(w, result)
		return
	}

	// No data at all, return the errors
	if len(errs) > 0 {
		s.WriteError(w, Error{
			Message: "Failed to retrieve any data",
			Errors:  errs,
		})
		return
	}

	// Should not reach here, but just in case
	s.WriteJson(w, []*CandleResponse{})
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

	slog.Info("Received SQL query", "query", req.Query)
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
			// If EOF is returned, it means the request body is empty
			if err != io.EOF {
				return err
			}
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
	DataPath   string // Path to data directory
}

type CandleDuckDBQuery struct {
	Market     string `validate:"required"`
	Frame      string `validate:"required"`
	Indicator  string `validate:"required"`
	MarketType string `validate:"required"`
	From       int64  `validate:"required"`
	To         int64  `validate:"required"`
	DuckDBPath string // Path to DuckDB file
}

// CandlesFromParquet returns candles from parquet files
func (s *Server) CandlesFromParquet(q CandleParquetQuery) (result []*CandleResponse, err error) {
	// Use absolute path for DuckDB queries (relative paths break with -trimpath builds)
	q.DataPath, _ = filepath.Abs(fs.GetModuleRelativePath("data"))

	// Query historical data (excludes current/ folder which has different schema)
	resCh, errCh := data.QueryParquets(s.db, `
		SELECT
			make_timestamp(year::BIGINT, month::BIGINT, day::BIGINT,
				date_part('hour', open_time)::BIGINT,
				date_part('minute', open_time)::BIGINT,
				date_part('second', open_time)) as openTime,
			openTime + interval '1' minute - interval '1' millisecond AS closeTime,

			open_price as open,
			close_price as close,
			high_price as high,
			low_price as low,

			volume as volume

		FROM read_parquet('%<DataPath>s/*/*/*/*/*/*/*/data.parquet')

		WHERE mtype = '%<MarketType>s'
			AND indicator = '%<Indicator>s'
			AND market = '%<Market>s'
			AND frame = '%<Frame>s'
			AND openTime BETWEEN epoch_ms(%<From>d) AND epoch_ms(%<To>d)
		ORDER BY
			openTime DESC
	`, q)

	for {
		select {
		case err, ok := <-errCh:
			if ok {
				return nil, err
			}
			return result, nil
		case entry, ok := <-resCh:
			if !ok {
				return
			}
			instance := new(CandleResponse)
			if err := mapstructure.WeakDecode(entry, instance); err != nil {
				return nil, err
			}
			result = append(result, instance)
		}
	}
}

// CandlesFromHourlyParquet returns candles from hourly parquet files (current day)
func (s *Server) CandlesFromHourlyParquet(q CandleParquetQuery) (result []*CandleResponse, err error) {
	// Construct path to hourly parquets: data/mtype=X/indicator=X/market=X/frame=X/year=X/month=X/day=X/current/*.parquet
	now := time.Now().UTC()
	hourlyPath := fmt.Sprintf("%s/mtype=%s/indicator=%s/market=%s/frame=%s/year=%d/month=%d/day=%d/current/*.parquet",
		q.DataPath, q.MarketType, q.Indicator, q.Market, q.Frame,
		now.Year(), int(now.Month()), now.Day())

	// Check if any hourly files exist
	if !fs.FileExists(filepath.Dir(hourlyPath)) {
		return []*CandleResponse{}, nil
	}

	// Query hourly parquet files (atomic writes prevent reading incomplete files)
	resCh, errCh := data.QueryParquets(s.db, `
		SELECT
			epoch_ms(open_time) as openTime,
			epoch_ms(close_time) as closeTime,
			open_price as open,
			close_price as close,
			high_price as high,
			low_price as low,
			volume as volume
		FROM read_parquet('%<HourlyPath>s')
		WHERE open_time BETWEEN %<From>d AND %<To>d
		ORDER BY openTime DESC
	`, struct {
		HourlyPath string
		From       int64
		To         int64
	}{hourlyPath, q.From, q.To})

	for {
		select {
		case err, ok := <-errCh:
			if ok {
				return nil, err
			}
			return result, nil
		case entry, ok := <-resCh:
			if !ok {
				return
			}
			instance := new(CandleResponse)
			if err := mapstructure.WeakDecode(entry, instance); err != nil {
				return nil, err
			}
			result = append(result, instance)
		}
	}
}

// CandlesFromDuckDB returns candles from DuckDB cache for today's data
func (s *Server) CandlesFromDuckDB(q CandleDuckDBQuery) (result []*CandleResponse, err error) {
	// Check if DuckDB file exists
	if !fs.FileExists(q.DuckDBPath) {
		// Return empty result if cache doesn't exist yet
		return []*CandleResponse{}, nil
	}

	// Open the DuckDB file shared, for reading use: `?access_mode=read_only&threads=4`
	db, err := sql.Open("duckdb", q.DuckDBPath+"?access_mode=READ_WRITE")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB file: %w", err)
	}
	defer db.Close()

	// Query the cached data
	query := `
		SELECT
			open_time as openTime,
			close_time as closeTime,
			open_price as open,
			high_price as high,
			low_price as low,
			close_price as close,
			volume as volume
		FROM klines
		WHERE open_time >= %d AND close_time <=%d
		ORDER BY close_time DESC`
	// Build a decoder with the hook

	for row := range data.QueryDuckDb(fmt.Sprintf(query, q.From*1000, q.To*1000), db) {
		if row.Error != nil {
			return nil, fmt.Errorf("failed to query DuckDB: %w", row.Error)
		}
		candle := new(CandleResponse)
		decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
			DecodeHook: DecodeCandleResponseHook(),
			Result:     candle,
		})
		if err != nil {
			return nil, fmt.Errorf("new decoder: %w", err)
		}

		if err := decoder.Decode(row.Data); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}

		result = append(result, candle)

	}

	return result, nil
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

type CandleResponse struct {
	OpenTime  data.ResponseDate `json:"openTime"`
	CloseTime data.ResponseDate `json:"closeTime"`
	Open      float64           `json:"open"`
	High      float64           `json:"high"`
	Low       float64           `json:"low"`
	Close     float64           `json:"close"`
	Volume    float64           `json:"volume"`
}

// DecodeCandleResponseHook decodes the candle response from string to the correct type
func DecodeCandleResponseHook() mapstructure.DecodeHookFuncType {
	return func(
		f reflect.Type, // type of the source value (e.g. string)
		t reflect.Type, // type of the target field (e.g. time.Time)
		d any, // the actual value (the string)
	) (any, error) {
		switch t.String() {
		case "data.ResponseDate":
			val, err := strconv.ParseInt(d.(string), 10, 64)
			return data.ResponseDate(*data.AnyTimestampToTime(val)), err
		case "float64":
			return strconv.ParseFloat(d.(string), 64)
		}
		return d, nil
	}
}
