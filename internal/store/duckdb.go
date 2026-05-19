package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
	"time"

	"github.com/eslider/go-fdp/internal/query"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/fs"
	"github.com/go-viper/mapstructure/v2"

	_ "github.com/duckdb/duckdb-go/v2"
)

type Store struct {
	db       *sql.DB
	dataPath string
}

func NewStore() (*Store, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}
	dataPath, _ := filepath.Abs(fs.GetModuleRelativePath("data"))
	return &Store{db: db, dataPath: dataPath}, nil
}

// NewStoreWithPath creates a repository with a custom data path (for testing)
func NewStoreWithPath(dataPath string) (*Store, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}
	return &Store{db: db, dataPath: dataPath}, nil
}

func (r *Store) Close() error {
	return r.db.Close()
}

func (r *Store) GetCandles(ctx context.Context, req query.Query) ([]*query.Candle, error) {
	histReq, todayReq := splitCandleRequest(req)
	var result []*query.Candle

	if histReq != nil {
		historical, err := r.candlesFromParquet(*histReq)
		if err != nil {
			return nil, err
		}
		result = append(result, historical...)
	}
	if todayReq != nil {
		today, err := r.candlesFromHourlyParquet(*todayReq)
		if err != nil {
			return nil, err
		}
		result = append(result, today...)
	}
	return result, nil
}

// splitCandleRequest splits [from, to) into hive daily files (before today) and hourly current (today).
func splitCandleRequest(req query.Query) (hist, today *query.Query) {
	from, to := req.From.UTC(), req.To.UTC()
	if !to.After(from) {
		return nil, nil
	}
	todayStart := time.Now().UTC().Truncate(24 * time.Hour)

	if !to.After(todayStart) {
		r := req
		r.From, r.To = from, to
		return &r, nil
	}
	if !from.Before(todayStart) {
		r := req
		r.From, r.To = from, to
		return nil, &r
	}
	h := req
	h.From = from
	h.To = todayStart
	t := req
	t.From = todayStart
	t.To = to
	return &h, &t
}

// effectiveQueryEnd extends [from, to) so the open (incomplete) candle at now is included when to is near now.
func effectiveQueryEnd(to time.Time, frame data.Frame) int64 {
	toMs := to.UTC().UnixMilli()
	frameMs := int64(time.Duration(frame) / time.Millisecond)
	if frameMs <= 0 {
		return toMs
	}
	nowMs := time.Now().UTC().UnixMilli()
	const liveTailMs = 5 * 60 * 1000
	if nowMs-toMs > liveTailMs {
		return toMs
	}
	currentOpen := (nowMs / frameMs) * frameMs
	liveEnd := currentOpen + frameMs
	if toMs < liveEnd {
		return liveEnd
	}
	return toMs
}

func (r *Store) GetAggTrades(ctx context.Context, req query.Query) ([]*query.AggTrade, error) {
	var result []*query.AggTrade

	// Query historical data
	historical, err := r.aggTradesFromParquet(req)
	if err != nil {
		// If no historical files found, continue (don't fail)
		// This allows today's queries to work even if historical query fails
		slog.Warn("Failed to query historical aggTrades data", "error", err)
	} else {
		result = append(result, historical...)
	}

	// Query today's data if needed
	if req.IsToday() {
		today, err := r.aggTradesFromHourlyParquet(req)
		if err != nil {
			return nil, err
		}
		result = append(result, today...)
	}

	return result, nil
}

func (r *Store) candlesFromParquet(req query.Query) ([]*query.Candle, error) {

	// Calculate interval
	frame := data.NewFrame(req.Frame.String())
	intervalStr := fmt.Sprintf("%d ms", int64(time.Duration(frame)/time.Millisecond))

	// Query historical data
	resCh, errCh := data.QueryParquets(r.db, `
		SELECT
			make_timestamp(year::BIGINT, month::BIGINT, day::BIGINT,
				date_part('hour', open_time)::BIGINT,
				date_part('minute', open_time)::BIGINT,
				date_part('second', open_time)) as openTime,
			openTime + interval '%<Interval>s' - interval '1' millisecond AS closeTime,

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
			AND openTime >= epoch_ms(%<From>d) AND openTime < epoch_ms(%<To>d)
		ORDER BY
			openTime DESC
	`, struct {
		DataPath   string
		MarketType string
		Indicator  string
		Market     string
		Frame      string
		From       int64
		To         int64
		Interval   string
	}{
		DataPath:   r.dataPath,
		MarketType: req.MarketType.String(),
		Indicator:  req.Indicator.String(),
		Market:     req.Market,
		Frame:      req.Frame.String(),
		From:       req.From.UnixMilli(),
		To:         req.To.UnixMilli(),
		Interval:   intervalStr,
	})

	var result []*query.Candle
	for {
		select {
		case err, ok := <-errCh:
			if ok {
				return nil, err
			}
			return result, nil
		case entry, ok := <-resCh:
			if !ok {
				return result, nil
			}

			// DEBUG: Print keys
			// fmt.Printf("Keys: %v\n", getKeys(entry))

			instance := &query.Candle{}
			//// Manual mapping with case-insensitive lookup logic if needed, but sticking to expected keys for now
			//instance.OpenTime = toTime(entry["openTime"])
			//instance.CloseTime = toTime(entry["closeTime"])
			//instance.Open = toFloat64(entry["open"])
			//instance.High = toFloat64(entry["high"])
			//instance.Low = toFloat64(entry["low"])
			//instance.Close = toFloat64(entry["close"])
			//instance.Volume = toFloat64(entry["volume"])
			//
			//// If all zero, try checking if keys are lowercased
			//if instance.Open == 0 && instance.Volume == 0 {
			//	// Fallback to lowercase check? DuckDB might lowercase aliases?
			//	// No, DuckDB usually preserves case in aliases if quoted, but here they are unquoted identifiers.
			//	// Unquoted identifiers are usually lowercased?
			//	// Let's try "opentime" etc.
			//	if t := toTime(entry["opentime"]); !t.IsZero() {
			//		instance.OpenTime = t
			//	}
			//	if t := toTime(entry["closetime"]); !t.IsZero() {
			//		instance.CloseTime = t
			//	}
			//	if v := toFloat64(entry["open"]); v != 0 {
			//		instance.Open = v
			//	}
			//	if v := toFloat64(entry["high"]); v != 0 {
			//		instance.High = v
			//	}
			//	if v := toFloat64(entry["low"]); v != 0 {
			//		instance.Low = v
			//	}
			//	if v := toFloat64(entry["close"]); v != 0 {
			//		instance.Close = v
			//	}
			//	if v := toFloat64(entry["volume"]); v != 0 {
			//		instance.Volume = v
			//	}
			//}

			if err := mapstructure.WeakDecode(entry, instance); err != nil {
				return nil, fmt.Errorf("failed to decode candle from parquet: %w", err)
			}

			result = append(result, instance)
		}
	}
}

func (r *Store) candlesFromHourlyParquet(req query.Query) ([]*query.Candle, error) {
	now := time.Now().UTC()
	hourlyPath := fmt.Sprintf("%s/mtype=%s/indicator=%s/market=%s/frame=%s/year=%d/month=%d/day=%d/current/*.parquet",
		r.dataPath, req.MarketType, req.Indicator, req.Market, req.Frame,
		now.Year(), int(now.Month()), now.Day())

	if !fs.FileExists(filepath.Dir(hourlyPath)) {
		return []*query.Candle{}, nil
	}

	frame := data.NewFrame(req.Frame.String())
	intervalStr := fmt.Sprintf("%d ms", int64(time.Duration(frame)/time.Millisecond))
	queryTo := effectiveQueryEnd(req.To, frame)

	resCh, errCh := data.QueryParquets(r.db, `
		SELECT openTime, closeTime, open, close, high, low, volume
		FROM (
			SELECT
				CASE
					WHEN typeof(open_time) = 'TIME' THEN make_timestamp(%<Year>d, %<Month>d, %<Day>d, 0, 0, 0.0) + (date_part('epoch', open_time) * interval '1 second')
					WHEN open_time::BIGINT < 86400000 THEN make_timestamp(%<Year>d, %<Month>d, %<Day>d, 0, 0, 0.0) + (open_time::BIGINT * interval '1 millisecond')
					ELSE epoch_ms(open_time::BIGINT)
				END as openTime,
				openTime + interval '%<Interval>s' - interval '1 ms' as closeTime,
				open_price as open,
				close_price as close,
				high_price as high,
				low_price as low,
				volume as volume,
				ROW_NUMBER() OVER (PARTITION BY openTime ORDER BY volume DESC) AS rn
			FROM read_parquet('%<HourlyPath>s', union_by_name=true)
		) deduped
		WHERE rn = 1
			AND openTime >= epoch_ms(%<From>d) AND openTime < epoch_ms(%<To>d)
		ORDER BY openTime DESC
	`, struct {
		HourlyPath string
		From       int64
		To         int64
		Year       int
		Month      int
		Day        int
		Interval   string
	}{
		HourlyPath: hourlyPath,
		From:       req.From.UnixMilli(),
		To:         queryTo,
		Year:       now.Year(),
		Month:      int(now.Month()),
		Day:        now.Day(),
		Interval:   intervalStr,
	})

	var result []*query.Candle
	for {
		select {
		case err, ok := <-errCh:
			if ok {
				return nil, err
			}
			return result, nil
		case entry, ok := <-resCh:
			if !ok {
				return result, nil
			}
			instance := &query.Candle{}

			// Try camelCase first
			instance.OpenTime = toTime(entry["openTime"])
			instance.CloseTime = toTime(entry["closeTime"])
			instance.Open = toFloat64(entry["open"])
			instance.High = toFloat64(entry["high"])
			instance.Low = toFloat64(entry["low"])
			instance.Close = toFloat64(entry["close"])
			instance.Volume = toFloat64(entry["volume"])

			// Fallback to lowercase if needed (DuckDB behavior)
			if instance.Open == 0 && instance.Volume == 0 {
				if t := toTime(entry["opentime"]); !t.IsZero() {
					instance.OpenTime = t
				}
				if t := toTime(entry["closetime"]); !t.IsZero() {
					instance.CloseTime = t
				}
				// Note: open, high, low, close, volume are already lowercase in query, so entry["open"] should work if they are lowercase.
				// But wait, open_price as open.
			}

			result = append(result, instance)
		}
	}
}

func toFloat64(v any) float64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

func (r *Store) aggTradesFromParquet(req query.Query) ([]*query.AggTrade, error) {

	// Query historical data
	resCh, errCh := data.QueryParquets(r.db, `
		SELECT
			agg_trade_id as aggTradeId,
			price as price,
			quantity as quantity,
			first_trade_id as firstTradeId,
			last_trade_id as lastTradeId,
			make_timestamp(year::BIGINT, month::BIGINT, day::BIGINT, 0, 0, 0.0) + (open_time::BIGINT * interval '1 millisecond') as time,
			is_buyer_maker as isBuyerMaker

		FROM read_parquet('%<DataPath>s/*/*/*/*/*/*/*/data.parquet')

		WHERE mtype = '%<MarketType>s'
			AND indicator = '%<Indicator>s'
			AND market = '%<Market>s'
			AND time BETWEEN epoch_ms(%<From>d) AND epoch_ms(%<To>d)
		ORDER BY
			time DESC
	`, struct {
		DataPath   string
		MarketType string
		Indicator  string
		Market     string
		From       int64
		To         int64
	}{
		DataPath:   r.dataPath,
		MarketType: req.MarketType.String(),
		Indicator:  req.Indicator.String(),
		Market:     req.Market,
		From:       req.From.UnixMilli(),
		To:         req.To.UnixMilli(),
	})

	var result []*query.AggTrade
	for {
		select {
		case err, ok := <-errCh:
			if ok {
				return nil, err
			}
			return result, nil
		case entry, ok := <-resCh:
			if !ok {
				return result, nil
			}

			instance := &query.AggTrade{}
			instance.ID = toInt64(getMapValue(entry, "aggTradeId", "aggtradeid", "agg_trade_id"))
			instance.Price = toFloat64(getMapValue(entry, "price"))
			instance.Quantity = toFloat64(getMapValue(entry, "quantity"))
			instance.FirstTradeID = toInt64(getMapValue(entry, "firstTradeId", "firsttradeid", "first_trade_id"))
			instance.LastTradeID = toInt64(getMapValue(entry, "lastTradeId", "lasttradeid", "last_trade_id"))
			// For aggTrades, time comes as timestamp from make_timestamp
			timeVal := getMapValue(entry, "time")
			if tv, ok := timeVal.(float64); ok {
				instance.Time = time.UnixMilli(int64(tv))
			} else if tv, ok := timeVal.(int64); ok {
				instance.Time = time.UnixMilli(tv)
			} else {
				instance.Time = toTime(timeVal)
			}
			instance.IsBuyerMaker = toBool(getMapValue(entry, "isBuyerMaker", "isbuyermaker", "is_buyer_maker"))

			result = append(result, instance)
		}
	}
}

func (r *Store) aggTradesFromHourlyParquet(req query.Query) (
	result []*query.AggTrade, err error,
) {
	result = make([]*query.AggTrade, 0)
	now := time.Now().UTC()
	// For aggTrades, don't include frame in the path since they don't have frames
	hourlyPath := fmt.Sprintf("%s/mtype=%s/indicator=%s/market=%s/year=%d/month=%d/day=%d/current/*.parquet",
		r.dataPath, req.MarketType, req.Indicator, req.Market,
		now.Year(), int(now.Month()), now.Day())

	if !fs.FileExists(filepath.Dir(hourlyPath)) {
		return []*query.AggTrade{}, nil
	}

	resCh, errCh := data.QueryParquets(r.db, `
		SELECT
			aggTradeId,
			price,
			quantity,
			firstTradeId,
			lastTradeId,
			time,
			isBuyerMaker
		FROM (
			SELECT
				agg_trade_id as aggTradeId,
				price as price,
				quantity as quantity,
				first_trade_id as firstTradeId,
				last_trade_id as lastTradeId,
				CASE
					WHEN typeof(open_time) = 'TIME' THEN make_timestamp(%<Year>d, %<Month>d, %<Day>d, 0, 0, 0.0) + (date_part('epoch', open_time) * interval '1 second')
					WHEN open_time::BIGINT < 86400000 THEN make_timestamp(%<Year>d, %<Month>d, %<Day>d, 0, 0, 0.0) + (open_time::BIGINT * interval '1 millisecond')
					ELSE epoch_ms(open_time::BIGINT)
				END as time,
				is_buyer_maker as isBuyerMaker
			FROM read_parquet('%<HourlyPath>s', union_by_name=true)
		)
		WHERE time BETWEEN epoch_ms(%<From>d) AND epoch_ms(%<To>d)
		ORDER BY time DESC
	`, struct {
		HourlyPath string
		From       int64
		To         int64
		Year       int
		Month      int
		Day        int
	}{
		HourlyPath: hourlyPath,
		From:       req.From.UnixMilli(),
		To:         req.To.UnixMilli(),
		Year:       now.Year(),
		Month:      int(now.Month()),
		Day:        now.Day(),
	})

	// var result []*query.AggTrade
	for {
		select {
		case err, ok := <-errCh:
			if ok {
				return nil, err
			}
			return result, nil
		case entry, ok := <-resCh:
			if !ok {
				return result, nil
			}

			instance := &query.AggTrade{}
			// Try both camelCase and lowercase (DuckDB driver may lowercase)
			instance.ID = toInt64(getMapValue(entry, "aggTradeId", "aggtradeid", "agg_trade_id"))
			instance.Price = toFloat64(getMapValue(entry, "price"))
			instance.Quantity = toFloat64(getMapValue(entry, "quantity"))
			instance.FirstTradeID = toInt64(getMapValue(entry, "firstTradeId", "firsttradeid", "first_trade_id"))
			instance.LastTradeID = toInt64(getMapValue(entry, "lastTradeId", "lasttradeid", "last_trade_id"))
			// For hourly parquet, time comes as milliseconds since epoch (from epoch_ms)
			timeVal := getMapValue(entry, "time")
			if tv, ok := timeVal.(float64); ok {
				instance.Time = time.UnixMilli(int64(tv))
			} else if tv, ok := timeVal.(int64); ok {
				instance.Time = time.UnixMilli(tv)
			} else {
				instance.Time = toTime(timeVal)
			}
			instance.IsBuyerMaker = toBool(getMapValue(entry, "isBuyerMaker", "isbuyermaker", "is_buyer_maker"))

			result = append(result, instance)
		}
	}
}

// getMapValue tries multiple keys and returns the first non-nil value
func getMapValue(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			return v
		}
	}
	return nil
}

func toInt64(v any) int64 {
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case int64:
		return val
	case uint64:
		return int64(val)
	case int:
		return int64(val)
	case uint:
		return int64(val)
	case int32:
		return int64(val)
	case uint32:
		return int64(val)
	case float64:
		return int64(val)
	case float32:
		return int64(val)
	case string:
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i
		}
		return 0
	default:
		return 0
	}
}

func toBool(v any) bool {
	if v == nil {
		return false
	}
	switch val := v.(type) {
	case bool:
		return val
	case int64:
		return val != 0
	case int:
		return val != 0
	case string:
		return val == "true" || val == "1"
	default:
		return false
	}
}

func toTime(v any) time.Time {
	if v == nil {
		return time.Time{}
	}
	switch val := v.(type) {
	case time.Time:
		return val
	case string:
		// Try standard formats
		if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
			return t
		}
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t
		}
		// DuckDB string timestamp might look different?
		return time.Time{}
	case int64:
		return *data.AnyTimestampToTime(val)
	default:
		return time.Time{}
	}
}

// Helper to debug keys
func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
