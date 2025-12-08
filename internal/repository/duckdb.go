package repository

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"time"

	"sync-v3/internal/domain"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"

	_ "github.com/duckdb/duckdb-go/v2"
)

type DuckDBRepository struct {
	db *sql.DB
}

func NewDuckDBRepository() (*DuckDBRepository, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}
	return &DuckDBRepository{db: db}, nil
}

func (r *DuckDBRepository) Close() error {
	return r.db.Close()
}

func (r *DuckDBRepository) GetCandles(ctx context.Context, req domain.MarketDataRequest) ([]*domain.Candle, error) {
	var result []*domain.Candle

	// Query historical data
	historical, err := r.candlesFromParquet(req)
	if err != nil {
		return nil, err
	}
	result = append(result, historical...)

	// Query today's data if needed
	if req.IsToday() {
		today, err := r.candlesFromHourlyParquet(req)
		if err != nil {
			return nil, err
		}
		result = append(result, today...)
	}

	return result, nil
}

func (r *DuckDBRepository) candlesFromParquet(req domain.MarketDataRequest) ([]*domain.Candle, error) {
	dataPath, _ := filepath.Abs(fs.GetModuleRelativePath("data"))

	// Calculate interval
	frame := binance.NewFrame(req.Frame.String())
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
			AND openTime BETWEEN epoch_ms(%<From>d) AND epoch_ms(%<To>d)
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
		DataPath:   dataPath,
		MarketType: req.MarketType.String(),
		Indicator:  req.Indicator.String(),
		Market:     req.Market,
		Frame:      req.Frame.String(),
		From:       req.From.UnixMilli(),
		To:         req.To.UnixMilli(),
		Interval:   intervalStr,
	})

	var result []*domain.Candle
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

			instance := &domain.Candle{}
			// Manual mapping with case-insensitive lookup logic if needed, but sticking to expected keys for now
			instance.OpenTime = toTime(entry["openTime"])
			instance.CloseTime = toTime(entry["closeTime"])
			instance.Open = toFloat64(entry["open"])
			instance.High = toFloat64(entry["high"])
			instance.Low = toFloat64(entry["low"])
			instance.Close = toFloat64(entry["close"])
			instance.Volume = toFloat64(entry["volume"])

			// If all zero, try checking if keys are lowercased
			if instance.Open == 0 && instance.Volume == 0 {
				// Fallback to lowercase check? DuckDB might lowercase aliases?
				// No, DuckDB usually preserves case in aliases if quoted, but here they are unquoted identifiers.
				// Unquoted identifiers are usually lowercased?
				// Let's try "opentime" etc.
				if t := toTime(entry["opentime"]); !t.IsZero() {
					instance.OpenTime = t
				}
				if t := toTime(entry["closetime"]); !t.IsZero() {
					instance.CloseTime = t
				}
				if v := toFloat64(entry["open"]); v != 0 {
					instance.Open = v
				}
				if v := toFloat64(entry["high"]); v != 0 {
					instance.High = v
				}
				if v := toFloat64(entry["low"]); v != 0 {
					instance.Low = v
				}
				if v := toFloat64(entry["close"]); v != 0 {
					instance.Close = v
				}
				if v := toFloat64(entry["volume"]); v != 0 {
					instance.Volume = v
				}
			}

			result = append(result, instance)
		}
	}
}

func (r *DuckDBRepository) candlesFromHourlyParquet(req domain.MarketDataRequest) ([]*domain.Candle, error) {
	dataPath, _ := filepath.Abs(fs.GetModuleRelativePath("data"))
	now := time.Now().UTC()
	hourlyPath := fmt.Sprintf("%s/mtype=%s/indicator=%s/market=%s/frame=%s/year=%d/month=%d/day=%d/current/*.parquet",
		dataPath, req.MarketType, req.Indicator, req.Market, req.Frame,
		now.Year(), int(now.Month()), now.Day())

	if !fs.FileExists(filepath.Dir(hourlyPath)) {
		return []*domain.Candle{}, nil
	}

	frame := binance.NewFrame(req.Frame.String())
	intervalStr := fmt.Sprintf("%d ms", int64(time.Duration(frame)/time.Millisecond))

	resCh, errCh := data.QueryParquets(r.db, `
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
			volume as volume
		FROM read_parquet('%<HourlyPath>s', union_by_name=true)
		WHERE openTime BETWEEN epoch_ms(%<From>d) AND epoch_ms(%<To>d)
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
		To:         req.To.UnixMilli(),
		Year:       now.Year(),
		Month:      int(now.Month()),
		Day:        now.Day(),
		Interval:   intervalStr,
	})

	var result []*domain.Candle
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
			instance := &domain.Candle{}

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
