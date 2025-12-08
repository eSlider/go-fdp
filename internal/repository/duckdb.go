package repository

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
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

			instance := &domain.Candle{}
			// Manual mapping for better performance and reliability
			if val, ok := entry["openTime"]; ok && val != nil {
				if t, ok := val.(time.Time); ok {
					instance.OpenTime = t
				}
			}
			if val, ok := entry["closeTime"]; ok && val != nil {
				if t, ok := val.(time.Time); ok {
					instance.CloseTime = t
				}
			}
			if val, ok := entry["open"]; ok && val != nil {
				instance.Open = toFloat64(val)
			}
			if val, ok := entry["high"]; ok && val != nil {
				instance.High = toFloat64(val)
			}
			if val, ok := entry["low"]; ok && val != nil {
				instance.Low = toFloat64(val)
			}
			if val, ok := entry["close"]; ok && val != nil {
				instance.Close = toFloat64(val)
			}
			if val, ok := entry["volume"]; ok && val != nil {
				instance.Volume = toFloat64(val)
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

			if val, ok := entry["openTime"]; ok && val != nil {
				if t, ok := val.(time.Time); ok {
					instance.OpenTime = t
				}
			}
			if val, ok := entry["closeTime"]; ok && val != nil {
				if t, ok := val.(time.Time); ok {
					instance.CloseTime = t
				}
			}
			if val, ok := entry["open"]; ok && val != nil {
				instance.Open = toFloat64(val)
			}
			if val, ok := entry["high"]; ok && val != nil {
				instance.High = toFloat64(val)
			}
			if val, ok := entry["low"]; ok && val != nil {
				instance.Low = toFloat64(val)
			}
			if val, ok := entry["close"]; ok && val != nil {
				instance.Close = toFloat64(val)
			}
			if val, ok := entry["volume"]; ok && val != nil {
				instance.Volume = toFloat64(val)
			}

			result = append(result, instance)
		}
	}
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}
