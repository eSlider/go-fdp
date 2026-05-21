package store

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/eslider/go-fdp/internal/query"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/fs"
	"github.com/eslider/go-fdp/pkg/polymarket"
)

func (r *Store) GetPredictions(ctx context.Context, req query.PredictionQuery) ([]*query.Prediction, error) {
	histReq, todayReq := splitPredictionRequest(req)
	var result []*query.Prediction

	if histReq != nil {
		historical, err := r.predictionsFromParquet(*histReq)
		if err != nil {
			return nil, err
		}
		result = append(result, historical...)
	}
	if todayReq != nil {
		today, err := r.predictionsFromHourlyParquet(*todayReq)
		if err != nil {
			return nil, err
		}
		result = append(result, today...)
	}
	return result, nil
}

func splitPredictionRequest(req query.PredictionQuery) (hist, today *query.PredictionQuery) {
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

func (r *Store) predictionsFromParquet(req query.PredictionQuery) ([]*query.Prediction, error) {
	resCh, errCh := data.QueryParquets(r.db, `
		SELECT
			epoch_ms(ts) as timeMs,
			up_price as upPrice,
			down_price as downPrice,
			'%<Frame>s' as frame,
			'%<Market>s' as market,
			event_slug as eventSlug,
			epoch_ms(window_start) as windowStartMs,
			epoch_ms(window_end) as windowEndMs
		FROM read_parquet('%<ParquetGlob>s')
		WHERE ts >= %<From>d AND ts < %<To>d
		ORDER BY ts ASC
	`, struct {
		ParquetGlob string
		Market      string `validate:"required"`
		Frame       string `validate:"required"`
		From        int64
		To          int64
	}{
		ParquetGlob: predictionsParquetGlob(r.dataPath, req.Market, req.Frame.String()),
		Market:      req.Market,
		Frame:       req.Frame.String(),
		From:        req.From.UnixMilli(),
		To:          req.To.UnixMilli(),
	})
	return drainPredictions(resCh, errCh, req.Frame.String(), req.Market)
}

func (r *Store) predictionsFromHourlyParquet(req query.PredictionQuery) ([]*query.Prediction, error) {
	now := time.Now().UTC()
	asset := polymarket.Asset{Market: req.Market, Frame: req.Frame, Date: now}
	hourlyPath := filepath.Join(r.dataPath, asset.TodayParquetDir(), "*.parquet")

	if !fs.FileExists(filepath.Join(r.dataPath, asset.TodayParquetDir())) {
		return []*query.Prediction{}, nil
	}

	resCh, errCh := data.QueryParquets(r.db, `
		SELECT
			ts as timeMs,
			up_price as upPrice,
			down_price as downPrice,
			'%<Frame>s' as frame,
			'%<Market>s' as market,
			event_slug as eventSlug,
			window_start as windowStartMs,
			window_end as windowEndMs
		FROM read_parquet('%<HourlyPath>s', union_by_name=true)
		WHERE ts >= %<From>d AND ts < %<To>d
		ORDER BY ts ASC
	`, struct {
		HourlyPath string
		Market     string `validate:"required"`
		Frame      string `validate:"required"`
		From       int64
		To         int64
	}{
		HourlyPath: hourlyPath,
		Market:     req.Market,
		Frame:      req.Frame.String(),
		From:       req.From.UnixMilli(),
		To:         req.To.UnixMilli(),
	})
	return drainPredictions(resCh, errCh, req.Frame.String(), req.Market)
}

func drainPredictions(resCh chan map[string]any, errCh chan error, frame, market string) ([]*query.Prediction, error) {
	var result []*query.Prediction
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
			p, err := decodePrediction(entry, frame, market)
			if err != nil {
				return nil, err
			}
			result = append(result, p)
		}
	}
}

func decodePrediction(m map[string]any, frame, market string) (*query.Prediction, error) {
	ts, err := toInt64(m["timeMs"])
	if err != nil {
		return nil, fmt.Errorf("prediction time: %w", err)
	}
	ws, _ := toInt64(m["windowStartMs"])
	we, _ := toInt64(m["windowEndMs"])
	up, _ := toFloat64(m["upPrice"])
	down, _ := toFloat64(m["downPrice"])
	slug, _ := m["eventSlug"].(string)
	return &query.Prediction{
		Time:        time.UnixMilli(ts).UTC(),
		UpPrice:     up,
		DownPrice:   down,
		Frame:       frame,
		Market:      market,
		EventSlug:   slug,
		WindowStart: time.UnixMilli(ws).UTC(),
		WindowEnd:   time.UnixMilli(we).UTC(),
	}, nil
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case float64:
		return int64(x), nil
	case int:
		return int64(x), nil
	case time.Time:
		return x.UTC().UnixMilli(), nil
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}

func toFloat64(v any) (float64, error) {
	switch x := v.(type) {
	case float64:
		return x, nil
	case float32:
		return float64(x), nil
	case int64:
		return float64(x), nil
	case int:
		return float64(x), nil
	default:
		return 0, fmt.Errorf("unexpected type %T", v)
	}
}
