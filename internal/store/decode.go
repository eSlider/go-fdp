package store

import (
	"fmt"
	"reflect"
	"time"

	"github.com/eslider/go-fdp/internal/query"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/go-viper/mapstructure/v2"
)

func decodeParquetRow(entry map[string]any, dest any) error {
	dec, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName:          "mapstructure,json",
		Result:           dest,
		WeaklyTypedInput: true,
		DecodeHook: mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeHookFunc(time.RFC3339),
			duckDBTimeHook,
		),
	})
	if err != nil {
		return fmt.Errorf("parquet decoder: %w", err)
	}
	if err := dec.Decode(entry); err != nil {
		return err
	}
	return nil
}

func duckDBTimeHook(from reflect.Type, to reflect.Type, v any) (any, error) {
	if to != reflect.TypeOf(time.Time{}) {
		return v, nil
	}
	switch val := v.(type) {
	case time.Time:
		return val, nil
	case string:
		if t, err := time.Parse("2006-01-02 15:04:05", val); err == nil {
			return t, nil
		}
		if t, err := time.Parse(time.RFC3339, val); err == nil {
			return t, nil
		}
	case int64:
		if t := data.AnyTimestampToTime(val); t != nil {
			return *t, nil
		}
	case int:
		if t := data.AnyTimestampToTime(int64(val)); t != nil {
			return *t, nil
		}
	case float64:
		if t := data.AnyTimestampToTime(int64(val)); t != nil {
			return *t, nil
		}
	case float32:
		if t := data.AnyTimestampToTime(int64(val)); t != nil {
			return *t, nil
		}
	}
	return v, nil
}

func decodeCandle(entry map[string]any) (*query.Candle, error) {
	var c query.Candle
	if err := decodeParquetRow(entry, &c); err != nil {
		return nil, fmt.Errorf("decode candle: %w", err)
	}
	return &c, nil
}

func decodeAggTrade(entry map[string]any) (*query.AggTrade, error) {
	var t query.AggTrade
	if err := decodeParquetRow(entry, &t); err != nil {
		return nil, fmt.Errorf("decode aggTrade: %w", err)
	}
	return &t, nil
}
