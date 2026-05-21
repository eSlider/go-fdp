package polymarket

import (
	"fmt"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
)

const (
	SourceID      = "polymarket"
	MarketType    = "prediction"
	DefaultMarket = "BTCUSDT"
)

// Row is the Parquet on-disk schema for prediction snapshots.
type Row struct {
	Ts          int64   `parquet:"name=ts, type=INT64" json:"ts"`
	UpPrice     float64 `parquet:"name=up_price, type=DOUBLE" json:"up_price"`
	DownPrice   float64 `parquet:"name=down_price, type=DOUBLE" json:"down_price"`
	EventSlug   string  `parquet:"name=event_slug, type=BYTE_ARRAY, convertedtype=UTF8" json:"event_slug"`
	ConditionID string  `parquet:"name=condition_id, type=BYTE_ARRAY, convertedtype=UTF8" json:"condition_id"`
	WindowStart int64   `parquet:"name=window_start, type=INT64" json:"window_start"`
	WindowEnd   int64   `parquet:"name=window_end, type=INT64" json:"window_end"`
}

// Snapshot is an in-memory observation before persistence.
type Snapshot struct {
	Time        time.Time
	UpPrice     float64
	DownPrice   float64
	EventSlug   string
	ConditionID string
	WindowStart time.Time
	WindowEnd   time.Time
}

func (s Snapshot) ToRow() Row {
	down := s.DownPrice
	if down == 0 && s.UpPrice > 0 && s.UpPrice <= 1 {
		down = 1 - s.UpPrice
	}
	return Row{
		Ts:          s.Time.UTC().UnixMilli(),
		UpPrice:     s.UpPrice,
		DownPrice:   down,
		EventSlug:   s.EventSlug,
		ConditionID: s.ConditionID,
		WindowStart: s.WindowStart.UTC().UnixMilli(),
		WindowEnd:   s.WindowEnd.UTC().UnixMilli(),
	}
}

// Asset identifies a hive partition for predictions.
type Asset struct {
	Market string
	Frame  data.Frame
	Date   time.Time
}

func (a Asset) ParquetPath() string {
	return fmt.Sprintf(
		"mtype=%s/source=%s/market=%s/frame=%s/year=%d/month=%d/day=%d/data.parquet",
		MarketType,
		SourceID,
		a.Market,
		a.Frame.String(),
		a.Date.Year(),
		int(a.Date.Month()),
		a.Date.Day(),
	)
}

func (a Asset) TodayParquetDir() string {
	now := time.Now().UTC()
	return fmt.Sprintf(
		"mtype=%s/source=%s/market=%s/frame=%s/year=%d/month=%d/day=%d/current",
		MarketType,
		SourceID,
		a.Market,
		a.Frame.String(),
		now.Year(),
		int(now.Month()),
		now.Day(),
	)
}

func (a Asset) HourlyParquetPath(hour int) string {
	return fmt.Sprintf("%s/hour_%02d.parquet", a.TodayParquetDir(), hour)
}

// ResolvedEvent is a Polymarket event with CLOB token ids for Up/Down.
type ResolvedEvent struct {
	Slug        string
	ConditionID string
	UpTokenID   string
	DownTokenID string
	WindowStart time.Time
	WindowEnd   time.Time
	// OutcomeUp/OutcomeDown are implied probabilities from Gamma outcomePrices (0 = unset).
	OutcomeUp   float64
	OutcomeDown float64
}

// HasOutcomePrices reports whether Gamma outcomePrices were parsed.
func (ev *ResolvedEvent) HasOutcomePrices() bool {
	return ev.OutcomeUp > 0 && ev.OutcomeDown > 0
}
