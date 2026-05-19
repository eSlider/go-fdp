package store

import (
	"testing"
	"time"

	"github.com/eslider/go-fdp/internal/query"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeCandle_DuckDBRow(t *testing.T) {
	open := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	closeT := open.Add(time.Minute - time.Millisecond)

	tests := []struct {
		name string
		row  map[string]any
		want query.Candle
	}{
		{
			name: "camelCase time values",
			row: map[string]any{
				"openTime":  open,
				"closeTime": closeT,
				"open":      100.5,
				"high":      101.0,
				"low":       99.5,
				"close":     100.8,
				"volume":    12.34,
			},
			want: query.Candle{
				OpenTime:  open,
				CloseTime: closeT,
				Open:      100.5,
				High:      101.0,
				Low:       99.5,
				Close:     100.8,
				Volume:    12.34,
			},
		},
		{
			name: "lowercase keys",
			row: map[string]any{
				"opentime":  open,
				"closetime": closeT,
				"open":      float32(50.25),
				"high":      int64(51),
				"low":       "49.5",
				"close":     "50.75",
				"volume":    "1000",
			},
			want: query.Candle{
				OpenTime:  open,
				CloseTime: closeT,
				Open:      50.25,
				High:      51,
				Low:       49.5,
				Close:     50.75,
				Volume:    1000,
			},
		},
		{
			name: "unix millis as float64",
			row: map[string]any{
				"openTime":  float64(open.UnixMilli()),
				"closeTime": float64(closeT.UnixMilli()),
				"open":      1.0,
				"high":      2.0,
				"low":       0.5,
				"close":     1.5,
				"volume":    3.0,
			},
			want: query.Candle{
				OpenTime:  open,
				CloseTime: closeT,
				Open:      1,
				High:      2,
				Low:       0.5,
				Close:     1.5,
				Volume:    3,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeCandle(tt.row)
			require.NoError(t, err)
			assert.Equal(t, tt.want.OpenTime.UTC(), got.OpenTime.UTC())
			assert.Equal(t, tt.want.CloseTime.UTC(), got.CloseTime.UTC())
			assert.InDelta(t, tt.want.Open, got.Open, 1e-9)
			assert.InDelta(t, tt.want.High, got.High, 1e-9)
			assert.InDelta(t, tt.want.Low, got.Low, 1e-9)
			assert.InDelta(t, tt.want.Close, got.Close, 1e-9)
			assert.InDelta(t, tt.want.Volume, got.Volume, 1e-9)
		})
	}
}

func TestDecodeAggTrade_DuckDBRow(t *testing.T) {
	ts := time.Date(2024, 6, 1, 12, 30, 0, 0, time.UTC)

	tests := []struct {
		name string
		row  map[string]any
		want query.AggTrade
	}{
		{
			name: "camelCase aliases",
			row: map[string]any{
				"aggTradeId":   int64(42),
				"price":        65000.5,
				"quantity":     0.01,
				"firstTradeId": int64(100),
				"lastTradeId":  int64(101),
				"time":         ts,
				"isBuyerMaker": true,
			},
			want: query.AggTrade{
				ID:           42,
				Price:        65000.5,
				Quantity:     0.01,
				FirstTradeID: 100,
				LastTradeID:  101,
				Time:         ts,
				IsBuyerMaker: true,
			},
		},
		{
			name: "lowercase keys and unix millis time",
			row: map[string]any{
				"aggtradeid":   float64(99),
				"price":        "100.5",
				"quantity":     2.5,
				"firsttradeid": 10,
				"lasttradeid":  11,
				"time":         float64(ts.UnixMilli()),
				"isbuyermaker": 1,
			},
			want: query.AggTrade{
				ID:           99,
				Price:        100.5,
				Quantity:     2.5,
				FirstTradeID: 10,
				LastTradeID:  11,
				Time:         ts,
				IsBuyerMaker: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeAggTrade(tt.row)
			require.NoError(t, err)
			assert.Equal(t, tt.want.ID, got.ID)
			assert.InDelta(t, tt.want.Price, got.Price, 1e-9)
			assert.InDelta(t, tt.want.Quantity, got.Quantity, 1e-9)
			assert.Equal(t, tt.want.FirstTradeID, got.FirstTradeID)
			assert.Equal(t, tt.want.LastTradeID, got.LastTradeID)
			assert.Equal(t, tt.want.Time.UTC(), got.Time.UTC())
			assert.Equal(t, tt.want.IsBuyerMaker, got.IsBuyerMaker)
		})
	}
}
