package binance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterKlinesInHour_excludesHourEnd(t *testing.T) {
	start := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	in := []*Kline{
		{OpenTime: start.UnixMilli()},
		{OpenTime: end.UnixMilli()},
	}
	out := KlineSeries(in).Filter(start, end.Sub(start))
	require.Len(t, out, 1)
	assert.Equal(t, start.UnixMilli(), out[0].OpenTime)
}

func TestHourWindow(t *testing.T) {
	s := &HistoryConsumer{ctx: context.Background()}
	midnight := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)
	start, end := s.hourWindow(midnight, 3)
	assert.Equal(t, time.Date(2025, 1, 1, 3, 0, 0, 0, time.UTC), start)
	assert.Equal(t, time.Date(2025, 1, 1, 4, 0, 0, 0, time.UTC), end)
}

func spotKlineAssetTest(market string, date time.Time) *HistoryAsset {
	return &HistoryAsset{
		MarketType: Spot,
		Frequency:  Daily,
		Frame:      data.Minute,
		Indicator:  Klines,
		Date:       date,
		Market:     market,
	}
}

func TestHourParquetIntegrityOK(t *testing.T) {
	s := &HistoryConsumer{ctx: context.Background()}
	asset := spotKlineAssetTest("TEST", time.Now().UTC())
	hourStart := time.Date(2025, 6, 1, 10, 0, 0, 0, time.UTC)
	hourEnd := hourStart.Add(time.Hour)
	var full []*Kline
	for i := 0; i < 60; i++ {
		full = append(full, &Kline{OpenTime: hourStart.Add(time.Duration(i) * time.Minute).UnixMilli()})
	}
	path := filepath.Join(t.TempDir(), "hour.parquet")
	require.NoError(t, s.writeHourlyParquet(path, full))
	assert.True(t, s.hourParquetIntegrityOK(s.ctx, path, asset, hourStart, hourEnd, 10, true))

	var partial []*Kline
	for i := 0; i < 59; i++ {
		partial = append(partial, &Kline{OpenTime: hourStart.Add(time.Duration(i) * time.Minute).UnixMilli()})
	}
	path2 := filepath.Join(t.TempDir(), "hour2.parquet")
	require.NoError(t, s.writeHourlyParquet(path2, partial))
	assert.True(t, s.hourParquetIntegrityOK(s.ctx, path2, asset, hourStart, hourEnd, 10, false))
	assert.False(t, s.hourParquetIntegrityOK(s.ctx, path2, asset, hourStart, hourEnd, 10, true))
}
