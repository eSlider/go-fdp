package binance

import (
	"testing"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/stretchr/testify/assert"
)

func TestHourFetchStart_LeadingGap(t *testing.T) {
	asset := &HistoryAsset{Frame: data.NewFrame("1m")}
	hourStart := time.Date(2026, 5, 17, 21, 0, 0, 0, time.UTC)
	existing := []*Kline{{OpenTime: hourStart.Add(11 * time.Minute).UnixMilli()}}
	got := hourFetchStart(asset, hourStart, int64(time.Minute/time.Millisecond), existing)
	assert.Equal(t, hourStart, got)
}

func TestHourFetchStart_TailRefresh(t *testing.T) {
	asset := &HistoryAsset{Frame: data.NewFrame("1m")}
	hourStart := time.Date(2026, 5, 17, 21, 0, 0, 0, time.UTC)
	last := hourStart.Add(5 * time.Minute)
	existing := []*Kline{{OpenTime: hourStart.UnixMilli()}, {OpenTime: last.UnixMilli()}}
	got := hourFetchStart(asset, hourStart, int64(time.Minute/time.Millisecond), existing)
	assert.Equal(t, last, got)
}

func TestHourHasLeadingGap(t *testing.T) {
	s := &HistoryConsumer{}
	asset := &HistoryAsset{Frame: data.NewFrame("1m")}
	hourStart := time.Date(2026, 5, 17, 21, 0, 0, 0, time.UTC)
	assert.True(t, s.hourHasLeadingGap([]*Kline{{OpenTime: hourStart.Add(11 * time.Minute).UnixMilli()}}, hourStart, asset))
	assert.False(t, s.hourHasLeadingGap([]*Kline{{OpenTime: hourStart.UnixMilli()}}, hourStart, asset))
}
