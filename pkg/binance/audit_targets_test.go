package binance

import (
	"testing"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/stretchr/testify/assert"
)

func TestBuildHourlyTargets_FromHourAfterToHour_ReturnsEmpty(t *testing.T) {
	asset := &HistoryAsset{
		MarketType: Spot,
		Indicator:  Klines,
		Frame:      data.Minute,
		Market:     "BTCUSDT",
	}
	day := time.Date(2026, 5, 21, 0, 0, 0, 0, time.UTC)

	targets := BuildHourlyTargets(asset, day, 20, 13, nil)

	assert.Empty(t, targets)
}

func TestBuildAuditTargetsForRange_TodayFutureFromHour_NoPanic(t *testing.T) {
	now := time.Now().UTC()
	from := time.Date(now.Year(), now.Month(), now.Day(), 20, 0, 0, 0, time.UTC)
	to := time.Date(now.Year(), now.Month(), now.Day(), 15, 0, 0, 0, time.UTC)
	if !from.After(to) {
		t.Skip("current hour layout does not produce fromH > toH")
	}

	asset := &HistoryAsset{
		MarketType: Spot,
		Indicator:  Klines,
		Frame:      data.Minute,
		Market:     "BTCUSDT",
		Date:       now,
	}

	assert.NotPanics(t, func() {
		_ = BuildAuditTargetsForRange(asset, from, to)
	})
}
