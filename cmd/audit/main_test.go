package main

import (
	"context"
	"flag"
	"testing"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/eslider/go-binance-fdp/pkg/integrity"
	"github.com/eslider/go-binance-fdp/pkg/integrity/run"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuditToday_flagsParse(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	market := fs.String("market", "BTCUSDT", "")
	_ = fs.Bool("today", false, "")
	if err := fs.Parse([]string{"-today", "-market", "ETHUSDT"}); err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, "ETHUSDT", *market)
}

func TestAuditDay_missingFileIssue(t *testing.T) {
	db, err := integrity.OpenDB()
	require.NoError(t, err)
	defer db.Close()

	day := time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC)
	result, err := run.Audit(context.Background(), db, run.Options{
		MarketType: "spot",
		Market:     "NOPEUSDT",
		Frame:      data.Minute,
		From:       day,
		To:         day,
		CountOnly:  true,
	})
	require.NoError(t, err)
	assert.Empty(t, result.Counts)
	assert.True(t, integrity.HasErrors(result.Issues))
}

func TestExpectedCandles_1m(t *testing.T) {
	assert.Equal(t, 60, integrity.ExpectedCandlesPerHour(data.Minute))
	assert.Equal(t, 1440, integrity.ExpectedCandlesPerDay(data.Minute))
}
