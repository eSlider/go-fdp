package paper

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBroker_OpenCloseLong(t *testing.T) {
	b := NewBroker(DefaultConfig(), "BTCUSDT")
	now := time.Now().UTC()
	require.NoError(t, b.OpenLong(now, 100_000, 0.1, 500, 2, 1.5))
	assert.NotNil(t, b.Position)
	pnl, err := b.CloseLong(now.Add(time.Hour), 101_000)
	require.NoError(t, err)
	assert.Nil(t, b.Position)
	assert.True(t, pnl > 0)
}

func TestBroker_KillSwitch(t *testing.T) {
	b := NewBroker(Config{InitialCash: 10_000, MaxDailyLoss: 0.01}, "BTCUSDT")
	b.dayStart = time.Now().UTC().Truncate(24 * time.Hour)
	b.dayStartEquity = 10_000
	b.Cash = 9_800
	assert.True(t, b.KillSwitch(100_000))
}
