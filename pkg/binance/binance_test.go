package binance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"sync-v3/pkg/binance/v3"
	"sync-v3/pkg/data"
	"sync-v3/pkg/fs"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHistoryAssetPathRoundTrip(t *testing.T) {
	cases := []struct {
		name      string
		market    string
		frame     string
		freq      Frequency
		indicator Indicator
		date      time.Time
	}{
		{"daily klines 1m", "BTCUSDT", "1m", Daily, Klines, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"daily klines 1h", "ETHUSDT", "1h", Daily, Klines, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"monthly klines", "BTCUSDT", "1m", Monthly, Klines, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"monthly aggTrades", "ETHUSDT", "", Monthly, AggTrades, time.Date(2021, 6, 1, 0, 0, 0, 0, time.UTC)},
		{"daily trades", "BTCUSDT", "", Daily, Trades, time.Date(2022, 3, 15, 0, 0, 0, 0, time.UTC)},
		{"daily aggTrades", "BTCUSDT", "", Daily, AggTrades, time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			asset := &HistoryAsset{
				MarketType: Spot,
				Frequency:  tc.freq,
				Indicator:  tc.indicator,
				Date:       tc.date,
				Frame:      data.StringToFrame(tc.frame),
				Market:     tc.market,
			}

			link := asset.SymbolDateAssetZipLink()
			parsed, err := NewHistoryAssetByPath(link)
			require.NoError(t, err)
			assert.Equal(t, link, parsed.SymbolDateAssetZipLink())
		})
	}
}

func TestKlineParquetRoundTrip(t *testing.T) {
	date := time.Date(2023, 10, 26, 0, 0, 0, 0, time.UTC)
	openTime := date.Add(12*time.Hour + 30*time.Minute)

	kline := &v3.Kline{
		OpenTime:   openTime.UnixMilli(),
		CloseTime:  openTime.Add(time.Minute).UnixMilli(),
		OpenPrice:  100,
		HighPrice:  110,
		LowPrice:   90,
		ClosePrice: 105,
		Volume:     1000,
	}

	pq, err := kline.Parquet()
	require.NoError(t, err)
	assert.Equal(t, int32((12*60+30)*60*1000), pq.OpenTime)

	back := pq.ToKline(date)
	assert.Equal(t, kline.OpenTime, back.OpenTime)
	assert.Equal(t, kline.OpenPrice, back.OpenPrice)
}

func TestAggTradeParquetRoundTrip(t *testing.T) {
	now := time.Date(2025, 12, 13, 12, 0, 0, 0, time.UTC)
	trade := &v3.AggTrade{
		AggTradeID:       12345,
		Price:            100000.50,
		Quantity:         0.5,
		FirstTradeID:     1000,
		LastTradeID:      1005,
		Timestamp:        now.UnixMilli(),
		IsBuyerMaker:     true,
		IsBestPriceMatch: true,
	}

	pq, err := trade.Parquet()
	require.NoError(t, err)
	assert.Equal(t, trade.AggTradeID, pq.AggTradeID)

	back := pq.ToAggTrade(now.Truncate(24 * time.Hour))
	assert.Equal(t, trade.Timestamp, back.Timestamp)
	assert.Equal(t, trade.AggTradeID, back.AggTradeID)
}

func TestSortAggTradesByID(t *testing.T) {
	trades := []*v3.AggTrade{
		{AggTradeID: 5}, {AggTradeID: 2}, {AggTradeID: 8}, {AggTradeID: 1},
	}
	sortAggTradesByID(trades)
	for i := 1; i < len(trades); i++ {
		assert.GreaterOrEqual(t, trades[i].AggTradeID, trades[i-1].AggTradeID)
	}
}

func TestRegistry(t *testing.T) {
	reg, err := NewExchangeRegistry()
	require.NoError(t, err)
	assert.NotEmpty(t, reg.Symbols)
	assert.NotEmpty(t, reg.Markets)
}

// --- integration tests (network / S3 / API); skip with -short ---

func TestAPI_GetCurrentCandles(t *testing.T) {
	skipIntegration(t)

	now := time.Now()
	start := now.Add(-time.Minute).UnixMicro()
	end := now.UnixMicro()

	for _, market := range []string{"BTCUSDT", "ETHUSDT"} {
		t.Run(market, func(t *testing.T) {
			candles, err := v3.Klines(&v3.KlineRequest{
				Base: v3.SymbolRequest{
					Symbol:    market,
					StartTime: new(start),
					EndTime:   new(end),
				},
				Interval: "1m",
			})
			require.NoError(t, err)
			require.NoError(t, err)
			require.Len(t, candles, 1)
		})
	}
}

func TestETL_HistoricalAggTrades(t *testing.T) {
	skipIntegration(t)

	consumer := mustHistoryConsumer(t)
	asset := &HistoryAsset{
		MarketType: Spot,
		Frequency:  Daily,
		Indicator:  AggTrades,
		Market:     "BTCUSDT",
		Date:       time.Date(2025, 12, 13, 0, 0, 0, 0, time.UTC),
	}

	infos, errs := drainETL(consumer.DownloadAndTransform(asset))
	for _, err := range errs {
		t.Logf("ETL warning: %v", err)
	}
	require.NotEmpty(t, infos)
	t.Logf("parquet path: %s", asset.ParquetPath())
}

func TestETL_CurrentDayKlines(t *testing.T) {
	skipIntegration(t)

	consumer := mustHistoryConsumer(t)
	asset := spotKlineAsset("BNBUSDT", time.Now().UTC())

	candles, err := consumer.FetchAndCacheCurrentDay(asset)
	require.NoError(t, err)
	require.NotEmpty(t, candles)

	parquetDir := fs.GetModuleRelativePath(asset.TodayParquetDir())
	files, err := filepath.Glob(filepath.Join(parquetDir, "*.parquet"))
	require.NoError(t, err)
	assert.NotEmpty(t, files)

	cached, err := consumer.ReadCachedCurrentDay(asset)
	require.NoError(t, err)
	assert.NotEmpty(t, cached)

	lastBefore := candles[len(candles)-1].OpenTime
	time.Sleep(time.Second)
	require.NoError(t, consumer.RefreshLastHour(asset))

	cachedAfter, err := consumer.ReadCachedCurrentDay(asset)
	require.NoError(t, err)
	lastAfter := cachedAfter[len(cachedAfter)-1].OpenTime
	assert.GreaterOrEqual(t, lastAfter, lastBefore)
}

func TestETL_CurrentDayAggTrades(t *testing.T) {
	skipIntegration(t)

	consumer := mustHistoryConsumer(t)
	now := time.Now().UTC()
	asset := &HistoryAsset{
		MarketType: Spot,
		Frequency:  Daily,
		Indicator:  AggTrades,
		Market:     "BTCUSDT",
		Date:       now,
	}

	trades, err := consumer.FetchAndCacheCurrentDayAggTrades(asset)
	require.NoError(t, err)
	require.NotEmpty(t, trades)

	hourPath := fs.GetModuleRelativePath(asset.HourlyParquetPath(now.Hour()))
	info, err := os.Stat(hourPath)
	require.NoError(t, err)
	assert.Positive(t, info.Size())

	oneHourAgo := now.Add(-time.Hour)
	hourTrades, err := consumer.fetchHourAggTradesData(asset, oneHourAgo, now)
	require.NoError(t, err)
	require.NotEmpty(t, hourTrades)
}

// --- helpers ---

func skipIntegration(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
}

func mustHistoryConsumer(t *testing.T) *HistoryConsumer {
	t.Helper()
	c, err := NewHistoryConsumer(context.Background())
	require.NoError(t, err)
	return c
}

func spotKlineAsset(market string, date time.Time) *HistoryAsset {
	return &HistoryAsset{
		MarketType: Spot,
		Frequency:  Daily,
		Frame:      data.Minute,
		Indicator:  Klines,
		Date:       date,
		Market:     market,
	}
}

func drainETL(infoCh <-chan *AssetETLInfo, errCh <-chan error) ([]*AssetETLInfo, []error) {
	var infos []*AssetETLInfo
	var errs []error
	for infoCh != nil || errCh != nil {
		select {
		case info, ok := <-infoCh:
			if !ok {
				infoCh = nil
				continue
			}
			infos = append(infos, info)
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			errs = append(errs, err)
		}
	}
	return infos, errs
}

func int64Ptr(v int64) *int64 {
	return &v
}
