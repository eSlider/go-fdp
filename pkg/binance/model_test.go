package binance

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestKline_Parquet_Conversion(t *testing.T) {
	// 2023-10-26 12:30:00 UTC
	date := time.Date(2023, 10, 26, 0, 0, 0, 0, time.UTC)
	openTime := date.Add(12*time.Hour + 30*time.Minute)

	kline := &Kline{
		OpenTime:   openTime.UnixMilli(),
		CloseTime:  openTime.Add(1 * time.Minute).UnixMilli(),
		OpenPrice:  100.0,
		HighPrice:  110.0,
		LowPrice:   90.0,
		ClosePrice: 105.0,
		Volume:     1000.0,
	}

	// Convert to ParquetKline
	pq, err := kline.Parquet()
	assert.NoError(t, err)
	assert.NotNil(t, pq)

	// Check OpenTime (should be ms from midnight)
	expectedMs := int32((12*60 + 30) * 60 * 1000)
	assert.Equal(t, expectedMs, pq.OpenTime)

	// Check other fields
	assert.Equal(t, kline.OpenPrice, pq.Open)
	assert.Equal(t, kline.HighPrice, pq.High)

	// Convert back to Kline (requires date)
	reconstructedKline := pq.ToKline(date)

	assert.Equal(t, kline.OpenTime, reconstructedKline.OpenTime)
	assert.Equal(t, kline.OpenPrice, reconstructedKline.OpenPrice)
}
