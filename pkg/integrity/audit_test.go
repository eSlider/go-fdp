package integrity

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testKlineParquet matches binance hourly parquet layout without importing pkg/binance.
type testKlineParquet struct {
	OpenTime int32   `parquet:"name=open_time,type=INT32, convertedtype=TIME_MILLIS" json:"open_time"`
	Open     float64 `parquet:"name=open_price, type=DOUBLE" json:"open_price"`
	High     float64 `parquet:"name=high_price, type=DOUBLE" json:"high_price"`
	Low      float64 `parquet:"name=low_price, type=DOUBLE" json:"low_price"`
	Close    float64 `parquet:"name=close_price, type=DOUBLE" json:"close_price"`
	Volume   float64 `parquet:"name=volume, type=DOUBLE" json:"volume"`
}

func openTimeMsSinceMidnight(midnight, t time.Time) int32 {
	return int32(t.UnixMilli() - midnight.UnixMilli())
}

func writeTestHourParquet(t *testing.T, dir, name string, midnight time.Time, times []time.Time) string {
	t.Helper()
	path := filepath.Join(dir, name)
	writeCh, errCh := data.WriteParquet[testKlineParquet](path)
	for _, ts := range times {
		writeCh <- &testKlineParquet{OpenTime: openTimeMsSinceMidnight(midnight, ts)}
	}
	close(writeCh)
	for err := range errCh {
		require.NoError(t, err)
	}
	return path
}

func TestAuditParquet_completeHour(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDB()
	require.NoError(t, err)
	defer db.Close()

	midnight := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	hourStart := midnight.Add(10 * time.Hour)
	var times []time.Time
	for i := 0; i < 60; i++ {
		times = append(times, hourStart.Add(time.Duration(i)*time.Minute))
	}
	path := writeTestHourParquet(t, t.TempDir(), "hour_10.parquet", midnight, times)

	issues, cr, err := AuditParquet(ctx, db, AuditConfig{
		Path: path, Midnight: midnight,
		WindowStart: hourStart, WindowEnd: hourStart.Add(time.Hour),
		FrameMs: 60_000, CheckMissing: true, Hour: 10,
	})
	require.NoError(t, err)
	assert.True(t, cr.OK)
	assert.False(t, HasErrors(issues))
}

func TestAuditParquet_missingMinute(t *testing.T) {
	ctx := context.Background()
	db, err := OpenDB()
	require.NoError(t, err)
	defer db.Close()

	midnight := time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	hourStart := midnight.Add(10 * time.Hour)
	times := []time.Time{
		hourStart,
		hourStart.Add(2 * time.Minute),
	}
	path := writeTestHourParquet(t, t.TempDir(), "hour_10.parquet", midnight, times)

	issues, _, err := AuditParquet(ctx, db, AuditConfig{
		Path: path, Midnight: midnight,
		WindowStart: hourStart, WindowEnd: hourStart.Add(time.Hour),
		FrameMs: 60_000, CheckMissing: true, Hour: 10,
	})
	require.NoError(t, err)
	assert.True(t, HasErrors(issues))
	var missing bool
	for _, iss := range issues {
		if iss.Code == CodeMissingInterval {
			missing = true
		}
	}
	assert.True(t, missing)
}

func TestHasGapIssues(t *testing.T) {
	assert.True(t, HasGapIssues([]Issue{{Code: CodeMissingInterval, Severity: SeverityError}}))
	assert.False(t, HasGapIssues([]Issue{{Code: CodeOutOfWindow, Severity: SeverityWarning}}))
}
