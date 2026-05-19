package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/eslider/go-fdp/internal/query"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/polymarket"
	"github.com/stretchr/testify/require"
)

func TestGetPredictions_FromParquet(t *testing.T) {
	dir := t.TempDir()
	pm := polymarket.NewStore(dir)
	day := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	asset := polymarket.Asset{Market: polymarket.DefaultMarket, Frame: data.FiveMinute, Date: day}
	ts := day.Add(12 * time.Minute)
	require.NoError(t, pm.MergeDay(asset, []polymarket.Snapshot{{
		Time:        ts,
		UpPrice:     0.62,
		DownPrice:   0.38,
		EventSlug:   "btc-updown-5m-test",
		ConditionID: "0x1",
		WindowStart: day,
		WindowEnd:   day.Add(5 * time.Minute),
	}}))

	st, err := NewStoreWithPath(dir)
	require.NoError(t, err)
	defer st.Close()

	from := day
	to := day.Add(24 * time.Hour)
	got, err := st.GetPredictions(context.Background(), query.PredictionQuery{
		From:   from,
		To:     to,
		Market: polymarket.DefaultMarket,
		Frame:  data.FiveMinute,
	})
	require.NoError(t, err)
	require.NotEmpty(t, got)
	require.InDelta(t, 0.62, got[0].UpPrice, 1e-9)
	_, err = filepath.Glob(filepath.Join(dir, "mtype=prediction", "*", "*", "*", "*", "*", "*", "data.parquet"))
	require.NoError(t, err)
}
