package polymarket

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/stretchr/testify/require"
)

func TestStore_MergeDayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st := NewStore(dir)
	day := time.Date(2026, 1, 15, 0, 0, 0, 0, time.UTC)
	asset := Asset{Market: DefaultMarket, Frame: data.FiveMinute, Date: day}
	ts := day.Add(10 * time.Minute)
	snaps := []Snapshot{{
		Time:        ts,
		UpPrice:     0.55,
		DownPrice:   0.45,
		EventSlug:   "btc-updown-5m-test",
		ConditionID: "0xabc",
		WindowStart: day,
		WindowEnd:   day.Add(5 * time.Minute),
	}}
	require.NoError(t, st.MergeDay(asset, snaps))
	rows, err := st.ReadDay(asset)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.InDelta(t, 0.55, rows[0].UpPrice, 1e-9)
	_, err = os.Stat(filepath.Join(dir, asset.ParquetPath()))
	require.NoError(t, err)
}
