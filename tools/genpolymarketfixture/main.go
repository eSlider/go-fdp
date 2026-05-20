// Command genpolymarketfixture writes a minimal Parquet file in FDP prediction Row schema
// for testing polymarket-import -source hf.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/polymarket"
)

func main() {
	outDir := "testdata/polymarket/hf"
	if len(os.Args) > 1 {
		outDir = os.Args[1]
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	path := filepath.Join(outDir, "btc_sample.parquet")
	t0 := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	ws := t0
	we := t0.Add(5 * time.Minute)
	rows := []polymarket.Row{
		{Ts: t0.UnixMilli(), UpPrice: 0.52, DownPrice: 0.48, EventSlug: "btc-updown-5m-demo", ConditionID: "0x1", WindowStart: ws.UnixMilli(), WindowEnd: we.UnixMilli()},
		{Ts: t1.UnixMilli(), UpPrice: 0.48, DownPrice: 0.52, EventSlug: "btc-updown-5m-demo", ConditionID: "0x1", WindowStart: ws.UnixMilli(), WindowEnd: we.UnixMilli()},
	}
	rCh, errCh := data.WriteParquet[polymarket.Row](path)
	for i := range rows {
		rCh <- &rows[i]
	}
	close(rCh)
	if err, ok := <-errCh; ok {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("wrote", path)
}
