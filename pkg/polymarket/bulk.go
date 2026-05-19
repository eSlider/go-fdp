package polymarket

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
)

// BulkSource identifies tier-C import origin.
type BulkSource string

const (
	BulkSourceHF       BulkSource = "hf"
	BulkSourcePolyData BulkSource = "polydata"
)

// BulkImporter seeds FDP prediction Parquet from local bulk exports.
type BulkImporter struct {
	store *Store
}

func NewBulkImporter(store *Store) *BulkImporter {
	return &BulkImporter{store: store}
}

// ImportFromPath imports rows from a local directory or file into hive partitions.
func (b *BulkImporter) ImportFromPath(ctx context.Context, src BulkSource, path string, market string, frame data.Frame, from, to time.Time) error {
	switch src {
	case BulkSourceHF:
		return b.importHFDir(ctx, path, market, frame, from, to)
	case BulkSourcePolyData:
		return b.importPolyDataCSV(ctx, path, market, frame, from, to)
	default:
		return fmt.Errorf("polymarket bulk: unknown source %q", src)
	}
}

func (b *BulkImporter) importHFDir(ctx context.Context, dir, market string, frame data.Frame, from, to time.Time) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var snaps []Snapshot
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		rows, err := readRows(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		for _, r := range rows {
			if !matchesBTCUpDown(r.EventSlug) {
				continue
			}
			t := time.UnixMilli(r.Ts).UTC()
			if t.Before(from) || !t.Before(to) {
				continue
			}
			snaps = append(snaps, rowToSnapshot(r))
		}
	}
	return b.writeByDay(ctx, market, frame, snaps)
}

func (b *BulkImporter) importPolyDataCSV(ctx context.Context, path, market string, frame data.Frame, from, to time.Time) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	rd := csv.NewReader(f)
	header, err := rd.Read()
	if err != nil {
		return err
	}
	col := map[string]int{}
	for i, h := range header {
		col[strings.ToLower(strings.TrimSpace(h))] = i
	}
	var snaps []Snapshot
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		rec, err := rd.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		snap, ok := csvRecordToSnapshot(rec, col)
		if !ok || !matchesBTCUpDown(snap.EventSlug) {
			continue
		}
		if snap.Time.Before(from) || !snap.Time.Before(to) {
			continue
		}
		snaps = append(snaps, snap)
	}
	_ = market
	snaps = ResampleLast(snaps, frame)
	return b.writeByDay(ctx, market, frame, snaps)
}

func (b *BulkImporter) writeByDay(ctx context.Context, market string, frame data.Frame, snaps []Snapshot) error {
	byDay := make(map[string][]Snapshot)
	for _, s := range snaps {
		d := truncateDay(s.Time).Format("2006-01-02")
		byDay[d] = append(byDay[d], s)
	}
	for dayStr, daySnaps := range byDay {
		if err := ctx.Err(); err != nil {
			return err
		}
		t, _ := time.Parse("2006-01-02", dayStr)
		asset := Asset{Market: market, Frame: frame, Date: t.UTC()}
		if err := b.store.MergeDay(asset, daySnaps); err != nil {
			return err
		}
	}
	return nil
}

func matchesBTCUpDown(slug string) bool {
	s := strings.ToLower(slug)
	return strings.Contains(s, "btc-updown") ||
		strings.Contains(s, "bitcoin-up-or-down") ||
		strings.Contains(s, "btc-up-or-down")
}

func rowToSnapshot(r Row) Snapshot {
	return Snapshot{
		Time:        time.UnixMilli(r.Ts).UTC(),
		UpPrice:     r.UpPrice,
		DownPrice:   r.DownPrice,
		EventSlug:   r.EventSlug,
		ConditionID: r.ConditionID,
		WindowStart: time.UnixMilli(r.WindowStart).UTC(),
		WindowEnd:   time.UnixMilli(r.WindowEnd).UTC(),
	}
}

func csvRecordToSnapshot(rec []string, col map[string]int) (Snapshot, bool) {
	get := func(name string) string {
		i, ok := col[name]
		if !ok || i >= len(rec) {
			return ""
		}
		return rec[i]
	}
	tsStr := get("timestamp")
	if tsStr == "" {
		tsStr = get("ts")
	}
	ts, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return Snapshot{}, false
	}
	if ts < 1e12 {
		ts *= 1000
	}
	price, _ := strconv.ParseFloat(get("price"), 64)
	slug := get("slug")
	if slug == "" {
		slug = get("event_slug")
	}
	return Snapshot{
		Time:      time.UnixMilli(ts).UTC(),
		UpPrice:   price,
		DownPrice: 1 - price,
		EventSlug: slug,
	}, true
}
