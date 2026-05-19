package polymarket

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/fs"
)

// Store reads and writes prediction Parquet under the module data directory.
type Store struct {
	dataRoot string
}

func NewStore(dataRoot string) *Store {
	if dataRoot == "" {
		var err error
		dataRoot, err = filepath.Abs(fs.GetModuleRelativePath("data"))
		if err != nil {
			dataRoot = fs.GetModuleRelativePath("data")
		}
	}
	return &Store{dataRoot: dataRoot}
}

func (s *Store) absPath(rel string) string {
	return filepath.Join(s.dataRoot, rel)
}

func (s *Store) DayFileExists(asset Asset) bool {
	return fs.FileExists(s.absPath(asset.ParquetPath()))
}

// ReadDay loads all rows from a daily parquet file.
func (s *Store) ReadDay(asset Asset) ([]Row, error) {
	path := s.absPath(asset.ParquetPath())
	if !fs.FileExists(path) {
		return nil, nil
	}
	rCh, errCh := data.ReadParquet[Row](path)
	var rows []Row
	for r := range rCh {
		rows = append(rows, *r)
	}
	if err, ok := <-errCh; ok {
		return nil, err
	}
	return rows, nil
}

// WriteDay replaces a daily file with deduped rows.
func (s *Store) WriteDay(asset Asset, rows []Row) error {
	rows = dedupeRows(rows)
	path := s.absPath(asset.ParquetPath())
	rCh, errCh := data.WriteParquet[Row](path)
	for i := range rows {
		rCh <- &rows[i]
	}
	close(rCh)
	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

// MergeDay appends snapshots into the daily file with dedupe.
func (s *Store) MergeDay(asset Asset, snaps []Snapshot) error {
	existing, err := s.ReadDay(asset)
	if err != nil {
		return err
	}
	byKey := make(map[string]Row)
	for _, r := range existing {
		byKey[rowKey(r.Ts, r.EventSlug)] = r
	}
	for _, snap := range snaps {
		r := snap.ToRow()
		byKey[rowKey(r.Ts, r.EventSlug)] = r
	}
	merged := make([]Row, 0, len(byKey))
	for _, r := range byKey {
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].Ts == merged[j].Ts {
			return merged[i].EventSlug < merged[j].EventSlug
		}
		return merged[i].Ts < merged[j].Ts
	})
	return s.WriteDay(asset, merged)
}

// AppendHourly writes snapshots to the current hour parquet file.
func (s *Store) AppendHourly(asset Asset, hour int, snaps []Snapshot) error {
	if len(snaps) == 0 {
		return nil
	}
	rel := asset.HourlyParquetPath(hour)
	path := s.absPath(rel)
	var existing []Row
	if fs.FileExists(path) {
		var err error
		existing, err = readRows(path)
		if err != nil {
			return err
		}
	}
	byKey := make(map[string]Row)
	for _, r := range existing {
		byKey[rowKey(r.Ts, r.EventSlug)] = r
	}
	for _, snap := range snaps {
		r := snap.ToRow()
		byKey[rowKey(r.Ts, r.EventSlug)] = r
	}
	merged := make([]Row, 0, len(byKey))
	for _, r := range byKey {
		merged = append(merged, r)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Ts < merged[j].Ts
	})
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("mkdir hourly: %w", err)
	}
	rCh, errCh := data.WriteParquet[Row](path)
	for i := range merged {
		rCh <- &merged[i]
	}
	close(rCh)
	if err, ok := <-errCh; ok {
		return err
	}
	return nil
}

func readRows(path string) ([]Row, error) {
	rCh, errCh := data.ReadParquet[Row](path)
	var rows []Row
	for r := range rCh {
		rows = append(rows, *r)
	}
	if err, ok := <-errCh; ok {
		return nil, err
	}
	return rows, nil
}

func dedupeRows(rows []Row) []Row {
	byKey := make(map[string]Row, len(rows))
	for _, r := range rows {
		byKey[rowKey(r.Ts, r.EventSlug)] = r
	}
	out := make([]Row, 0, len(byKey))
	for _, r := range byKey {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Ts < out[j].Ts
	})
	return out
}

func rowKey(ts int64, slug string) string {
	return fmt.Sprintf("%d|%s", ts, slug)
}
