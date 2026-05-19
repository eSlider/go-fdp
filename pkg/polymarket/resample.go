package polymarket

import (
	"sort"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
)

// ResampleLast keeps the last observation per target frame bucket.
func ResampleLast(snaps []Snapshot, target data.Frame) []Snapshot {
	if len(snaps) == 0 {
		return snaps
	}
	d := time.Duration(target)
	if d <= 0 {
		return snaps
	}
	sort.Slice(snaps, func(i, j int) bool {
		return snaps[i].Time.Before(snaps[j].Time)
	})
	byBucket := make(map[int64]Snapshot)
	for _, s := range snaps {
		bucket := AlignWindowStart(s.Time, target).UnixMilli()
		byBucket[bucket] = s
	}
	out := make([]Snapshot, 0, len(byBucket))
	for _, s := range byBucket {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Time.Before(out[j].Time)
	})
	return out
}

// DedupeSnapshots removes duplicate (ts, event_slug) rows keeping the last.
func DedupeSnapshots(snaps []Snapshot) []Snapshot {
	if len(snaps) == 0 {
		return snaps
	}
	sort.Slice(snaps, func(i, j int) bool {
		if snaps[i].Time.Equal(snaps[j].Time) {
			return snaps[i].EventSlug < snaps[j].EventSlug
		}
		return snaps[i].Time.Before(snaps[j].Time)
	})
	out := make([]Snapshot, 0, len(snaps))
	for _, s := range snaps {
		if len(out) == 0 {
			out = append(out, s)
			continue
		}
		last := &out[len(out)-1]
		if last.Time.Equal(s.Time) && last.EventSlug == s.EventSlug {
			out[len(out)-1] = s
			continue
		}
		out = append(out, s)
	}
	return out
}
