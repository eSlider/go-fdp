package polymarket

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
)

// Collector lazy-backfills prediction history into Parquet.
type Collector struct {
	client *Client
	store  *Store
	// MaxWindowsPerDay limits API calls per UTC day (0 = unlimited). Used by bulk import smoke tests.
	MaxWindowsPerDay int
}

func NewCollector(client *Client, store *Store) *Collector {
	return &Collector{client: client, store: store}
}

// EnsureRange backfills missing UTC days in [from, to) for market and frame.
func (c *Collector) EnsureRange(ctx context.Context, market string, frame data.Frame, from, to time.Time) error {
	from = from.UTC()
	to = to.UTC()
	for day := truncateDay(from); day.Before(to); day = day.AddDate(0, 0, 1) {
		if err := ctx.Err(); err != nil {
			return err
		}
		asset := Asset{Market: market, Frame: frame, Date: day}
		if c.store.DayFileExists(asset) {
			continue
		}
		dayEnd := day.AddDate(0, 0, 1)
		rangeFrom := from
		if rangeFrom.Before(day) {
			rangeFrom = day
		}
		rangeTo := to
		if rangeTo.After(dayEnd) {
			rangeTo = dayEnd
		}
		if err := c.backfillDay(ctx, asset, rangeFrom, rangeTo); err != nil {
			slog.Warn("polymarket backfill day", "day", day.Format("2006-01-02"), "frame", frame.String(), "error", err)
		}
	}
	return nil
}

func (c *Collector) backfillDay(ctx context.Context, asset Asset, from, to time.Time) error {
	var all []Snapshot
	windows := WindowsInRange(from, to, asset.Frame)
	if c.MaxWindowsPerDay > 0 && len(windows) > c.MaxWindowsPerDay {
		windows = windows[:c.MaxWindowsPerDay]
	}
	for _, w := range windows {
		if err := ctx.Err(); err != nil {
			return err
		}
		snaps, err := c.fetchWindow(ctx, asset.Frame, w)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return err
		}
		all = append(all, snaps...)
	}
	if len(all) == 0 {
		return nil
	}
	target := asset.Frame
	if NativeFrameDuration(asset.Frame) > time.Duration(asset.Frame) {
		all = ResampleLast(all, target)
	}
	all = DedupeSnapshots(all)
	return c.store.MergeDay(asset, all)
}

func (c *Collector) fetchWindow(ctx context.Context, frame data.Frame, windowStart time.Time) ([]Snapshot, error) {
	if !HasNativeSlug(frame) {
		return nil, ErrNotFound
	}
	slug := SlugForWindow(frame, windowStart)
	ev, err := c.client.FetchEventBySlug(ctx, slug)
	if err != nil {
		return nil, err
	}
	winEnd := windowStart.Add(NativeFrameDuration(frame))
	if !ev.WindowEnd.IsZero() {
		winEnd = ev.WindowEnd
	}
	history, err := c.client.FetchPricesHistory(ctx, ev.UpTokenID, windowStart, winEnd, 1)
	if err != nil {
		return nil, err
	}
	snaps := historyToSnapshots(ev, history)
	if len(snaps) == 0 && ev.UpTokenID != "" {
		if up, down, err := c.livePrices(ctx, ev); err == nil {
			now := time.Now().UTC()
			snaps = []Snapshot{{
				Time:        now,
				UpPrice:     up,
				DownPrice:   down,
				EventSlug:   ev.Slug,
				ConditionID: ev.ConditionID,
				WindowStart: windowStart,
				WindowEnd:   winEnd,
			}}
		}
	}
	native := NativeFrameDuration(frame)
	if time.Duration(frame) < native && len(snaps) > 0 {
		snaps = ResampleLast(snaps, frame)
	}
	return snaps, nil
}

// FetchCurrentSnapshot resolves the active native Polymarket window and returns a live snapshot.
func (c *Collector) FetchCurrentSnapshot(ctx context.Context, market string, frame data.Frame) ([]Snapshot, error) {
	_ = market
	if !HasNativeSlug(frame) {
		return nil, ErrNotFound
	}
	now := time.Now().UTC()
	ws := AlignWindowStart(now, frame)
	ev, err := c.client.FetchEventBySlug(ctx, SlugForWindow(frame, ws))
	if err != nil {
		return nil, err
	}
	up, down, err := c.livePrices(ctx, ev)
	if err != nil {
		return nil, err
	}
	winEnd := ws.Add(NativeFrameDuration(frame))
	if !ev.WindowEnd.IsZero() {
		winEnd = ev.WindowEnd
	}
	return []Snapshot{{
		Time:        now,
		UpPrice:     up,
		DownPrice:   down,
		EventSlug:   ev.Slug,
		ConditionID: ev.ConditionID,
		WindowStart: ws,
		WindowEnd:   winEnd,
	}}, nil
}

// livePrices returns implied Up/Down probabilities, preferring Gamma outcomePrices
// over the noisy CLOB midpoint for the Up token alone.
func (c *Collector) livePrices(ctx context.Context, ev *ResolvedEvent) (float64, float64, error) {
	if ev.HasOutcomePrices() {
		return ev.OutcomeUp, ev.OutcomeDown, nil
	}
	up, err := c.client.FetchPrice(ctx, ev.UpTokenID)
	if err != nil {
		return 0, 0, err
	}
	return up, 1 - up, nil
}

func truncateDay(t time.Time) time.Time {
	return t.UTC().Truncate(24 * time.Hour)
}
