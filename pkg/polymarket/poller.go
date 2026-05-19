package polymarket

import (
	"context"
	"log/slog"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
)

// Poller periodically snapshots current prediction prices into hourly Parquet.
type Poller struct {
	collector *Collector
	store     *Store
	market    string
	interval  time.Duration
	frames    []data.Frame
}

type PollerConfig struct {
	Market   string
	Interval time.Duration
	Frames   []data.Frame
}

func NewPoller(collector *Collector, store *Store, cfg PollerConfig) *Poller {
	if cfg.Market == "" {
		cfg.Market = DefaultMarket
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Second
	}
	frames := cfg.Frames
	if len(frames) == 0 {
		frames = AllFrames
	}
	return &Poller{
		collector: collector,
		store:     store,
		market:    cfg.Market,
		interval:  cfg.Interval,
		frames:    frames,
	}
}

// Run blocks until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.tick(ctx)
		}
	}
}

func (p *Poller) tick(ctx context.Context) {
	now := time.Now().UTC()
	hour := now.Hour()
	for _, frame := range p.frames {
		snaps, err := p.collector.FetchCurrentSnapshot(ctx, p.market, frame)
		if err != nil {
			slog.Debug("polymarket poll", "frame", frame.String(), "error", err)
			continue
		}
		asset := Asset{Market: p.market, Frame: frame, Date: now}
		if err := p.store.AppendHourly(asset, hour, snaps); err != nil {
			slog.Warn("polymarket append hourly", "frame", frame.String(), "error", err)
			continue
		}
		if err := p.store.MergeDay(asset, snaps); err != nil {
			slog.Warn("polymarket merge day", "frame", frame.String(), "error", err)
		}
	}
}
