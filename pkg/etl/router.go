package etl

import (
	"context"
	"fmt"
)

// Router dispatches ETL operations by source.
type Router struct {
	bulk map[Source]BulkLoader
	live map[Source]LiveSeries
}

// NewRouter registers bulk and live loaders per source.
func NewRouter(bulk map[Source]BulkLoader, live map[Source]LiveSeries) *Router {
	if bulk == nil {
		bulk = make(map[Source]BulkLoader)
	}
	if live == nil {
		live = make(map[Source]LiveSeries)
	}
	return &Router{bulk: bulk, live: live}
}

// Bulk returns download channels for a job.
func (r *Router) Bulk(ctx context.Context, job Job) (<-chan Progress, <-chan error, error) {
	l, ok := r.bulk[job.Source]
	if !ok {
		return nil, nil, fmt.Errorf("etl: bulk loader for %q not registered", job.Source)
	}
	info, err := l.DownloadAndTransform(ctx, job)
	return info, err, nil
}

// Live returns the live series handler for a source.
func (r *Router) Live(job Job) (LiveSeries, error) {
	l, ok := r.live[job.Source]
	if !ok {
		return nil, fmt.Errorf("etl: live series for %q not registered", job.Source)
	}
	return l, nil
}
