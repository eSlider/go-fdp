package etl

import (
	"context"
	"time"

	"github.com/eslider/go-fdp/pkg/gapfill/hourplan"
	"github.com/eslider/go-fdp/pkg/integrity"
)

// BulkLoader downloads historical archive data into parquet.
type BulkLoader interface {
	Source() Source
	DownloadAndTransform(ctx context.Context, job Job) (<-chan Progress, <-chan error)
}

// LiveSeries maintains today hourly parquet and gap fill for live data.
type LiveSeries interface {
	Source() Source
	RunHourPlan(ctx context.Context, job Job, plan []hourplan.Step) error
	BuildAuditTargets(job Job, from, to time.Time) ([]integrity.HourlyTarget, error)
}
