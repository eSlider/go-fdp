package bitfinex

import (
	"context"
	"fmt"
	"time"

	"github.com/eslider/go-fdp/pkg/etl"
	"github.com/eslider/go-fdp/pkg/gapfill/hourplan"
	"github.com/eslider/go-fdp/pkg/integrity"
)

// Stub is a placeholder until Bitfinex ETL is implemented.
type Stub struct{}

func NewStub() *Stub { return &Stub{} }

func (Stub) Source() etl.Source { return etl.SourceBitfinex }

func (Stub) DownloadAndTransform(context.Context, etl.Job) (<-chan etl.Progress, <-chan error) {
	errCh := make(chan error, 1)
	errCh <- fmt.Errorf("etl: %s: not implemented", etl.SourceBitfinex)
	close(errCh)
	return nil, errCh
}

func (Stub) RunHourPlan(context.Context, etl.Job, []hourplan.Step) error {
	return fmt.Errorf("etl: %s: not implemented", etl.SourceBitfinex)
}

func (Stub) BuildAuditTargets(etl.Job, time.Time, time.Time) ([]integrity.HourlyTarget, error) {
	return nil, fmt.Errorf("etl: %s: not implemented", etl.SourceBitfinex)
}
