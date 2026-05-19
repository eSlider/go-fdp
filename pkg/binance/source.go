package binance

import (
	"context"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/etl"
	"github.com/eslider/go-binance-fdp/pkg/gapfill/hourplan"
	"github.com/eslider/go-binance-fdp/pkg/integrity"
)

// Source implements etl.BulkLoader and etl.LiveSeries for Binance.
type Source struct {
	consumer *HistoryConsumer
}

// NewSource wraps a history consumer as an ETL source.
func NewSource(consumer *HistoryConsumer) *Source {
	return &Source{consumer: consumer}
}

func (s *Source) Source() etl.Source { return etl.SourceBinance }

func (s *Source) DownloadAndTransform(ctx context.Context, job etl.Job) (<-chan etl.Progress, <-chan error) {
	asset := AssetFromJob(job)
	infoCh := make(chan etl.Progress)
	errCh := make(chan error, 1)
	go func() {
		defer close(infoCh)
		defer close(errCh)
		binanceInfo, binanceErr := s.consumer.DownloadAndTransform(asset)
		for binanceInfo != nil || binanceErr != nil {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			case pi, ok := <-binanceInfo:
				if !ok {
					binanceInfo = nil
					continue
				}
				infoCh <- etl.Progress{
					Status: assetETLStatusString(pi.Status),
					Path:   pi.Path,
					Info:   pi.Info,
					Err:    pi.Err,
				}
			case err, ok := <-binanceErr:
				if !ok {
					binanceErr = nil
					continue
				}
				errCh <- err
				return
			}
		}
	}()
	return infoCh, errCh
}

func (s *Source) RunHourPlan(ctx context.Context, job etl.Job, plan []hourplan.Step) error {
	asset := AssetFromJob(job)
	midnight := time.Date(job.Date.Year(), job.Date.Month(), job.Date.Day(), 0, 0, 0, 0, time.UTC)
	for _, step := range plan {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		switch step.Op {
		case hourplan.OpRefreshOpen:
			if err := s.consumer.RefreshLastHour(asset); err != nil {
				return err
			}
		case hourplan.OpSealCompleted:
			if _, err := s.consumer.loadOrSealHour(asset, step.Hour, midnight); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Source) BuildAuditTargets(job etl.Job, from, to time.Time) ([]integrity.HourlyTarget, error) {
	asset := AssetFromJob(job)
	return BuildAuditTargetsForRange(asset, from, to), nil
}

// assetETLStatusString converts ETL status to string for progress.
func assetETLStatusString(st ETLStatus) string {
	return []string{
		"error",
		"zip-downloading",
		"zip-reading",
		"zip-ready",
		"csv-reading",
		"parquet-ready",
	}[st]
}
