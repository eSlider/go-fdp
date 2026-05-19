package binance

import (
	"context"
	"fmt"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/eslider/go-binance-fdp/pkg/etl"
	"github.com/eslider/go-binance-fdp/pkg/integrity"
)

// RepairKlineGaps re-seals or re-downloads data for hours/days with integrity issues.
func (s *HistoryConsumer) RepairKlineGaps(ctx context.Context, asset *HistoryAsset, targets []integrity.HourlyTarget, issues []integrity.Issue) error {
	if len(issues) == 0 {
		return nil
	}
	affected := integrity.TargetsForIssues(targets, issues)
	now := time.Now().UTC()
	currentHour := now.Hour()

	for _, t := range affected {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if t.Hour >= 0 && data.IsToday(t.Midnight) {
			if t.Hour == currentHour {
				if err := s.RefreshLastHour(asset); err != nil {
					return fmt.Errorf("refresh hour %d: %w", t.Hour, err)
				}
				continue
			}
			if _, err := s.loadOrSealHour(asset, t.Hour, t.Midnight); err != nil {
				return fmt.Errorf("seal hour %d: %w", t.Hour, err)
			}
			continue
		}
		dayAsset := *asset
		dayAsset.Date = t.Midnight
		infoCh, errCh := s.DownloadAndTransform(&dayAsset)
		if err := etl.DrainETL(ctx, infoCh, errCh); err != nil {
			return fmt.Errorf("etl %s: %w", t.Midnight.Format("2006-01-02"), err)
		}
	}
	return nil
}
