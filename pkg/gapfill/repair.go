package gapfill

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/etl"
	"github.com/eslider/go-fdp/pkg/gapfill/hourplan"
	"github.com/eslider/go-fdp/pkg/integrity"
	"golang.org/x/sync/errgroup"
)

// Repairer performs lazy gap repair for a requested time range.
type Repairer struct {
	DB       *sql.DB
	Router   *etl.Router
	Consumer *binance.HistoryConsumer
	sem      chan struct{}
}

// NewRepairer creates a repairer with bounded per-request day parallelism.
func NewRepairer(db *sql.DB, router *etl.Router, consumer *binance.HistoryConsumer, maxParallelDays int) *Repairer {
	if maxParallelDays <= 0 {
		maxParallelDays = 4
	}
	return &Repairer{
		DB:       db,
		Router:   router,
		Consumer: consumer,
		sem:      make(chan struct{}, maxParallelDays),
	}
}

// EnsureForQuery audits [from, to] with count-only checks, repairs mismatches, re-audits.
func (r *Repairer) EnsureForQuery(ctx context.Context, job etl.Job, from, to time.Time) error {
	if job.Indicator != string(binance.Klines) {
		return nil
	}
	live, err := r.Router.Live(job)
	if err != nil {
		return err
	}
	targets, err := live.BuildAuditTargets(job, from, to)
	if err != nil {
		return err
	}
	if len(targets) == 0 {
		return nil
	}
	frameMs := int64(time.Duration(job.Frame) / time.Millisecond)
	issues, _, err := integrity.AuditHourlyTargets(ctx, r.DB, targets, frameMs)
	if err != nil {
		return fmt.Errorf("audit klines: %w", err)
	}
	if !integrity.HasGapIssues(issues) {
		return nil
	}
	affected := integrity.TargetsForIssues(targets, issues)
	if err := r.repairTargets(ctx, job, live, affected, issues); err != nil {
		return err
	}
	issues, _, err = integrity.AuditHourlyTargets(ctx, r.DB, targets, frameMs)
	if err != nil {
		return fmt.Errorf("re-audit klines: %w", err)
	}
	rangeIssues := integrity.IssuesAffectingRange(issues, from, to)
	if integrity.HasBlockingGaps(rangeIssues) {
		return fmt.Errorf("klines still have gaps after repair: %s", integrity.FormatIssues(rangeIssues))
	}
	return nil
}

func (r *Repairer) repairTargets(ctx context.Context, job etl.Job, live etl.LiveSeries, targets []integrity.HourlyTarget, issues []integrity.Issue) error {
	byDay := groupTargetsByDay(targets)
	g, ctx := errgroup.WithContext(ctx)
	var mu sync.Mutex
	var firstErr error
	for midnight, dayTargets := range byDay {
		midnight := midnight
		dayTargets := dayTargets
		dayIssues := filterIssuesForDay(issues, dayTargets)
		g.Go(func() error {
			select {
			case r.sem <- struct{}{}:
				defer func() { <-r.sem }()
			case <-ctx.Done():
				return ctx.Err()
			}
			if err := r.repairDay(ctx, job, live, midnight, dayTargets, dayIssues); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return err
			}
			return nil
		})
	}
	_ = g.Wait()
	return firstErr
}

func (r *Repairer) repairDay(ctx context.Context, job etl.Job, live etl.LiveSeries, midnight time.Time, targets []integrity.HourlyTarget, issues []integrity.Issue) error {
	dayJob := job
	dayJob.Date = midnight
	asset := binance.AssetFromJob(dayJob)
	if data.IsToday(midnight) {
		now := time.Now().UTC()
		fromH, toH := 0, now.Hour()
		for _, t := range targets {
			if t.Hour < fromH {
				fromH = t.Hour
			}
			if t.Hour > toH {
				toH = t.Hour
			}
		}
		plan := hourplan.PlanHours(midnight, fromH, toH, now.Hour())
		return live.RunHourPlan(ctx, dayJob, plan)
	}
	return r.Consumer.RepairKlineGaps(ctx, asset, targets, issues)
}

func groupTargetsByDay(targets []integrity.HourlyTarget) map[time.Time][]integrity.HourlyTarget {
	out := make(map[time.Time][]integrity.HourlyTarget)
	for _, t := range targets {
		midnight := time.Date(t.Midnight.Year(), t.Midnight.Month(), t.Midnight.Day(), 0, 0, 0, 0, time.UTC)
		out[midnight] = append(out[midnight], t)
	}
	return out
}

func filterIssuesForDay(issues []integrity.Issue, targets []integrity.HourlyTarget) []integrity.Issue {
	if len(issues) == 0 || len(targets) == 0 {
		return issues
	}
	out := make([]integrity.Issue, 0)
	for _, iss := range issues {
		for _, t := range targets {
			if iss.AffectsTarget(t) {
				out = append(out, iss)
				break
			}
		}
	}
	return out
}
