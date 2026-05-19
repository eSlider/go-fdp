package market

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/eslider/go-fdp/internal/store"
	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/etl"
	"github.com/eslider/go-fdp/pkg/gapfill"
	"golang.org/x/sync/errgroup"
)

// API is the application layer for market data HTTP use cases.
type API struct {
	Store    *store.Store
	ETL      *etl.Router
	Repair   *gapfill.Repairer
	Consumer *binance.HistoryConsumer
}

// NewAPI wires store, ETL router, and lazy gap repair.
func NewAPI(st *store.Store, router *etl.Router, consumer *binance.HistoryConsumer, db *sql.DB) *API {
	return &API{
		Store:    st,
		ETL:      router,
		Consumer: consumer,
		Repair:   gapfill.NewRepairer(db, router, consumer, 4),
	}
}

// Candles returns historical klines for the query range (lazy ETL + gap repair).
func (a *API) Candles(ctx context.Context, q Query) ([]*Candle, error) {
	if err := a.ensureBulkForRange(ctx, q); err != nil {
		slog.Error("ensure bulk ETL", "error", err)
	}
	if q.Indicator == Klines {
		job := q.ETLJob(q.To.UTC())
		if err := a.Repair.EnsureForQuery(ctx, job, q.From, q.To); err != nil {
			return nil, err
		}
	}
	return a.Store.GetCandles(ctx, q)
}

// AggTrades returns aggregate trades for the query range.
func (a *API) AggTrades(ctx context.Context, q Query) ([]*AggTrade, error) {
	if q.IsToday() {
		return a.fetchAggTradesFromAPI(ctx, q)
	}
	if err := a.ensureBulkForRange(ctx, q); err != nil {
		slog.Error("ensure bulk ETL", "error", err)
	}
	return a.Store.GetAggTrades(ctx, q)
}

func (a *API) fetchAggTradesFromAPI(ctx context.Context, q Query) ([]*AggTrade, error) {
	trades, err := binance.FetchAggTrades(ctx, &binance.AggTradeRequest{
		Base: binance.SymbolRequest{
			Symbol:    q.Market,
			StartTime: new(q.From.UnixMilli()),
			EndTime:   new(q.To.UnixMilli()),
		},
		Limit: 1000,
	})
	if err != nil {
		return nil, fmt.Errorf("fetch aggTrades: %w", err)
	}
	result := make([]*AggTrade, len(trades))
	for i, t := range trades {
		result[i] = &AggTrade{
			ID:           t.AggTradeID,
			Price:        t.Price,
			Quantity:     t.Quantity,
			FirstTradeID: t.FirstTradeID,
			LastTradeID:  t.LastTradeID,
			Time:         time.UnixMilli(t.Timestamp),
			IsBuyerMaker: t.IsBuyerMaker,
		}
	}
	return result, nil
}

func (a *API) ensureBulkForRange(ctx context.Context, q Query) error {
	g, ctx := errgroup.WithContext(ctx)
	for cur := q.From.UTC(); !cur.After(q.To.UTC()); cur = cur.AddDate(0, 0, 1) {
		cur := cur
		job := q.ETLJob(cur)
		g.Go(func() error {
			infoCh, errCh, err := a.ETL.Bulk(ctx, job)
			if err != nil {
				return err
			}
			if err := etl.DrainETL(ctx, infoCh, errCh); err != nil {
				return fmt.Errorf("etl %s: %w", cur.Format("2006-01-02"), err)
			}
			return nil
		})
	}
	return g.Wait()
}

// Markets lists known markets.
func (a *API) Markets(ctx context.Context) ([]string, error) {
	registry, err := binance.NewExchangeRegistry()
	if err != nil {
		return nil, err
	}
	var res []string
	for _, m := range registry.Markets {
		res = append(res, m.Name)
	}
	return res, nil
}

// Symbols lists tradable symbols.
func (a *API) Symbols(ctx context.Context) ([]string, error) {
	registry, err := binance.NewExchangeRegistry()
	if err != nil {
		return nil, err
	}
	var res []string
	for _, sym := range registry.Symbols {
		res = append(res, sym.Name)
	}
	return res, nil
}
