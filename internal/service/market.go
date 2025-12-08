package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"sync-v3/internal/domain"
	"sync-v3/pkg/binance"
)

type MarketService struct {
	repo            domain.MarketRepository
	historyConsumer *binance.HistoryConsumer
}

func NewMarketService(repo domain.MarketRepository, consumer *binance.HistoryConsumer) *MarketService {
	return &MarketService{
		repo:            repo,
		historyConsumer: consumer,
	}
}

func (s *MarketService) GetMarketHistory(ctx context.Context, req domain.MarketDataRequest) ([]*domain.Candle, error) {
	// 1. Ensure data is available (ETL)
	if err := s.ensureDataAvailable(ctx, req); err != nil {
		slog.Error("Failed to ensure data availability", "error", err)
		// We might still want to return partial data, so we don't return error here immediately
		// or we can return error if strict consistency is required.
		// For now, let's log and proceed to query what we have.
	}

	// 2. Query data
	return s.repo.GetCandles(ctx, req)
}

func (s *MarketService) ensureDataAvailable(ctx context.Context, req domain.MarketDataRequest) error {
	var wg sync.WaitGroup
	var errs []error
	var errMu sync.Mutex

	fromTime := req.From
	toTime := req.To

	// Loop between dates - download/transform
	for cur := fromTime; !cur.After(toTime); cur = cur.AddDate(0, 0, 1) {
		asset := &binance.HistoryAsset{
			MarketType: binance.NewMarketType(req.MarketType.String()),
			Frequency:  binance.Daily,
			Frame:      binance.NewFrame(req.Frame.String()),
			Indicator:  binance.Klines,
			Date:       cur,
			Market:     req.Market,
		}

		wg.Add(1)
		go func(asset *binance.HistoryAsset) {
			defer wg.Done()

			// Download and transform
			infoCh, errCh := s.historyConsumer.DownloadAndTransform(asset)

			for done := false; !done; {
				select {
				case _, ok := <-infoCh:
					if !ok {
						done = true
					}
				case err, ok := <-errCh:
					if ok {
						errMu.Lock()
						errs = append(errs, fmt.Errorf("date %s: %w", asset.Date.Format("2006-01-02"), err))
						errMu.Unlock()
					} else {
						done = true
					}
				case <-ctx.Done():
					done = true
				}
			}
		}(asset)
	}
	wg.Wait()

	if len(errs) > 0 {
		return fmt.Errorf("ETL errors: %v", errs)
	}
	return nil
}

func (s *MarketService) GetMarkets(ctx context.Context) ([]string, error) {
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

func (s *MarketService) GetSymbols(ctx context.Context) ([]string, error) {
	registry, err := binance.NewExchangeRegistry()
	if err != nil {
		return nil, err
	}
	var res []string
	for _, s := range registry.Symbols {
		res = append(res, s.Name)
	}
	return res, nil
}
