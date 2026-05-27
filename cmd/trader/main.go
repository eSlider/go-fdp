// Command trader runs the multi-agent paper-trading loop against go-fdp (or Binance REST).
package main

import (
	"context"
	"flag"
	"log/slog"
	"os/signal"
	"strings"
	"syscall"
	"time"

	trade "github.com/eslider/go-trade"

	"github.com/eslider/go-fdp/pkg/agent"
	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/features"
	"github.com/eslider/go-fdp/pkg/marketdata/fdp"
	"github.com/eslider/go-fdp/pkg/paper"
	"github.com/eslider/go-fdp/pkg/strategy"
)

func main() {
	market := flag.String("market", "BTCUSDT", "symbol")
	frame := flag.String("frame", "1h", "signal timeframe")
	fdpURL := flag.String("fdp-url", "http://127.0.0.1:8082", "go-fdp base URL")
	polyFrame := flag.String("poly-frame", "15m", "Polymarket frame")
	poll := flag.Duration("poll", time.Minute, "poll interval")
	lookback := flag.Int("lookback", 300, "candles to load")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	broker := paper.NewBroker(paper.DefaultConfig(), *market)
	pipe := agent.DefaultPipeline()
	baseCfg := strategy.DefaultBaseConfig()

	slog.Info("trader started", "market", *market, "frame", *frame, "paper", true)

	ticker := time.NewTicker(*poll)
	defer ticker.Stop()

	runOnce := func() {
		candles, preds, err := fetchState(ctx, *fdpURL, *market, *frame, *polyFrame, *lookback)
		if err != nil {
			slog.Error("fetch state", "error", err)
			return
		}
		if len(candles) < 250 {
			slog.Warn("insufficient candles", "n", len(candles))
			return
		}
		feats, err := strategy.ComputeFeatures(candles, baseCfg)
		if err != nil {
			slog.Error("features", "error", err)
			return
		}
		i := len(candles) - 1
		c := candles[i]
		broker.RollDay(c.TimeClose, c.Close)

		pipe = updateRisk(pipe, broker, c.Close)
		dec := pipe.Decide(agent.Context{
			Index:       i,
			Candles:     candles,
			Features:    feats,
			Predictions: preds,
			Spot:        c.Close,
		})

		ohlc := features.OHLCFromCandles(candles)
		atr := features.ATR(ohlc.High, ohlc.Low, ohlc.Close, 14)
		atrv := atr[i]

		if broker.KillSwitch(c.Close) {
			slog.Warn("kill switch active")
			_, _ = broker.CloseLong(c.TimeClose, c.Close)
			return
		}

		closed, pnl, _ := broker.CheckBarriers(c.TimeClose, c.High, c.Low)
		if closed {
			slog.Info("barrier exit", "pnl", pnl)
		}

		switch dec.Action {
		case agent.ActionBuy:
			if broker.Position == nil && atrv > 0 {
				eq := broker.Equity(c.Close)
				qty := eq * 0.01 / (1.5 * atrv)
				if err := broker.OpenLong(c.TimeClose, c.Close, qty, atrv, 2, 1.5); err != nil {
					slog.Error("open long", "error", err)
				} else {
					slog.Info("open long", "qty", qty, "price", c.Close, "reason", dec.Reason)
				}
			}
		case agent.ActionSell:
			if broker.Position != nil {
				pnl, _ := broker.CloseLong(c.TimeClose, c.Close)
				slog.Info("close long", "pnl", pnl, "reason", dec.Reason)
			}
		default:
			slog.Debug("hold", "reason", dec.Reason)
		}
		slog.Info("equity", "usd", broker.Equity(c.Close), "action", dec.Action.String())
	}

	runOnce()
	for {
		select {
		case <-ctx.Done():
			slog.Info("trader stopped")
			return
		case <-ticker.C:
			runOnce()
		}
	}
}

func fetchState(ctx context.Context, fdpURL, market, frame, polyFrame string, lookback int) ([]trade.Candle, []fdp.Prediction, error) {
	to := time.Now().UTC()
	from := to.Add(-time.Duration(lookback) * time.Hour)

	var candles []trade.Candle
	var preds []fdp.Prediction
	var err error

	if strings.TrimSpace(fdpURL) != "" {
		client := fdp.NewClient(fdpURL)
		candles, err = client.FetchCandles(ctx, market, frame, from, to)
		if err == nil {
			preds, _ = client.FetchPredictions(ctx, market, polyFrame, from, to)
		}
	}
	if len(candles) == 0 {
		klines, kerr := binance.FetchKlines(ctx, &binance.KlineRequest{
			Base:     binance.SymbolRequest{Symbol: market},
			Interval: frame,
			Limit:    int64(lookback),
		})
		if kerr != nil {
			return nil, nil, kerr
		}
		candles = features.CandlesFromBinance(klines)
	}
	return candles, preds, err
}

func updateRisk(pipe agent.Pipeline, broker *paper.Broker, mark float64) agent.Pipeline {
	for i, a := range pipe.Agents {
		if r, ok := a.(agent.RiskAgent); ok {
			r.Limits.DailyLossPct = broker.DailyLossPct(mark)
			r.Limits.OpenExposurePct = broker.OpenExposurePct(mark)
			pipe.Agents[i] = r
		}
	}
	return pipe
}
