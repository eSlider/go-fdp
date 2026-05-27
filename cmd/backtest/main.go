// Command backtest runs MACD+RSI+SMA strategy with triple-barrier labels on FDP/Binance candles.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	trade "github.com/eslider/go-trade"

	"github.com/eslider/go-fdp/pkg/backtest"
	"github.com/eslider/go-fdp/pkg/features"
	"github.com/eslider/go-fdp/pkg/label"
	binancemd "github.com/eslider/go-fdp/pkg/marketdata/binance"
	"github.com/eslider/go-fdp/pkg/marketdata/fdp"
)

func main() {
	market := flag.String("market", "BTCUSDT", "symbol")
	frame := flag.String("frame", "1h", "candle frame")
	days := flag.Int("days", 90, "history length in days")
	fdpURL := flag.String("fdp-url", "http://127.0.0.1:8082", "go-fdp base URL (empty = Binance REST)")
	polyFrame := flag.String("poly-frame", "15m", "Polymarket frame for predictions")
	folds := flag.Int("folds", 2, "walk-forward folds (>=2)")
	embargo := flag.Int("embargo", 24, "purge embargo bars")
	flag.Parse()

	to := time.Now().UTC()
	from := to.Add(-time.Duration(*days) * 24 * time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	candles, err := loadCandles(ctx, *fdpURL, *market, *frame, from, to)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}

	var preds []fdp.Prediction
	if strings.TrimSpace(*fdpURL) != "" {
		client := fdp.NewClient(*fdpURL)
		preds, _ = client.FetchPredictions(ctx, *market, *polyFrame, from, to)
	}

	cfg := backtest.DefaultConfig()
	if *folds >= 2 {
		results, err := backtest.RunWalkForward(candles, preds, cfg, *folds, *embargo)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		printWalkForward(results)
	} else {
		res, err := backtest.Run(candles, preds, cfg)
		if err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		printResult(res)
	}

	printLabelStats(candles, cfg)
}

func loadCandles(ctx context.Context, fdpURL, market, frame string, from, to time.Time) ([]trade.Candle, error) {
	if strings.TrimSpace(fdpURL) != "" {
		client := fdp.NewClient(fdpURL)
		candles, err := client.FetchCandles(ctx, market, frame, from, to)
		if err == nil && len(candles) > 0 {
			return candles, nil
		}
	}
	return binancemd.LoadCandles(ctx, market, frame, from, to)
}

func printResult(r backtest.Result) {
	fmt.Printf("initial_equity=%.2f final_equity=%.2f return_pct=%.2f sharpe=%.3f max_dd_pct=%.2f trades=%d win_rate=%.1f%%\n",
		r.InitialEquity, r.FinalEquity, r.ReturnPct, r.Sharpe, r.MaxDrawdown, r.Trades, r.WinRate)
}

func printWalkForward(results []backtest.Result) {
	if len(results) == 0 {
		fmt.Println("no walk-forward folds produced results")
		return
	}
	var sumRet, sumSharpe, sumDD float64
	for i, r := range results {
		fmt.Printf("fold_%d return_pct=%.2f sharpe=%.3f max_dd_pct=%.2f trades=%d\n",
			i+1, r.ReturnPct, r.Sharpe, r.MaxDrawdown, r.Trades)
		sumRet += r.ReturnPct
		sumSharpe += r.Sharpe
		sumDD += r.MaxDrawdown
	}
	n := float64(len(results))
	fmt.Printf("avg return_pct=%.2f sharpe=%.3f max_dd_pct=%.2f folds=%d\n",
		sumRet/n, sumSharpe/n, sumDD/n, len(results))
}

func printLabelStats(candles []trade.Candle, cfg backtest.Config) {
	ohlc := features.OHLCFromCandles(candles)
	atr := features.ATR(ohlc.High, ohlc.Low, ohlc.Close, 14)
	labels := label.LabelSeries(ohlc.High, ohlc.Low, ohlc.Close, atr, cfg.Label)
	var up, down, none int
	for _, l := range labels {
		switch l {
		case label.OutcomeUp:
			up++
		case label.OutcomeDown:
			down++
		default:
			none++
		}
	}
	fmt.Printf("triple_barrier labels up=%d down=%d timeout=%d\n", up, down, none)
}

func ptr[T any](v T) *T { return &v }
