---
name: go-fdp-backtest
description: >-
  Run BTC/USDT agent backtests in go-fdp using local go-trade models.
  Use when backtesting, linking go-trade, running cmd/backtest or cmd/trader,
  or reporting strategy metrics.
---

# go-fdp backtest & go-trade

## go-trade dependency

Published: `github.com/eslider/go-trade v0.2.1` (Candle volume, `ParseSpotTicker`).

For local development on `/mnt/raid/projects/go-trade`, add temporarily:

```text
replace github.com/eslider/go-trade => /mnt/raid/projects/go-trade
```

After model changes: `go test` in go-trade, tag release, `go get github.com/eslider/go-trade@vX.Y.Z` in go-fdp.

## Models (`github.com/eslider/go-trade`)

| Type | Use |
|------|-----|
| `trade.Candle` | OHLCV bars (TimeOpen, TimeClose, Volume, QuoteVolume) |
| `trade.Market` | Base/quote pair; parse via `trade.ParseSpotTicker("BTCUSDT")` |
| `trade.TimeAndSale` | AggTrade / microstructure (embedded `Sale`) |

Convert Binance klines: `features.CandlesFromBinance` or `binancemd.LoadCandles`.

## Run backtest

```bash
cd /mnt/raid/projects/sync-v3

# Binance REST (paginated)
go run -tags no_duckdb_arrow ./cmd/backtest \
  -market BTCUSDT -frame 4h -days 365 -fdp-url "" -folds 1

# With FDP cache + Polymarket predictions
go run -tags no_duckdb_arrow ./cmd/backtest \
  -market BTCUSDT -frame 1h -days 90 \
  -fdp-url http://127.0.0.1:8082 -poly-frame 15m -folds 3 -embargo 24

# Paper trader
go run -tags no_duckdb_arrow ./cmd/trader -market BTCUSDT -frame 1h -poll 1m
```

## GitHub (`gh`)

```bash
gh repo view eSlider/go-trade
gh repo view eSlider/go-fdp
# After go-trade model fixes: commit in go-trade, tag, then bump require in go-fdp
```

## Strategy stack

- Base: MACD cross + RSI [45,60] + close > SMA200 (`pkg/strategy`)
- Meta: ATR/vol filters (`MetaFilter`)
- Poly: `PolyFilter` when FDP `/v1/predictions` available
- Agents: `pkg/agent` pipeline → `pkg/paper` broker
- Labels: triple-barrier `pkg/label` (reported separately from PnL backtest)

## Report template

When reporting a backtest run, include:

1. Data source (FDP vs Binance), symbol, frame, bar count, date range
2. `return_pct`, `sharpe`, `max_dd_pct`, `trades`, `win_rate`
3. Walk-forward fold averages if `-folds >= 2`
4. Triple-barrier label counts (up / down / timeout)
5. Caveats: fees 0.1%/side, slippage 5 bps, long-only paper
