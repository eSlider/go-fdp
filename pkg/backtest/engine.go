package backtest

import (
	"fmt"
	"time"

	trade "github.com/eslider/go-trade"

	"github.com/eslider/go-fdp/pkg/agent"
	"github.com/eslider/go-fdp/pkg/features"
	"github.com/eslider/go-fdp/pkg/label"
	"github.com/eslider/go-fdp/pkg/marketdata/fdp"
	"github.com/eslider/go-fdp/pkg/paper"
	"github.com/eslider/go-fdp/pkg/strategy"
)

// Config drives a single backtest run.
type Config struct {
	FeeRate      float64
	SlippageBps  float64
	Label        label.Config
	Base         strategy.BaseConfig
	Meta         strategy.MetaConfig
	MinPolyUp    float64
	RiskPct      float64 // fraction of equity risked per ATR unit
	TPMult       float64
	SLMult       float64
}

// DefaultConfig returns plan defaults.
func DefaultConfig() Config {
	lc := label.DefaultConfig()
	return Config{
		FeeRate:     0.001,
		SlippageBps: 5,
		Label:       lc,
		Base:        strategy.DefaultBaseConfig(),
		Meta:        strategy.DefaultMetaConfig(),
		MinPolyUp:   0.45,
		RiskPct:     0.01,
		TPMult:      lc.TPMult,
		SLMult:      lc.SLMult,
	}
}

// Result holds backtest metrics.
type Result struct {
	InitialEquity float64
	FinalEquity   float64
	ReturnPct     float64
	Sharpe        float64
	MaxDrawdown   float64
	Trades        int
	WinRate       float64
}

// Run executes agent+strategy backtest on candles with optional predictions.
func Run(candles []trade.Candle, preds []fdp.Prediction, cfg Config) (Result, error) {
	if len(candles) < 250 {
		return Result{}, fmt.Errorf("need at least 250 candles, got %d", len(candles))
	}
	ohlc := features.OHLCFromCandles(candles)
	atr := features.ATR(ohlc.High, ohlc.Low, ohlc.Close, 14)

	broker := paper.NewBroker(paper.Config{
		FeeRate:      cfg.FeeRate,
		SlippageBps:  cfg.SlippageBps,
		InitialCash:  100_000,
		MaxDailyLoss: 0.05,
		MaxExposure:  1,
	}, "BTCUSDT")

	baseSigs, feats, err := strategy.BaseSignals(candles, cfg.Base)
	if err != nil {
		return Result{}, err
	}
	sigs := strategy.MetaFilter(baseSigs, feats, cfg.Meta)
	times := make([]time.Time, len(candles))
	for i, c := range candles {
		times[i] = c.TimeClose
	}
	sigs = strategy.PolyFilter(sigs, times, preds, cfg.MinPolyUp)

	sigSet := make(map[int]strategy.Side, len(sigs))
	for _, s := range sigs {
		sigSet[s.Index] = s.Side
	}

	pipe := agent.DefaultPipeline()
	equityCurve := make([]float64, 0, len(candles))
	var wins, trades int
	initial := broker.Equity(candles[0].Close)

	for i := 250; i < len(candles); i++ {
		c := candles[i]
		broker.RollDay(c.TimeClose, c.Close)
		if broker.KillSwitch(c.Close) {
			_, _ = broker.CloseLong(c.TimeClose, c.Close)
		}
		closed, _, _ := broker.CheckBarriers(c.TimeClose, c.High, c.Low)
		if closed {
			trades++
		}

		pipe = updateRiskAgent(pipe, broker.DailyLossPct(c.Close), broker.OpenExposurePct(c.Close))

		ctx := agent.Context{
			Index:       i,
			Candles:     candles,
			Features:    feats,
			Predictions: preds,
			Spot:        c.Close,
		}
		dec := pipe.Decide(ctx)

		if side, ok := sigSet[i]; ok && side == strategy.SideLong && dec.Action == agent.ActionBuy {
			if broker.Position == nil && !broker.KillSwitch(c.Close) {
				eq := broker.Equity(c.Close)
				atrv := atr[i]
				if atrv > 0 && eq > 0 {
					riskUSD := eq * cfg.RiskPct
					qty := riskUSD / (cfg.SLMult * atrv)
					if qty* c.Close > eq {
						qty = eq / c.Close * 0.99
					}
					if err := broker.OpenLong(c.TimeClose, c.Close, qty, atrv, cfg.TPMult, cfg.SLMult); err == nil {
						trades++
					}
				}
			}
		}
		if dec.Action == agent.ActionSell && broker.Position != nil {
			_, _ = broker.CloseLong(c.TimeClose, c.Close)
			trades++
		}
		equityCurve = append(equityCurve, broker.Equity(c.Close))
	}

	final := broker.Equity(candles[len(candles)-1].Close)
	ret := (final - initial) / initial * 100
	sharpe := sharpeFromCurve(equityCurve)
	mdd := maxDrawdownPct(equityCurve)

	for _, f := range broker.Fills {
		if f.PnL > 0 {
			wins++
		}
	}
	wr := 0.0
	if trades > 0 {
		wr = float64(wins) / float64(trades) * 100
	}

	return Result{
		InitialEquity: initial,
		FinalEquity:   final,
		ReturnPct:     ret,
		Sharpe:        sharpe,
		MaxDrawdown:   mdd,
		Trades:        trades,
		WinRate:       wr,
	}, nil
}

// RunWalkForward runs purged walk-forward folds and averages return.
func RunWalkForward(candles []trade.Candle, preds []fdp.Prediction, cfg Config, folds, embargo int) ([]Result, error) {
	n := len(candles)
	if folds < 2 {
		r, err := Run(candles, preds, cfg)
		return []Result{r}, err
	}
	var results []Result
	for f := 0; f < folds; f++ {
		_, testIdx := label.PurgeEmbargo(n, f, folds, embargo)
		if len(testIdx) < 50 {
			continue
		}
		sub := make([]trade.Candle, len(testIdx))
		for j, idx := range testIdx {
			sub[j] = candles[idx]
		}
		r, err := Run(sub, preds, cfg)
		if err != nil {
			return results, err
		}
		results = append(results, r)
	}
	return results, nil
}

func updateRiskAgent(pipe agent.Pipeline, dailyLoss, exposure float64) agent.Pipeline {
	for i, a := range pipe.Agents {
		if r, ok := a.(agent.RiskAgent); ok {
			r.Limits.DailyLossPct = dailyLoss
			r.Limits.OpenExposurePct = exposure
			pipe.Agents[i] = r
		}
	}
	return pipe
}

func maxDrawdownPct(curve []float64) float64 {
	if len(curve) == 0 {
		return 0
	}
	peak := curve[0]
	var mdd float64
	for _, v := range curve {
		if v > peak {
			peak = v
		}
		if peak > 0 {
			dd := (peak - v) / peak
			if dd > mdd {
				mdd = dd
			}
		}
	}
	return mdd * 100
}
