package paper

import (
	"fmt"
	"time"

	trade "github.com/eslider/go-trade"
)

// Config holds fees, slippage, and risk kill switches.
type Config struct {
	FeeRate       float64 // per side, e.g. 0.001
	SlippageBps   float64 // e.g. 5 = 0.05%
	InitialCash   float64
	MaxDailyLoss  float64 // fraction of equity, 0.05 = 5%
	MaxExposure   float64 // max position notional / equity
}

// DefaultConfig returns Binance-spot-like defaults.
func DefaultConfig() Config {
	return Config{
		FeeRate:      0.001,
		SlippageBps:  5,
		InitialCash:  100_000,
		MaxDailyLoss: 0.05,
		MaxExposure:  1,
	}
}

// Broker simulates spot paper trading with go-trade Market identity.
type Broker struct {
	Config   Config
	Market   trade.Market
	Cash     float64
	Position *Position
	Fills    []Fill
	dayStart time.Time
	dayStartEquity float64
}

// NewBroker creates a paper broker for the given spot ticker (e.g. BTCUSDT).
func NewBroker(cfg Config, ticker string) *Broker {
	if cfg.InitialCash <= 0 {
		cfg.InitialCash = 100_000
	}
	mkt, err := trade.ParseSpotTicker(ticker)
	if err != nil {
		mkt = trade.Market{FromSymbol: "BTC", ToSymbol: "USDT", Title: ticker}
	}
	return &Broker{
		Config: cfg,
		Market: mkt,
		Cash:   cfg.InitialCash,
	}
}

// Equity returns cash plus marked-to-market position.
func (b *Broker) Equity(mark float64) float64 {
	eq := b.Cash
	if b.Position != nil && b.Position.Side != SideFlat {
		switch b.Position.Side {
		case SideLong:
			eq += b.Position.Qty * mark
		case SideShort:
			eq += b.Position.Qty * (2*b.Position.EntryPx - mark)
		}
	}
	return eq
}

// DailyLossPct returns today's loss as fraction of day-start equity.
func (b *Broker) DailyLossPct(mark float64) float64 {
	if b.dayStartEquity <= 0 {
		return 0
	}
	eq := b.Equity(mark)
	if eq >= b.dayStartEquity {
		return 0
	}
	return (b.dayStartEquity - eq) / b.dayStartEquity
}

// OpenExposurePct returns position notional / equity.
func (b *Broker) OpenExposurePct(mark float64) float64 {
	eq := b.Equity(mark)
	if eq <= 0 || b.Position == nil || b.Position.Side == SideFlat {
		return 0
	}
	return b.Position.Qty * mark / eq
}

// RollDay resets daily PnL tracking at UTC midnight boundaries.
func (b *Broker) RollDay(t time.Time, mark float64) {
	day := t.UTC().Truncate(24 * time.Hour)
	if b.dayStart.IsZero() || !day.Equal(b.dayStart) {
		b.dayStart = day
		b.dayStartEquity = b.Equity(mark)
	}
}

// KillSwitch is true when daily loss or exposure limits are hit.
func (b *Broker) KillSwitch(mark float64) bool {
	if b.Config.MaxDailyLoss > 0 && b.DailyLossPct(mark) >= b.Config.MaxDailyLoss {
		return true
	}
	if b.Config.MaxExposure > 0 && b.OpenExposurePct(mark) >= b.Config.MaxExposure {
		return true
	}
	return false
}

// OpenLong opens or adds to a long at mark with ATR-based stops.
func (b *Broker) OpenLong(t time.Time, mark, qty, atr, tpMult, slMult float64) error {
	if qty <= 0 {
		return fmt.Errorf("qty must be positive")
	}
	px := b.applySlippage(mark, SideLong)
	cost := px * qty
	fee := cost * b.Config.FeeRate
	if cost+fee > b.Cash {
		return fmt.Errorf("insufficient cash: need %.2f have %.2f", cost+fee, b.Cash)
	}
	b.Cash -= cost + fee
	sl := px - slMult*atr
	tp := px + tpMult*atr
	if b.Position != nil && b.Position.Side == SideLong {
		total := b.Position.Qty + qty
		b.Position.EntryPx = (b.Position.EntryPx*b.Position.Qty + px*qty) / total
		b.Position.Qty = total
	} else {
		b.Position = &Position{
			Market:     b.Market,
			Side:       SideLong,
			EntryTime:  t,
			EntryPx:    px,
			Qty:        qty,
			StopLoss:   sl,
			TakeProfit: tp,
		}
	}
	b.Fills = append(b.Fills, Fill{Time: t, Market: b.Market, Side: SideLong, Price: px, Qty: qty, Fee: fee})
	return nil
}

// CloseLong closes the long at mark.
func (b *Broker) CloseLong(t time.Time, mark float64) (float64, error) {
	if b.Position == nil || b.Position.Side != SideLong {
		return 0, nil
	}
	px := b.applySlippage(mark, SideShort)
	proceeds := px * b.Position.Qty
	fee := proceeds * b.Config.FeeRate
	pnl := proceeds - fee - b.Position.EntryPx*b.Position.Qty
	b.Cash += proceeds - fee
	b.Fills = append(b.Fills, Fill{
		Time: t, Market: b.Market, Side: SideShort, Price: px,
		Qty: b.Position.Qty, Fee: fee, PnL: pnl,
	})
	b.Position = nil
	return pnl, nil
}

// CheckBarriers closes on TP/SL if hit on this bar.
func (b *Broker) CheckBarriers(t time.Time, high, low float64) (closed bool, pnl float64, err error) {
	if b.Position == nil || b.Position.Side != SideLong {
		return false, 0, nil
	}
	if high >= b.Position.TakeProfit {
		p, e := b.CloseLong(t, b.Position.TakeProfit)
		return true, p, e
	}
	if low <= b.Position.StopLoss {
		p, e := b.CloseLong(t, b.Position.StopLoss)
		return true, p, e
	}
	return false, 0, nil
}

func (b *Broker) applySlippage(mark float64, side Side) float64 {
	bps := b.Config.SlippageBps / 10000
	if side == SideLong {
		return mark * (1 + bps)
	}
	return mark * (1 - bps)
}
