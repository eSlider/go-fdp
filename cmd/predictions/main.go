// Command predictions prints the current Polymarket BTC Up/Down prediction
// snapshot for each supported frame as a DuckDB-style ASCII table, enriched
// with the window-start Binance BTC price and signed diffs vs current/target.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"strings"
	"time"

	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/polymarket"
)

type row struct {
	Frame          string
	Prob           string
	WindowStart    string
	StartBTC       string
	CurrentDiff    string
	TargetBTC      string
	TargetDiff     string
	WindowEnd      string
	WindowEndInMin string
}

func main() {
	market := flag.String("market", polymarket.DefaultMarket, "polymarket symbol")
	binanceMarket := flag.String("binance-market", "BTCUSDT", "Binance symbol for reference price")
	frameList := flag.String("frames", "5m,15m,4h", "comma-separated native Polymarket frames (5m, 15m, 4h)")
	timeout := flag.Duration("timeout", 30*time.Second, "request timeout")
	flag.Parse()

	frames, err := parseFrames(*frameList)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	client := polymarket.NewClient()
	store := polymarket.NewStore("")
	collector := polymarket.NewCollector(client, store)

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	current, currentErr := fetchCurrentBTC(ctx, *binanceMarket)

	rows := make([]row, 0, len(frames))
	for _, f := range frames {
		if !polymarket.HasNativeSlug(f) {
			continue
		}
		if r, ok := fetchRow(ctx, collector, *market, *binanceMarket, f, current, currentErr); ok {
			rows = append(rows, r)
		}
	}

	renderTable(os.Stdout, rows)
}

func fetchRow(ctx context.Context, collector *polymarket.Collector, market, binanceMarket string, frame data.Frame, current float64, currentErr error) (row, bool) {
	snaps, err := collector.FetchCurrentSnapshot(ctx, market, frame)
	if err != nil || len(snaps) == 0 {
		return row{}, false
	}
	s := snaps[len(snaps)-1]
	r := row{Frame: frame.String()}
	r.Prob = formatUpDownProb(s.UpPrice, s.DownPrice)
	r.WindowStart = s.WindowStart.UTC().Format("2006-01-02 15:04:05")
	r.WindowEnd = s.WindowEnd.UTC().Format("2006-01-02 15:04:05")
	r.WindowEndInMin = fmt.Sprintf("%+.1f", time.Until(s.WindowEnd).Minutes())

	ref, refErr := fetchOpenAt(ctx, binanceMarket, s.WindowStart)
	if refErr == nil {
		r.StartBTC = fmt.Sprintf("%.2f", ref)
		if currentErr == nil {
			r.CurrentDiff = priceDiffFromStart(ref, current)
		}
		sigma, volErr := fetchRealizedVol(ctx, binanceMarket, frame.String(), s.WindowStart, 100)
		if volErr == nil && sigma > 0 {
			z := normInvCDF(s.UpPrice)
			target := ref * math.Exp(z*sigma)
			r.TargetBTC = fmt.Sprintf("%.2f", target)
			r.TargetDiff = priceDiffFromStart(ref, target)
		}
	}
	return r, true
}

func formatUpDownProb(up, down float64) string {
	if down > up {
		return fmt.Sprintf("Down %.2f%%", down*100)
	}
	return fmt.Sprintf("Up %.2f%%", up*100)
}

// priceDiffFromStart formats (start - other) as absolute USD and percent of start.
func priceDiffFromStart(start, other float64) string {
	if start <= 0 || math.IsNaN(other) || math.IsInf(other, 0) {
		return ""
	}
	diff := start - other
	pct := diff / start * 100
	if math.IsNaN(pct) || math.IsInf(pct, 0) {
		return ""
	}
	return fmt.Sprintf("%+.2f (%+.3f%%)", diff, pct)
}

// fetchOpenAt returns the BTC open price of the 1m kline that contains t.
func fetchOpenAt(ctx context.Context, symbol string, t time.Time) (float64, error) {
	start := t.UnixMilli()
	end := t.Add(time.Minute).UnixMilli()
	klines, err := binance.FetchKlines(ctx, &binance.KlineRequest{
		Base:     binance.SymbolRequest{Symbol: symbol, StartTime: &start, EndTime: &end},
		Interval: "1m",
		Limit:    1,
	})
	if err != nil {
		return 0, err
	}
	if len(klines) == 0 {
		return 0, fmt.Errorf("no kline at %s", t.UTC().Format(time.RFC3339))
	}
	return klines[0].OpenPrice, nil
}

// fetchRealizedVol returns the stddev of log returns over the last n klines of
// the given frame interval ending at until (exclusive). Used to scale implied move.
func fetchRealizedVol(ctx context.Context, symbol, interval string, until time.Time, n int) (float64, error) {
	end := until.UnixMilli()
	klines, err := binance.FetchKlines(ctx, &binance.KlineRequest{
		Base:     binance.SymbolRequest{Symbol: symbol, EndTime: &end},
		Interval: interval,
		Limit:    int64(n + 1),
	})
	if err != nil {
		return 0, err
	}
	if len(klines) < 3 {
		return 0, fmt.Errorf("not enough klines for vol estimate")
	}
	rets := make([]float64, 0, len(klines)-1)
	for i := 1; i < len(klines); i++ {
		prev, cur := klines[i-1].ClosePrice, klines[i].ClosePrice
		if prev > 0 && cur > 0 {
			rets = append(rets, math.Log(cur/prev))
		}
	}
	return stddev(rets), nil
}

func stddev(xs []float64) float64 {
	if len(xs) < 2 {
		return 0
	}
	var mean float64
	for _, x := range xs {
		mean += x
	}
	mean /= float64(len(xs))
	var sq float64
	for _, x := range xs {
		d := x - mean
		sq += d * d
	}
	return math.Sqrt(sq / float64(len(xs)-1))
}

// normInvCDF is the inverse of the standard normal CDF (Acklam's approximation).
func normInvCDF(p float64) float64 {
	if p <= 0 {
		return math.Inf(-1)
	}
	if p >= 1 {
		return math.Inf(1)
	}
	const (
		a1 = -3.969683028665376e+01
		a2 = 2.209460984245205e+02
		a3 = -2.759285104469687e+02
		a4 = 1.383577518672690e+02
		a5 = -3.066479806614716e+01
		a6 = 2.506628277459239e+00

		b1 = -5.447609879822406e+01
		b2 = 1.615858368580409e+02
		b3 = -1.556989798598866e+02
		b4 = 6.680131188771972e+01
		b5 = -1.328068155288572e+01

		c1 = -7.784894002430293e-03
		c2 = -3.223964580411365e-01
		c3 = -2.400758277161838e+00
		c4 = -2.549732539343734e+00
		c5 = 4.374664141464968e+00
		c6 = 2.938163982698783e+00

		d1 = 7.784695709041462e-03
		d2 = 3.224671290700398e-01
		d3 = 2.445134137142996e+00
		d4 = 3.754408661907416e+00

		pLow  = 0.02425
		pHigh = 1 - pLow
	)
	switch {
	case p < pLow:
		q := math.Sqrt(-2 * math.Log(p))
		return (((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	case p > pHigh:
		q := math.Sqrt(-2 * math.Log(1-p))
		return -(((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q + c6) /
			((((d1*q+d2)*q+d3)*q+d4)*q + 1)
	default:
		q := p - 0.5
		r := q * q
		return (((((a1*r+a2)*r+a3)*r+a4)*r+a5)*r + a6) * q /
			(((((b1*r+b2)*r+b3)*r+b4)*r+b5)*r + 1)
	}
}

// fetchCurrentBTC returns the latest 1m close price.
func fetchCurrentBTC(ctx context.Context, symbol string) (float64, error) {
	klines, err := binance.FetchKlines(ctx, &binance.KlineRequest{
		Base:     binance.SymbolRequest{Symbol: symbol},
		Interval: "1m",
		Limit:    1,
	})
	if err != nil {
		return 0, err
	}
	if len(klines) == 0 {
		return 0, fmt.Errorf("no current kline for %s", symbol)
	}
	return klines[0].ClosePrice, nil
}

func parseFrames(s string) ([]data.Frame, error) {
	parts := strings.Split(s, ",")
	out := make([]data.Frame, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		f := data.NewFrame(p)
		if f == data.NoFrame {
			return nil, fmt.Errorf("unknown frame %q", p)
		}
		out = append(out, f)
	}
	if len(out) == 0 {
		return nil, errors.New("no frames")
	}
	return out, nil
}

// renderTable prints a DuckDB-CLI-style box table with header types.
func renderTable(w *os.File, rows []row) {
	headers := []string{"frame", "prob", "window_start_utc", "start_btc", "current_diff", "target_btc", "target_diff", "window_end_utc", "window_end_in_mins"}
	cells := make([][]string, 0, len(rows))
	for _, r := range rows {
		cells = append(cells, []string{
			r.Frame, r.Prob,
			r.WindowStart, r.StartBTC, r.CurrentDiff, r.TargetBTC, r.TargetDiff, r.WindowEnd, r.WindowEndInMin,
		})
	}

	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, c := range cells {
		for i, v := range c {
			if len(v) > widths[i] {
				widths[i] = len(v)
			}
		}
	}

	fmt.Fprintln(w, line("┌", "┬", "┐", widths))
	fmt.Fprintln(w, dataLine(headers, widths))
	fmt.Fprintln(w, line("├", "┼", "┤", widths))
	for _, c := range cells {
		fmt.Fprintln(w, dataLine(c, widths))
	}
	fmt.Fprintln(w, line("└", "┴", "┘", widths))
}

func line(l, m, r string, widths []int) string {
	var b strings.Builder
	b.WriteString(l)
	for i, w := range widths {
		b.WriteString(strings.Repeat("─", w+2))
		if i < len(widths)-1 {
			b.WriteString(m)
		}
	}
	b.WriteString(r)
	return b.String()
}

func dataLine(values []string, widths []int) string {
	var b strings.Builder
	b.WriteString("│")
	for i, v := range values {
		fmt.Fprintf(&b, " %-*s ", widths[i], v)
		b.WriteString("│")
	}
	return b.String()
}
