// Command predictions shows Polymarket BTC Up/Down snapshots (TUI by default, --cli for one-shot table).
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"charm.land/lipgloss/v2"

	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/features"
	"github.com/eslider/go-fdp/pkg/polymarket"
)

var (
	probUpStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("10")) // green
	probDownStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))  // red
)

type appConfig struct {
	market        string
	binanceMarket string
	frames        []data.Frame
	timeout       time.Duration
	refresh       time.Duration
	fdpURL        string
}

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
	UpProbPct      float64
	ProbDiffPct    float64
	CurrentDiffPct float64
}

func main() {
	cli := flag.Bool("cli", false, "print table once to stdout and exit (default: interactive TUI)")
	market := flag.String("market", polymarket.DefaultMarket, "polymarket symbol")
	binanceMarket := flag.String("binance-market", "BTCUSDT", "Binance symbol for reference price")
	frameList := flag.String("frames", "5m,15m,4h", "comma-separated native Polymarket frames (5m, 15m, 4h)")
	timeout := flag.Duration("timeout", 30*time.Second, "per-request timeout")
	refresh := flag.Duration("refresh", 10*time.Second, "TUI predictions table refresh interval (overridable with +/- in UI)")
	fdpURL := flag.String("fdp-url", defaultFDPURL, "go-fdp base URL for charts tab (empty = Binance REST only)")
	flag.Parse()
	if *refresh < 2*time.Second {
		*refresh = 10 * time.Second
	}

	frames, err := parseFrames(*frameList)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(2)
	}

	cfg := appConfig{
		market:        *market,
		binanceMarket: *binanceMarket,
		frames:        frames,
		timeout:       *timeout,
		refresh:       *refresh,
		fdpURL:        strings.TrimSpace(*fdpURL),
	}

	client := polymarket.NewClient()
	store := polymarket.NewStore("")
	collector := polymarket.NewCollector(client, store)

	if *cli {
		if err := runCLI(cfg, collector); err != nil {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
		return
	}

	if err := runTUI(cfg, collector); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func runCLI(cfg appConfig, collector *polymarket.Collector) error {
	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()
	rows, err := loadRows(ctx, cfg, collector)
	if err != nil {
		return err
	}
	renderTable(os.Stdout, displayRows(cfg.frames, rows))
	return nil
}

func renderTableString(rows []row) string {
	var buf bytes.Buffer
	renderTable(&buf, rows)
	return buf.String()
}

// displayRows returns one row per configured native frame in stable order.
// Missing frames use placeholders so the table height does not change on refresh.
func displayRows(frames []data.Frame, rows []row) []row {
	byFrame := make(map[string]row, len(rows))
	for _, r := range rows {
		byFrame[r.Frame] = r
	}
	naStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("241"))
	na := naStyle.Render("—")
	out := make([]row, 0, len(frames))
	for _, f := range frames {
		if !polymarket.HasNativeSlug(f) {
			continue
		}
		name := f.String()
		if r, ok := byFrame[name]; ok {
			out = append(out, r)
			continue
		}
		out = append(out, row{
			Frame:          name,
			Prob:           na,
			WindowStart:    na,
			StartBTC:       na,
			CurrentDiff:    na,
			TargetBTC:      na,
			TargetDiff:     na,
			WindowEnd:      na,
			WindowEndInMin: na,
		})
	}
	return out
}

func nativeFrameCount(frames []data.Frame) int {
	n := 0
	for _, f := range frames {
		if polymarket.HasNativeSlug(f) {
			n++
		}
	}
	return n
}

func loadRows(ctx context.Context, cfg appConfig, collector *polymarket.Collector) ([]row, error) {
	current, currentErr := fetchCurrentBTC(ctx, cfg.binanceMarket)
	rows := make([]row, 0, len(cfg.frames))
	var fetchErrs []error
	for _, f := range cfg.frames {
		if !polymarket.HasNativeSlug(f) {
			continue
		}
		r, err := fetchRow(ctx, collector, cfg.market, cfg.binanceMarket, f, current, currentErr)
		if err != nil {
			fetchErrs = append(fetchErrs, fmt.Errorf("%s: %w", f, err))
			continue
		}
		rows = append(rows, r)
	}
	if len(rows) == 0 && len(fetchErrs) > 0 {
		return nil, errors.Join(fetchErrs...)
	}
	return rows, nil
}

func fetchRow(ctx context.Context, collector *polymarket.Collector, market, binanceMarket string, frame data.Frame, current float64, currentErr error) (row, error) {
	snaps, err := collector.FetchCurrentSnapshot(ctx, market, frame)
	if err != nil {
		return row{}, err
	}
	if len(snaps) == 0 {
		return row{}, fmt.Errorf("empty snapshot")
	}
	s := snaps[len(snaps)-1]
	r := row{Frame: frame.String()}
	r.UpProbPct = s.UpPrice * 100
	r.ProbDiffPct = (s.UpPrice - s.DownPrice) * 100
	r.Prob = formatUpDownProb(s.UpPrice, s.DownPrice)
	r.WindowStart = s.WindowStart.UTC().Format("2006-01-02 15:04:05")
	r.WindowEnd = s.WindowEnd.UTC().Format("2006-01-02 15:04:05")
	r.WindowEndInMin = fmt.Sprintf("%+.1f", time.Until(s.WindowEnd).Minutes())

	ref, refErr := fetchOpenAt(ctx, binanceMarket, s.WindowStart)
	if refErr == nil {
		r.StartBTC = fmt.Sprintf("%.2f", ref)
		if currentErr == nil {
			r.CurrentDiff = priceDiffFromStart(ref, current)
			if ref > 0 {
				r.CurrentDiffPct = (ref - current) / ref * 100
			}
		}
		sigma, volErr := fetchRealizedVol(ctx, binanceMarket, frame.String(), s.WindowStart, 100)
		if volErr == nil && sigma > 0 {
			target := features.ImpliedStrike(ref, s.UpPrice, sigma)
			r.TargetBTC = fmt.Sprintf("%.2f", target)
			r.TargetDiff = priceDiffFromStart(ref, target)
		}
	}
	return r, nil
}

func predictionsStatus(frames []data.Frame, rows []row, at time.Time, fetchErr error) string {
	if fetchErr != nil {
		return "error: " + shortErr(fetchErr)
	}
	if nativeFrameCount(frames) == 0 {
		return "no native frames"
	}
	if len(rows) == 0 {
		return "no live data"
	}
	return fmt.Sprintf("updated %s UTC", at.Format("15:04:05"))
}

func formatUpDownProb(up, down float64) string {
	if down > up {
		return probDownStyle.Render(fmt.Sprintf("Down %.2f%%", down*100))
	}
	return probUpStyle.Render(fmt.Sprintf("Up %.2f%%", up*100))
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
	closes := make([]float64, len(klines))
	for i, k := range klines {
		closes[i] = k.ClosePrice
	}
	return features.RealizedVol(closes, n), nil
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

func shortErr(err error) string {
	msg := err.Error()
	if len(msg) > 60 {
		msg = msg[:57] + "..."
	}
	return msg
}

// renderTable prints a DuckDB-CLI-style box table with header types.
func renderTable(w io.Writer, rows []row) {
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
			w := lipgloss.Width(v)
			if w > widths[i] {
				widths[i] = w
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
		pad := widths[i] - lipgloss.Width(v)
		if pad < 0 {
			pad = 0
		}
		b.WriteString(" ")
		b.WriteString(v)
		b.WriteString(strings.Repeat(" ", pad+1))
		b.WriteString("│")
	}
	return b.String()
}
