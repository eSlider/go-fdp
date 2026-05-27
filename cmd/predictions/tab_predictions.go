package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/eslider/go-fdp/pkg/polymarket"
)

const (
	minRefreshInterval = 2 * time.Second
	maxRefreshInterval = 2 * time.Minute
)

type predictionsTickMsg struct{}

type predictionsModel struct {
	cfg             appConfig
	collector       *polymarket.Collector
	rows            []row
	status          string
	lastAt          time.Time
	refreshInterval time.Duration
	fetching        bool
	width           int
	height          int
}

func newPredictionsModel(cfg appConfig, collector *polymarket.Collector) predictionsModel {
	interval := cfg.refresh
	if interval < minRefreshInterval {
		interval = minRefreshInterval
	}
	return predictionsModel{
		cfg:             cfg,
		collector:       collector,
		status:          "loading…",
		refreshInterval: interval,
		fetching:        true,
	}
}

func (m *predictionsModel) Init() tea.Cmd {
	return m.fetchCmd()
}

func (m *predictionsModel) fetchCmd() tea.Cmd {
	cfg := m.cfg
	collector := m.collector
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
		defer cancel()
		rows, err := loadRows(ctx, cfg, collector)
		return fetchMsg{rows: rows, at: time.Now().UTC(), err: err}
	}
}

func (m *predictionsModel) tickCmd() tea.Cmd {
	return tea.Tick(m.refreshInterval, func(time.Time) tea.Msg { return predictionsTickMsg{} })
}

func (m predictionsModel) StatusText() string {
	return fmt.Sprintf("%s · refresh %s", m.status, m.refreshInterval.Round(time.Second))
}

func (m *predictionsModel) adjustRefresh(delta time.Duration) {
	next := m.refreshInterval + delta
	if next < minRefreshInterval {
		next = minRefreshInterval
	}
	if next > maxRefreshInterval {
		next = maxRefreshInterval
	}
	m.refreshInterval = next
}

func (m *predictionsModel) resize(w, h int) {
	m.width, m.height = w, h
}

func (m predictionsModel) Update(msg tea.Msg) (predictionsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "r":
			if m.fetching {
				return m, nil
			}
			m.fetching = true
			m.status = "refreshing…"
			return m, m.fetchCmd()
		case "+", "=":
			m.adjustRefresh(2 * time.Second)
			m.status = fmt.Sprintf("refresh interval %s", m.refreshInterval.Round(time.Second))
			return m, m.tickCmd()
		case "-", "_":
			m.adjustRefresh(-2 * time.Second)
			m.status = fmt.Sprintf("refresh interval %s", m.refreshInterval.Round(time.Second))
			return m, m.tickCmd()
		}
	case predictionsTickMsg:
		if m.fetching {
			return m, m.tickCmd()
		}
		m.fetching = true
		return m, tea.Batch(m.fetchCmd(), m.tickCmd())
	case fetchMsg:
		m.fetching = false
		m.lastAt = msg.at
		if msg.err != nil {
			m.status = "error: " + shortErr(msg.err)
			return m, nil
		}
		m.rows = msg.rows
		m.status = predictionsStatus(m.cfg.frames, m.rows, msg.at, nil)
	}
	return m, nil
}

func predictionMethodology() string {
	return strings.TrimSpace(`
Columns
  frame              Polymarket window (5m / 15m / 4h native slug).
  prob               Market-implied Up probability (Up price × 100). Green = Up favored; red = Down.
  window_start_utc   Window open time (UTC).
  start_btc          Binance 1m candle open at window_start (reference strike).
  current_diff       start_btc − spot BTC now (USD and % of start).
  target_btc         Strike implied by Up price under a log-normal move:
                       target = start × exp(z × σ)
                     z = Φ⁻¹(up_price); σ = stdev of log returns over the last 100 bars of the
                     window frame interval ending at window_start (Binance klines).
  target_diff        start_btc − target_btc (USD and %).
  window_end_utc     Window close time (UTC).
  window_end_in_mins Minutes until window_end (negative = closed).

Data: Polymarket snapshots + Binance REST (1m open, frame klines for σ, latest 1m close).
`)
}

func (m predictionsModel) View(width int) string {
	if width < 1 {
		width = 80
	}
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	annoStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("252")).Italic(true)

	header := titleStyle.Render(fmt.Sprintf(
		"Polymarket · %s · %s",
		m.cfg.binanceMarket,
		strings.Join(frameStrings(m.cfg.frames), ", "),
	))
	help := helpStyle.Render(fmt.Sprintf(
		"r refresh now · +/- interval (now %s) · tab charts",
		m.refreshInterval.Round(time.Second),
	))
	annotation := annoStyle.Render(predictionMethodology())
	tableBody := renderTableString(displayRows(m.cfg.frames, m.rows))
	return lipgloss.JoinVertical(lipgloss.Left, header, help, "", annotation, "", tableBody)
}
