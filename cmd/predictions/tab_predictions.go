package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/eslider/go-fdp/pkg/polymarket"
	zone "github.com/lrstanley/bubblezone/v2"
)

type predictionsModel struct {
	cfg       appConfig
	collector *polymarket.Collector
	zone      *zone.Manager
	rows      []row
	history   frameHistory
	charts    chartBoard
	metric    chartMetric
	status    string
	lastAt    time.Time
	width     int
	height    int
}

func newPredictionsModel(cfg appConfig, collector *polymarket.Collector, zm *zone.Manager) predictionsModel {
	return predictionsModel{
		cfg:       cfg,
		collector: collector,
		zone:      zm,
		history:   make(frameHistory),
		charts:    newChartBoard(metricUpProb, zm),
		metric:    metricUpProb,
		status:    "loading…",
	}
}

func (m *predictionsModel) Init() tea.Cmd {
	return m.fetchCmd()
}

func (m *predictionsModel) fetchCmd() tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), m.cfg.timeout)
		defer cancel()
		rows, err := loadRows(ctx, m.cfg, m.collector)
		return fetchMsg{rows: rows, at: time.Now().UTC(), err: err}
	}
}

func (m *predictionsModel) tickCmd() tea.Cmd {
	return tea.Tick(m.cfg.refresh, func(time.Time) tea.Msg { return tickMsg{} })
}

func (m predictionsModel) StatusText() string {
	return m.status
}

func (m *predictionsModel) recordHistory(rows []row, at time.Time) {
	for _, r := range rows {
		m.history.appendPoint(r.Frame, historyPoint{
			At:             at,
			UpProbPct:      r.UpProbPct,
			ProbDiffPct:    r.ProbDiffPct,
			CurrentDiffPct: r.CurrentDiffPct,
		})
	}
}

func (m predictionsModel) frameOrder() []string {
	out := make([]string, 0, len(m.cfg.frames))
	for _, f := range m.cfg.frames {
		if polymarket.HasNativeSlug(f) {
			out = append(out, f.String())
		}
	}
	return out
}

func (m *predictionsModel) resize(w, h int) {
	m.width, m.height = w, h
	m.refreshCharts()
}

func (m *predictionsModel) refreshCharts() {
	plotW, plotH := m.predPlotSize()
	m.charts.metric = m.metric
	m.charts.resize(plotW, plotH)
	m.charts.sync(m.history, m.frameOrder())
}

func (m predictionsModel) predPlotSize() (int, int) {
	plotW := m.width - 4
	if plotW < 24 {
		plotW = 24
	}
	frames := nativeFrameCount(m.cfg.frames)
	if frames == 0 {
		frames = 1
	}
	plotH := 6
	rest := m.height - 8
	if rest > 0 {
		budget := rest / frames
		if budget > plotH {
			plotH = budget
		}
		if plotH > 10 {
			plotH = 10
		}
	}
	return plotW, plotH
}

func (m predictionsModel) Update(msg tea.Msg) (predictionsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		switch msg.String() {
		case "r":
			m.status = "refreshing…"
			return m, m.fetchCmd()
		case "1":
			m.metric = metricUpProb
			m.refreshCharts()
		case "2":
			m.metric = metricProbDiff
			m.refreshCharts()
		case "3":
			m.metric = metricCurrentDiff
			m.refreshCharts()
		}
	case fetchMsg:
		m.lastAt = msg.at
		if msg.err != nil {
			m.status = "error: " + shortErr(msg.err)
			return m, nil
		}
		m.rows = msg.rows
		m.recordHistory(msg.rows, msg.at)
		m.refreshCharts()
		n := historySampleCount(m.history)
		if len(m.rows) == 0 {
			m.status = "no native frames"
		} else {
			m.status = fmt.Sprintf("updated %s UTC · %d samples", msg.at.Format("15:04:05"), n)
		}
	}
	return m, nil
}

func (m predictionsModel) HandleMouse(msg tea.Msg) bool {
	return m.charts.handleMouse(msg, m.history, m.frameOrder())
}

func (m predictionsModel) View(width int) string {
	if width < 1 {
		width = 80
	}
	titleStyle := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("205"))
	helpStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("243"))

	tableBody := renderTableString(displayRows(m.cfg.frames, m.rows))
	charts := m.charts.render(m.frameOrder())
	header := titleStyle.Render(fmt.Sprintf(
		"Polymarket · %s · %s",
		m.cfg.binanceMarket,
		strings.Join(frameStrings(m.cfg.frames), ", "),
	))
	help := helpStyle.Render("r refresh · 1/2/3 metric · wheel zoom · drag pan")
	return lipgloss.JoinVertical(lipgloss.Left, header, help, tableBody, "", charts)
}
