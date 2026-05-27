package main

import (
	"os"
	"time"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/colorprofile"
	"github.com/eslider/go-fdp/pkg/data"
	"github.com/eslider/go-fdp/pkg/polymarket"
	zone "github.com/lrstanley/bubblezone/v2"
)

type fetchMsg struct {
	rows []row
	at   time.Time
	err  error
}

func frameStrings(frames []data.Frame) []string {
	out := make([]string, len(frames))
	for i, f := range frames {
		out[i] = f.String()
	}
	return out
}

type chartsTickMsg struct{}

func runTUI(cfg appConfig, collector *polymarket.Collector) error {
	env := append(os.Environ(), "CLICOLOR_FORCE=1")
	m := newRootModel(cfg, collector)
	p := tea.NewProgram(m,
		tea.WithEnvironment(env),
		tea.WithColorProfile(colorprofile.ANSI),
	)
	_, err := p.Run()
	return err
}

type rootModel struct {
	cfg       appConfig
	collector *polymarket.Collector
	zone      *zone.Manager
	tab       tabID
	preds     predictionsModel
	charts    chartsModel
	width     int
	height    int
}

func newRootModel(cfg appConfig, collector *polymarket.Collector) rootModel {
	zm := zone.New()
	return rootModel{
		cfg:       cfg,
		collector: collector,
		zone:      zm,
		tab:       tabPredictions,
		preds:     newPredictionsModel(cfg, collector),
		charts:    newChartsModel(cfg, zm),
	}
}

func (m rootModel) Init() tea.Cmd {
	return tea.Batch(
		m.preds.Init(),
		m.preds.tickCmd(),
	)
}

func (m rootModel) layoutContentSize() (int, int) {
	return contentSize(m.width, m.height)
}

func (m *rootModel) switchTab(t tabID) tea.Cmd {
	if !isValidTab(t) || m.tab == t {
		return nil
	}
	m.tab = t
	w, h := m.layoutContentSize()
	if t == tabCharts {
		m.charts.resize(w, h)
		m.charts.SetFocused(true)
		return tea.Batch(m.charts.fetchCmd(), m.charts.tickCmd())
	}
	m.charts.SetFocused(false)
	m.preds.resize(w, h)
	return nil
}

func (m rootModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	var cmds []tea.Cmd

	switch msg := msg.(type) {
	case tea.CapabilityMsg, tea.ColorProfileMsg:
		// Keep fixed ANSI profile; TrueColor/Ascii upgrades break candle colors in tmux.
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "esc", "ctrl+c":
			return m, tea.Quit
		case "tab":
			if m.tab == tabCharts {
				if !m.charts.IsFocused() {
					m.charts.SetFocused(true)
					break
				}
				if cmd := m.switchTab(tabPredictions); cmd != nil {
					cmds = append(cmds, cmd)
				}
				break
			}
			next := tabCharts
			if cmd := m.switchTab(next); cmd != nil {
				cmds = append(cmds, cmd)
			}
		default:
			if m.tab == tabPredictions {
				updated, cmd := m.preds.Update(msg)
				m.preds = updated
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
			} else {
				updated, cmd := m.charts.Update(msg)
				m.charts = updated
				if cmd != nil {
					cmds = append(cmds, cmd)
				}
			}
		}
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		w, h := m.layoutContentSize()
		m.preds.resize(w, h)
		m.charts.resize(w, h)
		if m.tab == tabCharts && m.charts.loading && len(m.charts.klines) == 0 {
			cmds = append(cmds, m.charts.fetchCmd())
		}
	case tea.MouseClickMsg:
		if t := tabFromMouse(m.zone, msg); isValidTab(t) {
			if cmd := m.switchTab(t); cmd != nil {
				cmds = append(cmds, cmd)
			}
		} else if m.tab == tabCharts {
			updated, handled, cmd := m.charts.HandleMouse(msg)
			m.charts = updated
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
			_ = handled
		}
	case tea.MouseMotionMsg, tea.MouseWheelMsg, tea.MouseReleaseMsg:
		if m.tab == tabCharts {
			updated, _, cmd := m.charts.HandleMouse(msg)
			m.charts = updated
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
		}
	case chartDragTickMsg:
		updated, cmd := m.charts.Update(msg)
		m.charts = updated
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	case predictionsTickMsg:
		if m.tab == tabPredictions {
			updated, cmd := m.preds.Update(msg)
			m.preds = updated
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
		}
	case chartsTickMsg:
		if m.tab == tabCharts {
			updated, cmd := m.charts.Update(msg)
			m.charts = updated
			if cmd != nil {
				cmds = append(cmds, cmd)
			}
		}
	case fetchMsg:
		updated, cmd := m.preds.Update(msg)
		m.preds = updated
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	case binanceKlinesMsg, chartLiveMsg:
		updated, cmd := m.charts.Update(msg)
		m.charts = updated
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	if len(cmds) == 0 {
		return m, nil
	}
	return m, tea.Batch(cmds...)
}

func (m rootModel) statusText() string {
	if m.tab == tabCharts {
		return m.charts.StatusText()
	}
	return m.preds.StatusText()
}

func (m rootModel) View() tea.View {
	if m.width == 0 {
		return tea.NewView("initializing…")
	}

	w, h := m.layoutContentSize()
	var body string
	if m.tab == tabCharts {
		body = m.charts.View(w)
	} else {
		body = m.preds.View(w)
	}

	nav := renderNav(m.tab, m.zone)
	status := renderStatusBar(m.statusText(), m.width)
	content := lipgloss.JoinVertical(lipgloss.Left, nav, body, status)
	content = m.zone.Scan(lipgloss.NewStyle().Width(m.width).Render(content))

	v := tea.NewView(content)
	v.AltScreen = true
	v.MouseMode = tea.MouseModeCellMotion
	_ = h
	return v
}
