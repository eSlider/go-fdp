package main

import (
	zone "github.com/lrstanley/bubblezone/v2"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

const (
	navBarLines    = 1
	statusBarLines = 1
	layoutPadLines = 2
)

const (
	zoneTabPredictions = "tab-predictions"
	zoneTabCharts      = "tab-charts"
)

type tabID int

const (
	tabPredictions tabID = iota
	tabCharts
)

var (
	tabActiveStyle = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("205")).
			Background(lipgloss.Color("235")).
			Padding(0, 2)
	tabIdleStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("243")).
			Padding(0, 2)
	statusBarStyle = lipgloss.NewStyle().
			Foreground(lipgloss.Color("252")).
			Background(lipgloss.Color("236")).
			Padding(0, 1)
)

func contentSize(totalW, totalH int) (w, h int) {
	w = totalW - 2
	if w < 20 {
		w = 20
	}
	h = totalH - navBarLines - statusBarLines - layoutPadLines
	if h < 4 {
		h = 4
	}
	return w, h
}

func renderNav(active tabID, zm *zone.Manager) string {
	p := tabActiveStyle.Render(" Predictions ")
	c := tabIdleStyle.Render(" Charts ")
	if active == tabCharts {
		p = tabIdleStyle.Render(" Predictions ")
		c = tabActiveStyle.Render(" Charts ")
	}
	if zm != nil {
		p = zm.Mark(zoneTabPredictions, p)
		c = zm.Mark(zoneTabCharts, c)
	}
	return lipgloss.JoinHorizontal(lipgloss.Top, p, c)
}

func tabFromMouse(zm *zone.Manager, msg tea.MouseMsg) tabID {
	if zm == nil {
		return tabPredictions
	}
	if z := zm.Get(zoneTabCharts); z != nil && z.InBounds(msg) {
		return tabCharts
	}
	if z := zm.Get(zoneTabPredictions); z != nil && z.InBounds(msg) {
		return tabPredictions
	}
	return tabID(-1)
}

func isValidTab(t tabID) bool {
	return t == tabPredictions || t == tabCharts
}

func renderStatusBar(text string, width int) string {
	if width < 1 {
		width = 1
	}
	return statusBarStyle.Width(width).Render(truncateStatus(text, width))
}

func truncateStatus(s string, width int) string {
	if lipgloss.Width(s) <= width {
		return s
	}
	for len(s) > 0 && lipgloss.Width(s) > width-1 {
		s = s[:len(s)-1]
	}
	return s + "…"
}
