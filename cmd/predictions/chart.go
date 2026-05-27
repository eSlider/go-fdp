package main

import (
	"math"
	"time"

	"charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/NimbleMarkets/ntcharts/v2/linechart"
	"github.com/NimbleMarkets/ntcharts/v2/linechart/timeserieslinechart"
	"github.com/eslider/go-fdp/pkg/binance"
	zone "github.com/lrstanley/bubblezone/v2"
)

// Wheel zoom steps: X = seconds on time axis, Y = value units (percent points).
const chartZoomXSec = 45
const chartZoomYVal = 1.5

// chartFrameSwitchEmptyBars: when adjacent visible candles are farther apart, switch frame instead of zoom.
const chartFrameSwitchEmptyBars = 5

const (
	dsOpen  = "open"
	dsHigh  = "high"
	dsLow   = "low"
	dsClose = "close"
)

// Raw SGR (not 256/truecolor): green up, red down. Transform embeds codes so tmux/ASCII
// profile cannot strip lipgloss ForegroundColor alone.
const (
	ansiCandleUp    = "\x1b[32m"
	ansiCandleDown  = "\x1b[31m"
	ansiCandleReset = "\x1b[0m"
)

func chartANSIStyle(color string) lipgloss.Style {
	return lipgloss.NewStyle().Transform(func(s string) string {
		if s == "" {
			return s
		}
		return color + s + ansiCandleReset
	})
}

var (
	chartBorderStyle = lipgloss.NewStyle().
				BorderStyle(lipgloss.NormalBorder()).
				BorderForeground(lipgloss.Color("8"))
	chartAxisStyle       = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	chartLabelStyle      = lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	chartCandleUpStyle   = chartANSIStyle(ansiCandleUp)
	chartCandleDownStyle = chartANSIStyle(ansiCandleDown)
)

// chartKeyboardHandler handles keys and wheel only; X-axis drag is implemented in tab_charts.
func chartKeyboardHandler(panSec float64) linechart.UpdateHandler {
	return func(m *linechart.Model, tm tea.Msg) {
		switch msg := tm.(type) {
		case tea.KeyMsg:
			switch msg.Key().Code {
			case tea.KeyLeft:
				m.MoveLeft(panSec)
			case tea.KeyRight:
				m.MoveRight(panSec)
			default:
				switch msg.String() {
				case "left", "h":
					m.MoveLeft(panSec)
				case "right", "l":
					m.MoveRight(panSec)
				}
			}
		}
	}
}

// maxEmptyBarsInView returns the largest number of missing bar slots between consecutive
// candles visible in the current X viewport (0 when bars are adjacent).
func maxEmptyBarsInView(klines []*binance.Kline, viewMinX, viewMaxX, barSec float64) int {
	if len(klines) < 2 || barSec <= 0 || viewMaxX <= viewMinX {
		return 0
	}
	maxGap := 0
	var prev *binance.Kline
	for _, k := range klines {
		if k == nil {
			continue
		}
		x := float64(k.OpenTime) / 1e3
		if x < viewMinX || x > viewMaxX {
			continue
		}
		if prev != nil {
			gapSec := x - float64(prev.OpenTime)/1e3
			empty := int(math.Round(gapSec/barSec)) - 1
			if empty > maxGap {
				maxGap = empty
			}
		}
		prev = k
	}
	return maxGap
}

// resyncChartView rescales datasets after the viewport changes (required before redraw).
func resyncChartView(m *timeserieslinechart.Model) {
	vmin, vmax := m.ViewMinX(), m.ViewMaxX()
	if vmax <= vmin {
		return
	}
	m.SetViewTimeRange(
		time.UnixMilli(int64(math.Round(vmin*1000))),
		time.UnixMilli(int64(math.Round(vmax*1000))),
	)
}

func chartZoomX(m *timeserieslinechart.Model, zoomIn bool, stepSec float64) {
	if stepSec < 1 {
		stepSec = 60
	}
	if zoomIn {
		m.Model.ZoomIn(stepSec, 0)
	} else {
		m.Model.ZoomOut(stepSec, 0)
	}
	resyncChartView(m)
}

func newBinanceChart(w, h int, panSec float64, zm *zone.Manager) timeserieslinechart.Model {
	if panSec < 1 {
		panSec = 60
	}
	c := timeserieslinechart.New(w, h,
		timeserieslinechart.WithAxesStyles(chartAxisStyle, chartLabelStyle),
		timeserieslinechart.WithXLabelFormatter(timeserieslinechart.HourTimeLabelFormatter()),
		timeserieslinechart.WithZoneManager(zm),
		timeserieslinechart.WithUpdateHandler(chartKeyboardHandler(panSec)),
	)
	// Keep viewport under our control; auto X would expand view to all candles on each push.
	c.AutoMinX = false
	c.AutoMaxX = false
	return c
}

// panChartViewByDX shifts the viewport horizontally based on pixel delta from drag start.
// originMinX/originMaxX are the view range when the drag started.
func panChartViewByDX(m *timeserieslinechart.Model, dx int, originMinX, originMaxX float64) {
	if dx == 0 || originMaxX <= originMinX {
		return
	}
	gw := float64(m.GraphWidth())
	if gw < 1 {
		gw = 1
	}
	span := originMaxX - originMinX
	shift := -float64(dx) * span / gw

	newMin := originMinX + shift
	newMax := originMaxX + shift
	dataMin := m.MinX()
	dataMax := m.MaxX()

	if newMin < dataMin {
		newMax += dataMin - newMin
		newMin = dataMin
	}
	if newMax > dataMax {
		newMin -= newMax - dataMax
		newMax = dataMax
	}
	if newMin < dataMin {
		newMin = dataMin
	}
	if newMax > dataMax {
		newMax = dataMax
	}
	if newMax <= newMin {
		return
	}
	m.SetViewTimeRange(
		time.UnixMilli(int64(math.Round(newMin*1000))),
		time.UnixMilli(int64(math.Round(newMax*1000))),
	)
}

func redrawKlineCandles(m *timeserieslinechart.Model) {
	m.DrawCandle(dsOpen, dsHigh, dsLow, dsClose, chartCandleUpStyle, chartCandleDownStyle)
}

func klineTimeRange(klines []*binance.Kline) (time.Time, time.Time) {
	first := klines[0]
	last := klines[len(klines)-1]
	return time.UnixMilli(first.OpenTime).UTC(), time.UnixMilli(last.CloseTime).UTC()
}

type klineChartOpts struct {
	preserveView bool
	scrollToEnd  bool
	barDuration  time.Duration
}

func klineYRange(klines []*binance.Kline) (minY, maxY float64) {
	minY, maxY = klines[0].LowPrice, klines[0].HighPrice
	for _, k := range klines {
		if k.LowPrice < minY {
			minY = k.LowPrice
		}
		if k.HighPrice > maxY {
			maxY = k.HighPrice
		}
	}
	return minY, maxY
}

func pushKlineDataSets(m *timeserieslinechart.Model, klines []*binance.Kline) {
	for _, k := range klines {
		t := time.UnixMilli(k.OpenTime).UTC()
		m.PushDataSet(dsOpen, timeserieslinechart.TimePoint{Time: t, Value: k.OpenPrice})
		m.PushDataSet(dsHigh, timeserieslinechart.TimePoint{Time: t, Value: k.HighPrice})
		m.PushDataSet(dsLow, timeserieslinechart.TimePoint{Time: t, Value: k.LowPrice})
		m.PushDataSet(dsClose, timeserieslinechart.TimePoint{Time: t, Value: k.ClosePrice})
	}
}

func chartAtLiveEdge(m *timeserieslinechart.Model) bool {
	if m.MaxX() <= m.MinX() {
		return true
	}
	margin := (m.MaxX() - m.MinX()) * 0.02
	return m.ViewMaxX() >= m.MaxX()-margin
}

func scrollKlineViewToEnd(m *timeserieslinechart.Model, klines []*binance.Kline, barDuration time.Duration) {
	if len(klines) == 0 {
		return
	}
	const visibleBars = 72
	last := klines[len(klines)-1]
	end := float64(last.CloseTime) / 1e3
	barSec := barDuration.Seconds()
	if barSec < 1 {
		barSec = 60
	}
	start := end - visibleBars*barSec
	minX := float64(klines[0].OpenTime) / 1e3
	if start < minX {
		start = minX
	}
	m.SetViewTimeRange(
		time.UnixMilli(int64(math.Round(start*1000))),
		time.UnixMilli(int64(math.Round(end*1000))),
	)
}

func syncKlineChart(m *timeserieslinechart.Model, klines []*binance.Kline, opts klineChartOpts) {
	if len(klines) == 0 {
		m.ClearAllData()
		m.Clear()
		m.DrawXYAxisAndLabel()
		return
	}

	minY, maxY := klineYRange(klines)
	pad := math.Max((maxY-minY)*0.05, 1)
	yMin, yMax := minY-pad, maxY+pad

	var (
		viewMinX, viewMaxX, viewMinY, viewMaxY float64
		keepView                               bool
	)
	if opts.preserveView && m.MaxX() > m.MinX() {
		viewMinX = m.ViewMinX()
		viewMaxX = m.ViewMaxX()
		viewMinY = m.ViewMinY()
		viewMaxY = m.ViewMaxY()
		keepView = true
	}

	minT, maxT := klineTimeRange(klines)
	m.ClearAllData()
	m.SetTimeRange(minT, maxT)
	m.SetYRange(yMin, yMax)
	if !keepView {
		m.SetViewYRange(yMin, yMax)
	}
	pushKlineDataSets(m, klines)
	redrawKlineCandles(m)

	if keepView {
		m.SetViewYRange(viewMinY, viewMaxY)
		m.SetViewTimeRange(
			time.UnixMilli(int64(math.Round(viewMinX*1000))),
			time.UnixMilli(int64(math.Round(viewMaxX*1000))),
		)
		redrawKlineCandles(m)
	} else if opts.scrollToEnd {
		scrollKlineViewToEnd(m, klines, opts.barDuration)
		redrawKlineCandles(m)
	}
}

func chartBlur(c *timeserieslinechart.Model) {
	c.Model.Blur()
}

func chartFocus(c *timeserieslinechart.Model) {
	c.Model.Focus()
}
