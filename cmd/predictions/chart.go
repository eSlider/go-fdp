package main

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/NimbleMarkets/ntcharts/v2/linechart"
	"github.com/NimbleMarkets/ntcharts/v2/linechart/timeserieslinechart"
	zone "github.com/lrstanley/bubblezone/v2"
	"github.com/eslider/go-fdp/pkg/binance"
	"charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

const maxHistoryPoints = 120

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

type historyPoint struct {
	At             time.Time
	UpProbPct      float64
	ProbDiffPct    float64
	CurrentDiffPct float64
}

type frameHistory map[string][]historyPoint

func (h frameHistory) appendPoint(frame string, p historyPoint) {
	pts := h[frame]
	pts = append(pts, p)
	if len(pts) > maxHistoryPoints {
		pts = pts[len(pts)-maxHistoryPoints:]
	}
	h[frame] = pts
}

type chartMetric int

const (
	metricUpProb chartMetric = iota
	metricProbDiff
	metricCurrentDiff
)

func (m chartMetric) label() string {
	switch m {
	case metricProbDiff:
		return "prob Δ (up−down) %"
	case metricCurrentDiff:
		return "BTC Δ vs window start %"
	default:
		return "up probability %"
	}
}

func (p historyPoint) value(m chartMetric) float64 {
	switch m {
	case metricProbDiff:
		return p.ProbDiffPct
	case metricCurrentDiff:
		return p.CurrentDiffPct
	default:
		return p.UpProbPct
	}
}

var (
	chartBorderStyle = lipgloss.NewStyle().
				BorderStyle(lipgloss.NormalBorder()).
				BorderForeground(lipgloss.Color("63"))
	chartAxisStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	chartLabelStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("6"))
	chartBullStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("10"))
	chartBearStyle  = lipgloss.NewStyle().Foreground(lipgloss.Color("9"))
)

func defaultYRange(metric chartMetric) (float64, float64) {
	switch metric {
	case metricProbDiff, metricCurrentDiff:
		return -5, 5
	default:
		return 0, 100
	}
}

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
	m.DrawCandle(dsOpen, dsHigh, dsLow, dsClose, chartBullStyle, chartBearStyle)
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

func newTimeseriesChart(w, h int, metric chartMetric, zm *zone.Manager) timeserieslinechart.Model {
	minY, maxY := defaultYRange(metric)
	return timeserieslinechart.New(w, h,
		timeserieslinechart.WithYRange(minY, maxY),
		timeserieslinechart.WithAxesStyles(chartAxisStyle, chartLabelStyle),
		timeserieslinechart.WithXLabelFormatter(timeserieslinechart.HourTimeLabelFormatter()),
		timeserieslinechart.WithZoneManager(zm),
		timeserieslinechart.WithUpdateHandler(linechart.XYAxesUpdateHandler(chartZoomXSec, chartZoomYVal)),
	)
}

// candleOHLC builds open/high/low/close from successive scalar samples (for ntcharts DrawCandle).
func candleOHLC(pts []historyPoint, metric chartMetric) (open, high, low, close []timeserieslinechart.TimePoint) {
	var prev float64
	havePrev := false
	for _, p := range pts {
		v := p.value(metric)
		if math.IsNaN(v) || math.IsInf(v, 0) {
			continue
		}
		o := v
		if havePrev {
			o = prev
		}
		c := v
		w := math.Max(0.05, math.Abs(v)*0.003)
		hi := math.Max(o, c) + w
		lo := math.Min(o, c) - w
		t := p.At
		open = append(open, timeserieslinechart.TimePoint{Time: t, Value: o})
		high = append(high, timeserieslinechart.TimePoint{Time: t, Value: hi})
		low = append(low, timeserieslinechart.TimePoint{Time: t, Value: lo})
		close = append(close, timeserieslinechart.TimePoint{Time: t, Value: c})
		prev = v
		havePrev = true
	}
	return open, high, low, close
}

func syncCandleChart(m *timeserieslinechart.Model, pts []historyPoint, metric chartMetric) {
	m.ClearAllData()
	if len(pts) == 0 {
		m.Clear()
		m.DrawXYAxisAndLabel()
		return
	}

	minY, maxY := defaultYRange(metric)
	for _, p := range pts {
		v := p.value(metric)
		if !math.IsNaN(v) && !math.IsInf(v, 0) {
			if v < minY {
				minY = v
			}
			if v > maxY {
				maxY = v
			}
		}
	}
	pad := math.Max(0.5, (maxY-minY)*0.1)
	if minY == maxY {
		pad = 1
	}
	m.SetYRange(minY-pad, maxY+pad)
	m.SetViewYRange(minY-pad, maxY+pad)

	o, h, l, c := candleOHLC(pts, metric)
	for i := range o {
		if o[i].Time.IsZero() {
			continue
		}
		m.PushDataSet(dsOpen, o[i])
		m.PushDataSet(dsHigh, h[i])
		m.PushDataSet(dsLow, l[i])
		m.PushDataSet(dsClose, c[i])
	}
	m.DrawCandle(dsOpen, dsHigh, dsLow, dsClose, chartBullStyle, chartBearStyle)
}

func chartBlur(c *timeserieslinechart.Model) {
	c.Model.Blur()
}

func chartFocus(c *timeserieslinechart.Model) {
	c.Model.Focus()
}

type chartBoard struct {
	charts  map[string]timeserieslinechart.Model
	zone    *zone.Manager
	focused string
	metric  chartMetric
	width   int
	height  int
}

func newChartBoard(metric chartMetric, zm *zone.Manager) chartBoard {
	return chartBoard{
		charts: make(map[string]timeserieslinechart.Model),
		zone:   zm,
		metric: metric,
	}
}

func (b *chartBoard) resize(w, h int) {
	if w == b.width && h == b.height {
		return
	}
	b.width = w
	b.height = h
	b.charts = make(map[string]timeserieslinechart.Model)
	b.focused = ""
}

func (b *chartBoard) chartFor(frame string) timeserieslinechart.Model {
	if c, ok := b.charts[frame]; ok {
		return c
	}
	c := newTimeseriesChart(b.width, b.height, b.metric, b.zone)
	b.charts[frame] = c
	return c
}

func (b *chartBoard) sync(hist frameHistory, frames []string) {
	for _, frame := range frames {
		c := b.chartFor(frame)
		syncCandleChart(&c, hist[frame], b.metric)
		if frame == b.focused {
			chartFocus(&c)
		}
		b.charts[frame] = c
	}
}

// handleMouse forwards wheel/click to the chart under the cursor (bubblezone + ntcharts).
func (b *chartBoard) handleMouse(msg tea.Msg, hist frameHistory, frames []string) bool {
	mouse, ok := msg.(tea.MouseMsg)
	if !ok {
		return false
	}

	var target string
	for _, frame := range frames {
		c, ok := b.charts[frame]
		if !ok {
			continue
		}
		zid := c.ZoneID()
		if zid == "" {
			continue
		}
		if z := b.zone.Get(zid); z != nil && z.InBounds(mouse) {
			target = frame
			break
		}
	}
	if target == "" {
		return false
	}

	for frame, c := range b.charts {
		if frame == target {
			chartFocus(&c)
			b.focused = frame
		} else {
			chartBlur(&c)
		}
		b.charts[frame] = c
	}

	c := b.charts[target]
	updated, _ := c.Update(msg)
	redrawKlineCandles(&updated)
	chartFocus(&updated)
	b.charts[target] = updated
	return true
}

func (b *chartBoard) Scan(content string) string {
	return b.zone.Scan(content)
}

func (b *chartBoard) render(frames []string) string {
	if b.width < 12 || b.height < 4 {
		return "terminal too small for charts"
	}
	var parts []string
	parts = append(parts, fmt.Sprintf("history · %s · candlesticks (wheel zoom)", b.metric.label()))
	for _, frame := range frames {
		c, ok := b.charts[frame]
		if !ok {
			parts = append(parts, frame+": waiting for data…")
			continue
		}
		title := lipgloss.NewStyle().Bold(true).Render(frame)
		if frame == b.focused {
			title += lipgloss.NewStyle().Foreground(lipgloss.Color("205")).Render(" ◆")
		}
		body := chartBorderStyle.Width(b.width + 2).Render(c.View())
		parts = append(parts, lipgloss.JoinVertical(lipgloss.Left, title, body))
	}
	return strings.Join(parts, "\n\n")
}
