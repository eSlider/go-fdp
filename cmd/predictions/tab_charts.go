package main

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/NimbleMarkets/ntcharts/v2/linechart/timeserieslinechart"
	zone "github.com/lrstanley/bubblezone/v2"
	"github.com/eslider/go-fdp/pkg/binance"
	"github.com/eslider/go-fdp/pkg/data"
	tea "charm.land/bubbletea/v2"
	"charm.land/bubbles/v2/key"
	"charm.land/lipgloss/v2"
)

var (
	chartSources = []string{"binance"}
	chartMarkets = []string{"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}
	chartFrames  = []data.Frame{
		data.Minute,
		data.FiveMinute,
		data.FifteenMin,
		data.Hour,
		data.OneDay,
	}
)

type binanceKlinesMsg struct {
	klines []*binance.Kline
	err    error
}

type chartLiveMsg struct {
	tail  []*binance.Kline
	price float64
	at    time.Time
	err   error
}

// chartDragTickMsg drives 10ms drag-to-pan updates while the mouse button is held.
type chartDragTickMsg struct{}

const chartDragPollInterval = 10 * time.Millisecond

type chartDragState struct {
	active   bool
	startX   int
	startY   int
	lastX    int
	lastY    int
	viewMinX float64
	viewMaxX float64
}

type chartsModel struct {
	cfg       appConfig
	zone      *zone.Manager
	sourceIdx int
	marketIdx int
	frameIdx  int
	chart     timeserieslinechart.Model
	klines    []*binance.Kline
	focused   bool
	drag      chartDragState
	loading   bool
	errText   string
	lastPrice float64
	lastTime  time.Time
	width     int
	height    int
	chartW    int
	chartH    int

	restoreViewAfterLoad bool
	savedViewMin         time.Time
	savedViewMax         time.Time
}

func newChartsModel(cfg appConfig, zm *zone.Manager) chartsModel {
	return chartsModel{
		cfg:       cfg,
		zone:      zm,
		sourceIdx: 0,
		marketIdx: 0,
		frameIdx:  1, // 5m default
		loading:   true,
	}
}

func (m *chartsModel) Init() tea.Cmd {
	return m.fetchCmd()
}

func (m *chartsModel) tickCmd() tea.Cmd {
	return tea.Tick(time.Second, func(time.Time) tea.Msg { return tickMsg{} })
}

func (m chartsModel) IsFocused() bool {
	return m.focused
}

func (m *chartsModel) SetFocused(on bool) {
	m.focused = on
	if on {
		chartFocus(&m.chart)
	} else {
		chartBlur(&m.chart)
	}
}

func (m *chartsModel) clearDrag() {
	m.drag = chartDragState{}
}

func (m chartsModel) dragPollCmd() tea.Cmd {
	return tea.Tick(chartDragPollInterval, func(time.Time) tea.Msg { return chartDragTickMsg{} })
}

func (m *chartsModel) chartZonePos(mouse tea.MouseMsg) (x, y int, ok bool) {
	if m.chart.ZoneID() == "" {
		return 0, 0, false
	}
	z := m.zone.Get(m.chart.ZoneID())
	if z == nil || !z.InBounds(mouse) {
		return 0, 0, false
	}
	x, y = z.Pos(mouse)
	return x, y, true
}

func (m *chartsModel) applyDragPan() {
	if !m.drag.active {
		return
	}
	dx := m.drag.lastX - m.drag.startX
	panChartViewByDX(&m.chart, dx, m.drag.viewMinX, m.drag.viewMaxX)
	redrawKlineCandles(&m.chart)
}

func (m chartsModel) StatusText() string {
	now := time.Now().UTC().Format("2006-01-02 15:04:05")
	if m.loading {
		return fmt.Sprintf("%s · loading %s %s…", now, m.market(), m.frame().String())
	}
	if m.errText != "" {
		return fmt.Sprintf("%s · error: %s", now, m.errText)
	}
	if m.lastPrice > 0 {
		return fmt.Sprintf("%s · %s %s · %.2f", now, m.market(), m.frame().String(), m.lastPrice)
	}
	return fmt.Sprintf("%s · %s %s · no data", now, m.market(), m.frame().String())
}

func (m chartsModel) market() string {
	if m.marketIdx < 0 || m.marketIdx >= len(chartMarkets) {
		return chartMarkets[0]
	}
	return chartMarkets[m.marketIdx]
}

func (m chartsModel) frame() data.Frame {
	if m.frameIdx < 0 || m.frameIdx >= len(chartFrames) {
		return data.FiveMinute
	}
	return chartFrames[m.frameIdx]
}

func (m chartsModel) source() string {
	if m.sourceIdx < 0 || m.sourceIdx >= len(chartSources) {
		return chartSources[0]
	}
	return chartSources[m.sourceIdx]
}

func (m chartsModel) panSeconds() float64 {
	d := time.Duration(m.frame())
	if d <= 0 {
		return 60
	}
	return d.Seconds()
}

func (m chartsModel) barDuration() time.Duration {
	d := time.Duration(m.frame())
	if d <= 0 {
		return time.Minute
	}
	return d
}

func (m *chartsModel) resize(w, h int) {
	m.clearDrag()
	m.width, m.height = w, h
	m.chartW = w - 4
	if m.chartW < 24 {
		m.chartW = 24
	}
	m.chartH = h - 3 // toolbar + border
	if m.chartH < 4 {
		m.chartH = 4
	}
	preserve := len(m.klines) > 0
	m.chart = newBinanceChart(m.chartW, m.chartH, m.panSeconds(), m.zone)
	if preserve {
		syncKlineChart(&m.chart, m.klines, klineChartOpts{
			preserveView: true,
			barDuration:  m.barDuration(),
		})
	}
	if m.focused {
		chartFocus(&m.chart)
	}
}

func (m chartsModel) fetchCmd() tea.Cmd {
	symbol := m.market()
	interval := m.frame().String()
	timeout := m.cfg.timeout
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		klines, err := binance.FetchKlines(ctx, &binance.KlineRequest{
			Base:     binance.SymbolRequest{Symbol: symbol},
			Interval: interval,
			Limit:    500,
		})
		return binanceKlinesMsg{klines: klines, err: err}
	}
}

func (m chartsModel) liveCmd() tea.Cmd {
	symbol := m.market()
	interval := m.frame().String()
	timeout := m.cfg.timeout
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		klines, err := binance.FetchKlines(ctx, &binance.KlineRequest{
			Base:     binance.SymbolRequest{Symbol: symbol},
			Interval: interval,
			Limit:    2,
		})
		if err != nil {
			return chartLiveMsg{err: err}
		}
		price, err := fetchCurrentBTC(ctx, symbol)
		if err != nil && len(klines) > 0 {
			last := klines[len(klines)-1]
			price = last.ClosePrice
			err = nil
		}
		return chartLiveMsg{
			tail:  klines,
			price: price,
			at:    time.Now().UTC(),
			err:   err,
		}
	}
}

func mergeKlineTail(series []*binance.Kline, tail []*binance.Kline) []*binance.Kline {
	if len(tail) == 0 {
		return series
	}
	if len(series) == 0 {
		return tail
	}
	out := make([]*binance.Kline, len(series))
	copy(out, series)
	for _, k := range tail {
		if k == nil {
			continue
		}
		if len(out) > 0 && out[len(out)-1].OpenTime == k.OpenTime {
			out[len(out)-1] = k
			continue
		}
		if len(out) == 0 || k.OpenTime > out[len(out)-1].OpenTime {
			out = append(out, k)
		}
	}
	const maxKlines = 500
	if len(out) > maxKlines {
		out = out[len(out)-maxKlines:]
	}
	return out
}

func (m *chartsModel) reloadChart(scrollToEnd bool) {
	m.chart = newBinanceChart(m.chartW, m.chartH, m.panSeconds(), m.zone)
	if len(m.klines) == 0 {
		return
	}
	syncKlineChart(&m.chart, m.klines, klineChartOpts{
		scrollToEnd:  scrollToEnd,
		barDuration:  m.barDuration(),
	})
	if m.focused {
		chartFocus(&m.chart)
	}
}

func (m *chartsModel) applyKlines(klines []*binance.Kline) {
	m.klines = klines
	if len(klines) > 0 {
		last := klines[len(klines)-1]
		m.lastPrice = last.ClosePrice
		m.lastTime = time.UnixMilli(last.CloseTime).UTC()
	}
	scrollEnd := true
	restoreMin, restoreMax := m.savedViewMin, m.savedViewMax
	if m.restoreViewAfterLoad {
		scrollEnd = false
		m.restoreViewAfterLoad = false
	}
	m.reloadChart(scrollEnd)
	if !scrollEnd && len(klines) > 0 {
		m.chart.SetViewTimeRange(restoreMin, restoreMax)
		redrawKlineCandles(&m.chart)
	}
}

func (m *chartsModel) applyLive(msg chartLiveMsg) {
	if msg.err != nil {
		return
	}
	if msg.price > 0 {
		m.lastPrice = msg.price
		m.lastTime = msg.at
	}
	if len(msg.tail) == 0 || len(m.klines) == 0 || m.drag.active {
		return
	}
	atEdge := chartAtLiveEdge(&m.chart)
	m.klines = mergeKlineTail(m.klines, msg.tail)
	syncKlineChart(&m.chart, m.klines, klineChartOpts{
		preserveView: true,
		barDuration:  m.barDuration(),
	})
	if atEdge {
		scrollKlineViewToEnd(&m.chart, m.klines, m.barDuration())
	}
}

func (m *chartsModel) frameUp() {
	if m.frameIdx <= 0 {
		m.frameIdx = len(chartFrames) - 1
	} else {
		m.frameIdx--
	}
}

func (m *chartsModel) frameDown() {
	m.frameIdx = (m.frameIdx + 1) % len(chartFrames)
}

func isChartPanKey(msg tea.KeyMsg) bool {
	switch msg.Key().Code {
	case tea.KeyLeft, tea.KeyRight:
		return true
	}
	switch msg.String() {
	case "left", "right", "h", "l":
		return true
	}
	return false
}

func isChartFrameKey(msg tea.KeyMsg) bool {
	switch msg.Key().Code {
	case tea.KeyUp, tea.KeyDown:
		return true
	}
	switch msg.String() {
	case "up", "down", "k", "j":
		return true
	}
	return false
}

func (m chartsModel) chartKeyMsg(msg tea.KeyMsg) bool {
	if !m.focused {
		return isChartPanKey(msg) || isChartFrameKey(msg)
	}
	if isChartFrameKey(msg) || isChartPanKey(msg) {
		return true
	}
	km := m.chart.Model.Canvas.KeyMap
	return key.Matches(msg, km.Left) || key.Matches(msg, km.Right)
}

func (m *chartsModel) chartInteract(msg tea.Msg) {
	m.SetFocused(true)
	updated, _ := m.chart.Update(msg)
	m.chart = updated
	resyncChartView(&m.chart)
	redrawKlineCandles(&m.chart)
}

// chartWheel zooms the viewport, or switches frame when bars are too sparse in view.
func (m chartsModel) chartWheel(zoomIn bool) (chartsModel, tea.Cmd) {
	m.SetFocused(true)
	if len(m.klines) == 0 {
		return m, nil
	}

	chartZoomX(&m.chart, zoomIn, m.panSeconds())
	redrawKlineCandles(&m.chart)

	gaps := maxEmptyBarsInView(m.klines, m.chart.ViewMinX(), m.chart.ViewMaxX(), m.panSeconds())
	if gaps <= chartFrameSwitchEmptyBars {
		return m, nil
	}

	m.savedViewMin = time.UnixMilli(int64(math.Round(m.chart.ViewMinX() * 1000))).UTC()
	m.savedViewMax = time.UnixMilli(int64(math.Round(m.chart.ViewMaxX() * 1000))).UTC()
	m.restoreViewAfterLoad = true
	if zoomIn && m.frameIdx > 0 {
		m.frameUp()
		return m, m.fetchCmd()
	}
	if !zoomIn && m.frameIdx < len(chartFrames)-1 {
		m.frameDown()
		return m, m.fetchCmd()
	}
	m.restoreViewAfterLoad = false
	return m, nil
}

func (m chartsModel) Update(msg tea.Msg) (chartsModel, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if isChartFrameKey(msg) && (m.focused || len(m.klines) > 0) {
			switch msg.Key().Code {
			case tea.KeyUp:
				m.frameUp()
			case tea.KeyDown:
				m.frameDown()
			default:
				switch msg.String() {
				case "up", "k":
					m.frameUp()
				default:
					m.frameDown()
				}
			}
			m.loading = true
			m.SetFocused(true)
			return m, m.fetchCmd()
		}
		if len(m.klines) > 0 && (isChartPanKey(msg) || (m.focused && m.chartKeyMsg(msg))) {
			m.chartInteract(msg)
			return m, nil
		}
		switch msg.String() {
		case "tab":
			m.SetFocused(true)
			return m, nil
		case "s":
			if len(chartSources) > 1 {
				m.sourceIdx = (m.sourceIdx + 1) % len(chartSources)
			}
			m.loading = true
			return m, m.fetchCmd()
		case "m":
			m.marketIdx = (m.marketIdx + 1) % len(chartMarkets)
			m.loading = true
			return m, m.fetchCmd()
		case "f":
			m.frameDown()
			m.loading = true
			return m, m.fetchCmd()
		case "r":
			m.loading = true
			return m, m.fetchCmd()
		}
	case binanceKlinesMsg:
		m.loading = false
		if msg.err != nil {
			m.errText = shortErr(msg.err)
			return m, nil
		}
		m.errText = ""
		m.applyKlines(msg.klines)
		m.SetFocused(true)
	case chartLiveMsg:
		m.applyLive(msg)
	case chartDragTickMsg:
		if m.drag.active {
			m.applyDragPan()
			return m, m.dragPollCmd()
		}
	case tickMsg:
		cmds := []tea.Cmd{m.tickCmd()}
		if !m.loading && m.errText == "" {
			cmds = append(cmds, m.liveCmd())
		}
		return m, tea.Batch(cmds...)
	}
	return m, nil
}

func (m chartsModel) HandleMouse(msg tea.Msg) (chartsModel, bool, tea.Cmd) {
	if m.chart.ZoneID() == "" || len(m.klines) == 0 {
		return m, false, nil
	}
	mouse, ok := msg.(tea.MouseMsg)
	if !ok {
		return m, false, nil
	}

	switch msg := msg.(type) {
	case tea.MouseClickMsg:
		if msg.Mouse().Button != tea.MouseLeft {
			return m, false, nil
		}
		x, y, ok := m.chartZonePos(mouse)
		if !ok {
			return m, false, nil
		}
		m.SetFocused(true)
		m.drag = chartDragState{
			active:   true,
			startX:   x,
			startY:   y,
			lastX:    x,
			lastY:    y,
			viewMinX: m.chart.ViewMinX(),
			viewMaxX: m.chart.ViewMaxX(),
		}
		resyncChartView(&m.chart)
		redrawKlineCandles(&m.chart)
		return m, true, m.dragPollCmd()

	case tea.MouseMotionMsg:
		if !m.drag.active {
			return m, false, nil
		}
		x, y, ok := m.chartZonePos(mouse)
		if !ok {
			return m, true, nil
		}
		m.drag.lastX = x
		m.drag.lastY = y
		return m, true, nil

	case tea.MouseReleaseMsg:
		if !m.drag.active {
			return m, false, nil
		}
		if x, y, ok := m.chartZonePos(mouse); ok {
			m.drag.lastX = x
			m.drag.lastY = y
			m.applyDragPan()
		}
		m.clearDrag()
		return m, true, nil

	case tea.MouseWheelMsg:
		if _, _, ok := m.chartZonePos(mouse); !ok {
			return m, false, nil
		}
		zoomIn := msg.Mouse().Button == tea.MouseWheelUp
		updated, cmd := m.chartWheel(zoomIn)
		return updated, true, cmd
	}

	return m, false, nil
}

func (m chartsModel) View(width int) string {
	if width < 1 {
		width = 80
	}
	toolbarStyle := lipgloss.NewStyle().Foreground(lipgloss.Color("243"))
	focusHint := ""
	if m.focused {
		focusHint = " ◆"
	}
	toolbar := toolbarStyle.Render(fmt.Sprintf(
		"source: %s (s) · market: %s (m) · frame: %s (↑↓) · r reload%s · wheel zoom/frame · drag pan",
		m.source(), m.market(), m.frame().String(), focusHint,
	))
	var chartBody string
	if m.loading {
		chartBody = lipgloss.NewStyle().Foreground(lipgloss.Color("241")).Render("  loading…")
	} else if m.errText != "" {
		chartBody = lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Render("  " + m.errText)
	} else if m.chart.ZoneID() != "" {
		chartBody = chartBorderStyle.Width(m.chartW + 2).Render(m.chart.View())
	} else {
		chartBody = "  (no chart)"
	}
	return lipgloss.JoinVertical(lipgloss.Left, toolbar, chartBody)
}
