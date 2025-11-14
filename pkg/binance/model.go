package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync-v3/pkg/data"
	"time"
)

// ETLStatus - represents asset processing status
type ETLStatus int

var StatusList = []string{
	"error",
	"zip-downloading",
	"zip-reading",
	"zip-persisting",
	"zip-ready",

	"csv-reading",
	"csv-2-parquet-transforming",
	"parquet-persisting",
	"parquet-ready",
	"parquet-downloading",
	"parquet-reading",
	"parquet-persisting",
	"parquet-ready",
	"parquet-downloading",
	"transforming",
	"reading_parquet",
}

func (s *ETLStatus) String() any {
	return StatusList[*s]
}

const (
	StatusError ETLStatus = iota
	StatusZipDownloading
	StatusZipReading
	StatusZipPersisting
	StatusZipReady
	StatusCSVReading
	StatusCSV2ParquetTransforming
	StatusParquetPersisting
	StatusParquetReady
)

type AssetETLInfo struct {
	Status ETLStatus
	Buffer *data.Buffer
	Path   string
	Err    error
	Info   string
}

func (i *AssetETLInfo) Done() bool {
	return i.Status == StatusParquetReady
}

type Frequency string

const (
	Monthly Frequency = "monthly"
	Daily             = "daily"
)

type Frame time.Duration

const (
	NoFrame     Frame = 0
	Nanosecond  Frame = 1
	Microsecond       = 1000 * Nanosecond
	Millisecond       = 1000 * Microsecond
	OneSecond         = 1000 * Millisecond
	OneMinute         = 60 * OneSecond
	ThreeMinute       = 3 * OneMinute
	FiveMinute        = 5 * OneMinute
	FifteenMin        = 15 * OneMinute
	Hour              = 60 * OneMinute
	TwoHour           = 2 * Hour
	OneDay            = 24 * Hour
	OneWeek           = 7 * OneDay
)

var Frames = map[string]Frame{
	"1s":  OneSecond,
	"1m":  OneMinute,
	"3m":  ThreeMinute,
	"5m":  FiveMinute,
	"15m": FifteenMin,
	"1h":  Hour,
	"2h":  TwoHour,
	"1d":  OneDay,
	"1w":  OneWeek,
}

// StringToFrame - converts a string to a frame
func StringToFrame(frame string) Frame {
	f, ok := Frames[frame]
	if !ok {
		return NoFrame
	}
	return f
}

func FrameToString(frame Frame) string {
	for k, v := range Frames {
		if v == frame {
			return k
		}
	}
	return ""
}

// String - returns a frame
func (f Frame) String() string {
	for k, v := range Frames {
		if v == f {
			return k
		}
	}
	return ""
}

// NewFrame - returns a frame
func NewFrame(frame string) Frame {
	if frame == "" {
		return OneMinute
	}
	return StringToFrame(frame)
}

type FutureType string

func (f FutureType) String() string {
	return string(f)
}

const (
	FutureTypeCm FutureType = "cm" // Case Management
	FutureTypeUm            = "um" // Utilization Management
)

type FutureData string

const (
	// FutureDataAggTrades
	// 	- https://data.binance.vision/data/futures/um/daily/aggTrades/ACHUSDT/ACHUSDT-aggTrades-2025-10-26.zip
	FutureDataAggTrades FutureData = "aggTrades"

	// FutureDataBookDepth
	//	- https://data.binance.vision/data/futures/um/daily/bookDepth/AGIXBUSD/AGIXBUSD-bookDepth-2023-07-20.zip
	FutureDataBookDepth = "bookDepth"

	// FutureDataBookTicker
	//	- https://data.binance.vision/data/futures/um/daily/bookTicker/AGIXBUSD/AGIXBUSD-bookTicker-2023-07-20.zip
	FutureDataBookTicker = "bookTicker"

	// FutureDataIndexPriceKline https://data.binance.vision/data/futures/um/daily/indexPriceKlines/BTCUSDT/1m/BTCUSDT-1m-2025-10-26.zip
	FutureDataIndexPriceKline = "indexPriceKline"

	// FutureDataKlines https://data.binance.vision/data/futures/um/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2025-10-26.zip
	FutureDataKlines             = "klines"
	FutureDataMarkPriceKlines    = "markPriceKline"
	FutureDataMetrics            = "metrics"
	FutureDataPremiumIndexKlines = "premiumIndexKline"
	FutureDataTrades             = "trades"
)

type MarketType string

const (
	Spot    MarketType = "spot"
	Futures            = "futures"
	Option             = "option"
)

func (m MarketType) String() string {
	return string(m)
}

func NewMarketType(s string) MarketType {
	switch s {
	case "futures":
		return Futures
	case "option":
		return Option
	case "spot":
		return Spot
	default:
		return Spot
	}
}

type Indicator string

const (
	Klines    Indicator = "klines"
	Trades    Indicator = "trades"
	AggTrades Indicator = "aggTrades"
)

func (i Indicator) String() string {
	return string(i)
}

type HistoryAsset struct {
	MarketType
	Frequency
	Frame
	Indicator
	Market string
	Date   time.Time
}

func (q *HistoryAsset) String() string {
	return q.SymbolDateAssetZipLink()
}

// SymbolLink - is a link to a specific asset directory of a symbol
func (q *HistoryAsset) SymbolLink() string {
	if q.MarketType == "" {
		q.MarketType = Spot
	}
	if q.Frequency == "" {
		q.Frequency = Monthly
	}
	if q.Frame.String() == "" {
		q.Frame = OneMinute
	}
	if q.Indicator == "" {
		q.Indicator = Klines
	}

	link := fmt.Sprintf("data/%s/%s/%s/%s",
		q.MarketType,
		q.Frequency,
		q.Indicator,
		q.Market,
	)
	return link
}

// SymbolFrameLink - is a link to a specific asset and frame directory of zip files
func (q *HistoryAsset) SymbolFrameLink() string {
	// Indicator having no frame directories
	if q.Indicator == Klines {
		return fmt.Sprintf("%s/%s",
			q.SymbolLink(),
			q.Frame)
	}
	return q.SymbolLink()

}

// SymbolDateAssetZipLink - is a link to a concrete asset zip file
func (q *HistoryAsset) SymbolDateAssetZipLink() string {
	var layout string
	switch q.Frequency {
	case Monthly:
		layout = "2006-01"
	case Daily:
		layout = "2006-01-02"

	}
	switch q.Indicator {
	case Klines:
		return fmt.Sprintf("%s/%s-%s-%s.zip",
			q.SymbolFrameLink(),
			q.Market,
			q.Frame,
			q.Date.Format(layout))
	default:
		return fmt.Sprintf("%s/%s-%s-%s.zip",
			q.SymbolLink(),
			q.Market,
			q.Indicator,
			q.Date.Format(layout))
	}
}

// NewHistoryAssetByPath parse path to asset
// Example: - data/spot/monthly/klines/ETHUSDT/1m/ETHUSDT-1m-2023-06.zip
func NewHistoryAssetByPath(path string) (a *HistoryAsset, err error) {
	chunks := strings.Split(path, "/")
	a = &HistoryAsset{
		MarketType: MarketType(chunks[1]),
		Frequency:  Frequency(chunks[2]),
		Indicator:  Indicator(chunks[3]),
		Market:     chunks[4],
	}

	// The latest chunk is the file name
	fileName := strings.TrimRight(chunks[len(chunks)-1], ".zip")
	fileNameChunks := strings.Split(fileName, "-")
	l := len(fileNameChunks)

	switch a.Frequency {
	case Monthly: // Example: BTCUSDT-aggTrades-2025-10.zip
		a.Date, err = time.Parse("2006-01", fmt.Sprintf("%s-%s",
			fileNameChunks[l-2],
			fileNameChunks[l-1]))
	case Daily: // Example: BTCUSDT-aggTrades-2025-10-26.zip
		a.Date, err = time.Parse("2006-01-02",
			fmt.Sprintf("%s-%s-%s", fileNameChunks[l-3], fileNameChunks[l-2], fileNameChunks[l-1]))
	}

	switch a.Indicator {
	case Klines:
		a.Frame = StringToFrame(chunks[5])
	}

	return
}

// IsHistoryLinkAvailable - is a link to a concrete asset zip file
func (q *HistoryAsset) IsHistoryLinkAvailable() (err error) {
	// Market and Date are always required
	if q.Date.IsZero() {
		return fmt.Errorf("date is required")
	}

	// If date is today, then there is no data for today
	if q.IsToday() {
		return fmt.Errorf("no data for today")
	}

	if q.MarketType == "" {
		return fmt.Errorf("market type is required")
	}

	if q.Market == "" {
		return fmt.Errorf("market is required")
	}

	// Frame is required only for klines
	if q.Indicator == Klines {
		if q.Frame == NoFrame {
			return fmt.Errorf("frame is required for klines")
		}
	}

	return nil
}

// ParquetPath - returns parquet file path
//   - Create link to work with hive partitioning
func (q *HistoryAsset) ParquetPath() string {
	dest := fmt.Sprintf(
		"data/mtype=%s/indicator=%s/market=%s/frame=%s/year=%d/month=%d/day=%d/data.parquet",
		q.MarketType,
		q.Indicator,
		q.Market,
		q.Frame,
		q.Date.Year(),
		int(q.Date.Month()),
		q.Date.Day(),
	)
	return dest
}

// TodayDuckDBPath - returns the path to the DuckDB file for today's cached data
func (q *HistoryAsset) TodayDuckDBPath() string {
	return fmt.Sprintf("data/mtype=%s/indicator=%s/market=%s/frame=%s/today.duckdb",
		q.MarketType,
		q.Indicator,
		q.Market,
		q.Frame)
}

// IsToday - Check  date,s before now until midnight, handle using other api
func (q *HistoryAsset) IsToday() bool {

	return data.IsToday(q.Date)
}

// Transformer - interface for transforming data to several formats
type Transformer interface {
	Parquet() (any, error) // Parquet - transforms the data to parquet format
}

// Kline - binance kline data
type Kline struct {
	OpenTime       int64   `csv:"0"`
	OpenPrice      float64 `csv:"1"`
	HighPrice      float64 `csv:"2"`
	LowPrice       float64 `csv:"3"`
	ClosePrice     float64 `csv:"4"`
	Volume         float64 `csv:"5"`
	CloseTime      int64   `csv:"6"`
	QuoteVolume    float64 `csv:"7"`
	NumberOfTrades int64   `csv:"8"`
	TakerBuyVolume float64 `csv:"9"`
	TakerBuyQuote  float64 `csv:"10"`
	Ignore         float64 `csv:"11"`
	// OpenTimeDate   *time.Time `csv:"-"`
	// CloseTimeDate  *time.Time `csv:"-"`
}

func (k *Kline) String() string {
	openTime := data.AnyTimestampToTime(k.OpenTime)
	closeTime := data.AnyTimestampToTime(k.CloseTime)

	// return only time human readable
	return fmt.Sprintf("%s - %s", openTime.Format("2006-01-02 15:04:05"), closeTime.Format("2006-01-02 15:04:05"))
}

func strToFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}
func (k *Kline) UnmarshalJSON(data []byte) error {
	// Intermediate slice for mixed types
	var tmp []any
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	// Assign by position (handle type assertions)
	k.OpenTime = int64(tmp[0].(float64))
	k.OpenPrice = strToFloat(tmp[1].(string))
	k.HighPrice = strToFloat(tmp[2].(string))
	k.LowPrice = strToFloat(tmp[3].(string))
	k.ClosePrice = strToFloat(tmp[4].(string))
	k.Volume = strToFloat(tmp[5].(string))
	k.CloseTime = int64(tmp[6].(float64))
	k.QuoteVolume = strToFloat(tmp[7].(string))
	k.NumberOfTrades = int64(tmp[8].(float64))
	k.TakerBuyVolume = strToFloat(tmp[9].(string))
	k.TakerBuyQuote = strToFloat(tmp[10].(string))
	k.Ignore = strToFloat(tmp[11].(string))

	return nil
}

// AggTrade - binance aggregated trade data
// CSV columns order (as in Binance public data files):
// 0: a (Aggregate tradeId)
// 1: p (Price)
// 2: q (Quantity)
// 3: f (First tradeId)
// 4: l (Last tradeId)
// 5: T (Timestamp in milliseconds)
// 6: m (Is buyer the market maker)
// 7: M (Ignore)
//
// Example row:
//
//	743,309.77000000,0.35856000,804,805,1502958744048,False,True
//
// Ref: https://data.binance.vision/?prefix=data/spot/daily/aggTrades/
// and Spot API docs: https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
type AggTrade struct {
	AggTradeID   int64   `csv:"0"` // a (Aggregate tradeId)
	Price        float64 `csv:"1"` // p (Price)
	Quantity     float64 `csv:"2"` // q (Quantity)
	FirstTradeID int64   `csv:"3"` // f (First tradeId)
	LastTradeID  int64   `csv:"4"` // l (Last tradeId)
	Timestamp    int64   `csv:"5"` // T (Timestamp in milliseconds)
	IsBuyerMaker bool    `csv:"6"` // m (Is buyer the market maker)
	Ignore       bool    `csv:"7"` // M (Ignore)
}

type ParquetAggTrade struct {
	Timestamp    int64   `parquet:"name=ts,type=INT64, logicaltype=TIMESTAMP, logicaltype.isadjustedtoutc=false, logicaltype.unit=MICROS"`
	AggTradeID   int64   `parquet:"name=agg_trade_id,type=INT64,convertedtype=UINT_64"`
	FirstTradeID int64   `parquet:"name=first_trade_id,type=INT64,convertedtype=UINT_64"`
	LastTradeID  int64   `parquet:"name=last_trade_id,type=INT64,convertedtype=UINT_64"`
	Price        float64 `parquet:"name=price,type=DOUBLE"`
	Quantity     float64 `parquet:"name=quantity,type=DOUBLE"`
	IsBuyerMaker bool    `parquet:"name=is_buyer_maker,type=BOOLEAN"`
}

func (a *AggTrade) Parquet() (*ParquetAggTrade, error) {
	if a == nil {
		return nil, errors.New("a is nil")
	}
	if a.Timestamp == 0 {
		return nil, errors.New("timestamp is zero")
	}

	// Check if the timestamp is milliseconds or microseconds
	return &ParquetAggTrade{
		Timestamp:    data.ToMicroseconds(a.Timestamp),
		AggTradeID:   a.AggTradeID,
		Price:        a.Price,
		Quantity:     a.Quantity,
		FirstTradeID: a.FirstTradeID,
		LastTradeID:  a.LastTradeID,
		IsBuyerMaker: a.IsBuyerMaker,
	}, nil
}

type ParquetKline struct {
	OpenTime int32 `parquet:"name=open_time,type=INT32, convertedtype=TIME_MILLIS" json:"open_time"`
	// CloseTime int64   `parquet:"name=close_time,type=INT64, convertedtype=TIMESTAMP_MICROS" json:"close_time"`
	Open   float64 `parquet:"name=open_price, type=DOUBLE" json:"open_price"`
	High   float64 `parquet:"name=high_price, type=DOUBLE" json:"high_price"`
	Low    float64 `parquet:"name=low_price, type=DOUBLE" json:"low_price"`
	Close  float64 `parquet:"name=close_price, type=DOUBLE" json:"close_price"`
	Volume float64 `parquet:"name=volume, type=DOUBLE" json:"volume"`
}

// Parquet - convert kline to parquet format
func (k *Kline) Parquet() (*ParquetKline, error) {
	if k == nil {
		return nil, errors.New("ParquetKline is nil")
	}

	if k.OpenTime == 0 {
		return nil, errors.New("open time is zero")
	}

	openTime := data.AnyTimestampToTime(k.OpenTime)
	// Get time, from middle night without date(only this day milliseconds) truncated.
	openTimeMs := int32(
		openTime.UnixMilli() - openTime.Truncate(24*time.Hour).UnixMilli(),
	)

	return &ParquetKline{
		OpenTime: openTimeMs,
		// The close time should be calculated from the open time and the interval between klines.
		// For example: 1m = 60 seconds, so the close time should be: open time + 60 seconds - 1 millisecond.
		// CloseTime: data.ToMicroseconds(k.CloseTime),
		Open:   k.OpenPrice,
		High:   k.HighPrice,
		Low:    k.LowPrice,
		Close:  k.ClosePrice,
		Volume: k.Volume,
	}, nil
}
