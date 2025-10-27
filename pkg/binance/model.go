package binance

import (
	"fmt"
	"strings"
	"sync-v3/pkg/data"
	"time"
)

// ETLStatus - represents asset processing status
type ETLStatus int

var StatusList = []string{
	"error",
	"downloading",
	"reading_zip",
	"persisting_zip",
	"reading_csv",
	"transforming",
	"reading_parquet",
}

func (s *ETLStatus) String() any {
	return StatusList[*s]
}

const (
	StatusError ETLStatus = iota
	StatusDownloading
	StatusReadingZip
	StatusPersistingZip
	StatusReadingCsv
	StatusTransforming
	StatusReadingParquet
)

type AssetETLInfo struct {
	Status ETLStatus
	Buffer *data.Buffer
	Path   string
	Err    error
}

type Frequency string

const (
	Monthly Frequency = "monthly"
	Daily             = "daily"
)

type Frame string

const (
	OneSecond   Frame = "1s"
	OneMinute         = "1m"
	ThreeMinute       = "3m"
	FiveMinute        = "5m"
	FifteenMin        = "15m"
	ThirtyMin         = "30m"
	OneHour           = "1h"
	TwoHour           = "2h"
	OneDay            = "1d"
)

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

type Indicator string

const (
	Klines    Indicator = "klines"
	Trades              = "trades"
	AggTrades           = "aggTrades"
)

type HistoryAsset struct {
	MarketType
	Frequency
	Frame
	Indicator
	Market string

	Date time.Time
}

func (q HistoryAsset) String() string {
	return q.SymbolDateAssetZipLink()
}

// SymbolLink - is a link to a specific asset directory of a symbol
func (q HistoryAsset) SymbolLink() string {
	if q.MarketType == "" {
		q.MarketType = Spot
	}
	if q.Frequency == "" {
		q.Frequency = Monthly
	}
	if q.Frame == "" {
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
func (q HistoryAsset) SymbolFrameLink() string {
	// Indicator having no frame directories
	if q.Indicator == Klines {
		return fmt.Sprintf("%s/%s",
			q.SymbolLink(),
			q.Frame)
	}
	return q.SymbolLink()

}

// SymbolDateAssetZipLink - is a link to a concrete asset zip file
func (q HistoryAsset) SymbolDateAssetZipLink() string {
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

	// Latest chunk is the file name
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
		a.Frame = Frame(chunks[5])
	}

	return
}

// IsZipLink - is a link to a concrete asset zip file
func (q HistoryAsset) IsZipLink() bool {
	if q.Date.IsZero() || q.Frame == "" || q.Market == "" {
		return false
	}
	return true
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
	Ignore         int64   `csv:"11"`
}

type ParquetKline struct {
	OpenTime  int64   `parquet:"name=open_time,type=INT64,logicaltype=TIME,logicaltype.isadjustedtoutc=true,logicaltype.unit=MILLIS"`
	CloseTime int64   `parquet:"name=close_time,type=INT64,logicaltype=TIME,logicaltype.isadjustedtoutc=true,logicaltype.unit=MILLIS"`
	Open      float64 `parquet:"name=open_price, type=DOUBLE"`
	High      float64 `parquet:"name=high_price, type=DOUBLE"`
	Low       float64 `parquet:"name=low_price, type=DOUBLE"`
	Close     float64 `parquet:"name=close_price, type=DOUBLE"`
	Volume    float64 `parquet:"name=volume, type=DOUBLE"`
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
	Timestamp    int64   `parquet:"name=ts,type=INT64,type=INT64,logicaltype=TIME,logicaltype.isadjustedtoutc=true,logicaltype.unit=MILLIS"`
	AggTradeID   int64   `parquet:"name=agg_trade_id,type=INT64,convertedtype=UINT_64"`
	FirstTradeID int64   `parquet:"name=first_trade_id,type=INT64,convertedtype=UINT_64"`
	LastTradeID  int64   `parquet:"name=last_trade_id,type=INT64,convertedtype=UINT_64"`
	Price        float64 `parquet:"name=price,type=DOUBLE"`
	Quantity     float64 `parquet:"name=quantity,type=DOUBLE"`
	IsBuyerMaker bool    `parquet:"name=is_buyer_maker,type=BOOLEAN"`
}

func NewParquetAggTrade(a *AggTrade) *ParquetAggTrade {
	// Check if the timestamp is milliseconds or microseconds
	return &ParquetAggTrade{
		Timestamp:    ToMs(a.Timestamp),
		AggTradeID:   a.AggTradeID,
		Price:        a.Price,
		Quantity:     a.Quantity,
		FirstTradeID: a.FirstTradeID,
		LastTradeID:  a.LastTradeID,
		IsBuyerMaker: a.IsBuyerMaker,
	}
}

// NewParquetKline - optimize kline data for parquet
func NewParquetKline(kline *Kline) *ParquetKline {
	if kline == nil {
		return nil
	}

	if kline.OpenTime == 0 {
		return nil
	}

	return &ParquetKline{
		OpenTime:  ToMs(kline.OpenTime),
		CloseTime: ToMs(kline.CloseTime),
		Open:      kline.OpenPrice,
		High:      kline.HighPrice,
		Low:       kline.LowPrice,
		Close:     kline.ClosePrice,
		Volume:    kline.Volume,
	}
}

func ToMs(ts int64) (v int64) {
	tp := TypeOfTimestamp(ts)
	switch tp {
	case TimestampInMicros:
		v = int64(ts / 1000)
	case TimestampInSeconds:
		v = int64(ts * 1000)
	case TimestampInMillis:
		v = int64(ts)
	}
	return v
}

type TimestampType int

const (
	TimestampInSeconds TimestampType = iota + 1
	TimestampInMillis
	TimestampInMicros
)

func TypeOfTimestamp(ts int64) TimestampType {
	switch {
	case ts > 1e18:
		return TimestampInMicros
	case ts > 1e12:
		return TimestampInMillis
	default:
		return TimestampInSeconds
	}
}
