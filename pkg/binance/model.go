package binance

import (
	"fmt"
	"time"
)

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
		q.Frame = OneSecond
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
	if q.Indicator == AggTrades {
		return q.SymbolLink()
	}

	return fmt.Sprintf("%s/%s",
		q.SymbolLink(),
		q.Frame)
}

// SymbolDateAssetZipLink - is a link to a concrete asset zip file
func (q HistoryAsset) SymbolDateAssetZipLink() string {

	return fmt.Sprintf("%s/%s-%s-%s.zip",
		q.SymbolFrameLink(),
		q.Market,
		q.Frame,
		q.Date.Format("2006-01"),
	)
}

// Kline - binance kline data
type Kline struct {
	OpenTime   int64   `csv:"0"`
	OpenPrice  float64 `csv:"1"`
	HighPrice  float64 `csv:"2"`
	LowPrice   float64 `csv:"3"`
	ClosePrice float64 `csv:"4"`
	Volume     float64 `csv:"5"`
	CloseTime  int64   `csv:"6"`

	QuoteVolume    float64 `csv:"7"`
	NumberOfTrades int64   `csv:"8"`
	TakerBuyVolume float64 `csv:"9"`
	TakerBuyQuote  float64 `csv:"10"`
	Ignore         int64   `csv:"11"`
}

type ParquetKline struct {
	OpenTime  int32 `parquet:"name=open_time, type=INT64, logicaltype=TIME,logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	CloseTime int32 `parquet:"name=close_time, type=INT64, logicaltype=TIME,logicaltype.isadjustedtoutc=true, logicaltype.unit=MILLIS"`
	Candle
}

type Candle struct {
	Open   float64 `parquet:"name=open, type=DOUBLE"`
	High   float64 `parquet:"name=high, type=DOUBLE"`
	Low    float64 `parquet:"name=low, type=DOUBLE"`
	Close  float64 `parquet:"name=close, type=DOUBLE"`
	Volume float64 `parquet:"name=volume, type=DOUBLE"`
}

// NewParquetKline - optimize kline data for parquet
func NewParquetKline(kline *Kline) *ParquetKline {
	return &ParquetKline{
		OpenTime:  int32(kline.OpenTime),
		CloseTime: int32(kline.CloseTime),
		Candle: Candle{
			Open:   kline.OpenPrice,
			High:   kline.HighPrice,
			Low:    kline.LowPrice,
			Close:  kline.ClosePrice,
			Volume: kline.Volume,
		},
	}
}
