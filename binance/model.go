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

type Interval string

const (
	OneSecond   Interval = "1s"
	OneMinute            = "1m"
	ThreeMinute          = "3m"
	FiveMinute           = "5m"
	FifteenMin           = "15m"
	ThirtyMin            = "30m"
	OneHour              = "1h"
	TwoHour              = "2h"
	OneDay               = "1d"
)

type Market string

const (
	Spot    Market = "spot"
	Futures        = "futures"
	Option         = "option"
)

type Indicator string

const (
	Klines    Indicator = "klines"
	Trades              = "trades"
	AggTrades           = "aggTrades"
)

type HistoryAsset struct {
	Market
	Frequency
	Interval
	Indicator

	Date   time.Time
	Symbol string
}

func (q HistoryAsset) String() interface{} {
	return q.Link()
}

func (q HistoryAsset) Link() interface{} {
	if q.Market == "" {
		q.Market = Spot
	}

	if q.Frequency == "" {
		q.Frequency = Monthly
	}
	if q.Interval == "" {
		q.Interval = OneSecond
	}

	if q.Indicator == "" {
		q.Indicator = Klines
	}

	return fmt.Sprintf("data/%s/%s/%s/%s/%s/%s-%s-%s.zip",
		q.Market,
		q.Frequency,
		q.Indicator,
		q.Symbol,
		q.Interval,
		q.Symbol,
		q.Interval,
		q.Date.Format("2006-01"),
	)
}

// Kline - binance kline data
type Kline struct {
	OpenTime   int64   `csv:"0" parquet:"name=open_time, type=INT64, logicaltype=TIME,logicaltype.isadjustedtoutc=true, logicaltype.unit=MICROS"`
	OpenPrice  float64 `csv:"1" parquet:"name=open, type=DOUBLE"`
	HighPrice  float64 `csv:"2" parquet:"name=high, type=DOUBLE"`
	LowPrice   float64 `csv:"3" parquet:"name=low, type=DOUBLE"`
	ClosePrice float64 `csv:"4" parquet:"name=close, type=DOUBLE"`
	Volume     float64 `csv:"5" parquet:"name=volume, type=DOUBLE"`
	CloseTime  int64   `csv:"6" parquet:"name=close_time, type=INT64, logicaltype=TIME,logicaltype.isadjustedtoutc=true, logicaltype.unit=MICROS"`

	QuoteVolume    float64 `csv:"7"`
	NumberOfTrades int64   `csv:"8"`
	TakerBuyVolume float64 `csv:"9"`
	TakerBuyQuote  float64 `csv:"10"`
	Ignore         int64   `csv:"11"`
}
