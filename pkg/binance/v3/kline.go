package v3

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync-v3/pkg/data"
	"time"
)

//type Klines []Kline

//type KlinesOption func(*[]Kline) error
//
//func NewKlines(opts ...KlinesOption) (l []Kline, err error) {
//	for _, opt := range opts {
//		if err = opt(&l); err != nil {
//			return
//		}
//	}
//	return
//}

// KlineRequest is the query for GET /api/v3/klines.
type KlineRequest struct {
	Base     SymbolRequest
	Interval string  `in:"query=interval;required" validate:"required"`
	TimeZone *string `in:"query=timeZone;omitempty"`
	Limit    int64   `in:"query=limit;omitempty"`
}

// Kline is a candle returned by GET /api/v3/klines.
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
}

func (k *Kline) UnmarshalJSON(d []byte) error {
	var tmp []any
	if err := json.Unmarshal(d, &tmp); err != nil {
		return err
	}
	err := data.PopulateStructFromSlice(&tmp, k)
	return err
}

// String human-readable time
func (k *Kline) String() string {
	openTime := data.AnyTimestampToTime(k.OpenTime)
	closeTime := data.AnyTimestampToTime(k.CloseTime)

	return fmt.Sprintf("%s - %s", openTime.Format("2006-01-02 15:04:05"), closeTime.Format("2006-01-02 15:04:05"))
}

type KlineParquet struct {
	OpenTime int32   `parquet:"name=open_time,type=INT32, convertedtype=TIME_MILLIS" json:"open_time"`
	Open     float64 `parquet:"name=open_price, type=DOUBLE" json:"open_price"`
	High     float64 `parquet:"name=high_price, type=DOUBLE" json:"high_price"`
	Low      float64 `parquet:"name=low_price, type=DOUBLE" json:"low_price"`
	Close    float64 `parquet:"name=close_price, type=DOUBLE" json:"close_price"`
	Volume   float64 `parquet:"name=volume, type=DOUBLE" json:"volume"`
}

// ToKline - convert parquet kline back to Kline
func (p *KlineParquet) ToKline(date time.Time) *Kline {
	// Reconstruct open time
	// date should be midnight of the day
	openTime := date.Add(time.Duration(p.OpenTime) * time.Millisecond)

	return &Kline{
		OpenTime: openTime.UnixMilli(),
		// CloseTime: inferred or left 0, caller might need to set it if frame is known
		OpenPrice:  p.Open,
		HighPrice:  p.High,
		LowPrice:   p.Low,
		ClosePrice: p.Close,
		Volume:     p.Volume,
	}
}

// Parquet - convert kline to parquet format
func (k *Kline) Parquet() (*KlineParquet, error) {
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

	return &KlineParquet{
		OpenTime: openTimeMs,
		// The close time should be calculated from the open time and the interval between klines.
		// For example: 1m = 60 seconds, so the close time should be: open time + 60 seconds - 1 millisecond.
		Open:   k.OpenPrice,
		High:   k.HighPrice,
		Low:    k.LowPrice,
		Close:  k.ClosePrice,
		Volume: k.Volume,
	}, nil
}
