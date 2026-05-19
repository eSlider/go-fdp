package binance

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/go-viper/mapstructure/v2"
)

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
	openTime := date.Add(time.Duration(p.OpenTime) * time.Millisecond)

	return &Kline{
		OpenTime:   openTime.UnixMilli(),
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
	openTimeMs := int32(
		openTime.UnixMilli() - openTime.Truncate(24*time.Hour).UnixMilli(),
	)

	return &KlineParquet{
		OpenTime: openTimeMs,
		Open:     k.OpenPrice,
		High:     k.HighPrice,
		Low:      k.LowPrice,
		Close:    k.ClosePrice,
		Volume:   k.Volume,
	}, nil
}

// Klines is a time-ordered candle series (REST, parquet, or stream).
type KlineSeries []*Kline

// Sorted returns a copy sorted by open_time ascending (non-nil only).
func (kl KlineSeries) Sorted() KlineSeries {
	out := make(KlineSeries, 0, len(kl))
	for _, c := range kl {
		if c != nil {
			out = append(out, c)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].OpenTime < out[j].OpenTime })
	return out
}

// Filter keeps candles with open_time in [start, start+duration).
func (kl KlineSeries) Filter(start time.Time, duration time.Duration) KlineSeries {
	if duration <= 0 {
		return nil
	}

	startMs := start.UnixMilli()
	endMs := start.Add(duration).UnixMilli()
	out := make(KlineSeries, 0, len(kl))
	for _, c := range kl {
		if c == nil {
			continue
		}
		if c.OpenTime >= startMs && c.OpenTime < endMs {
			out = append(out, c)
		}
	}
	return out.Sorted()
}

// Merge combines series; incoming overwrites same open_time.
func (kl KlineSeries) Merge(other KlineSeries) KlineSeries {
	merged := make(map[int64]*Kline, len(kl)+len(other))
	for _, c := range kl {
		if c != nil {
			merged[c.OpenTime] = c
		}
	}
	for _, c := range other {
		if c != nil {
			merged[c.OpenTime] = c
		}
	}
	out := make(KlineSeries, 0, len(merged))
	for _, c := range merged {
		out = append(out, c)
	}
	return out.Sorted()
}

// KlineStreamEvent is a @kline WebSocket message.
type KlineStreamEvent struct {
	EventType string      `json:"e"`
	EventTime int64       `json:"E"`
	Symbol    string      `json:"s"`
	Kline     KlineStream `json:"k"`
}

// KlineStream is the k object in a kline stream.
type KlineStream struct {
	OpenTime       int64   `json:"t"`
	CloseTime      int64   `json:"T"`
	Symbol         string  `json:"s"`
	Interval       string  `json:"i"`
	FirstTradeID   int64   `json:"f"`
	LastTradeID    int64   `json:"L"`
	OpenPrice      float64 `json:"o"`
	ClosePrice     float64 `json:"c"`
	HighPrice      float64 `json:"h"`
	LowPrice       float64 `json:"l"`
	Volume         float64 `json:"v"`
	NumberOfTrades int64   `json:"n"`

	IsClosed bool `json:"x"`

	QuoteVolume         float64 `json:"q"`
	TakerBuyVolume      float64 `json:"V"`
	TakerBuyQuoteVolume float64 `json:"Q"`
}

func (e *KlineStreamEvent) UnmarshalJSON(data []byte) error {
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName:          "json",
		Result:           e,
		WeaklyTypedInput: true,
	})
	if err != nil {
		return err
	}
	return decoder.Decode(parsed)
}

// ToKline maps stream fields to Kline.
func (s *KlineStream) ToKline() (*Kline, error) {
	var kl Kline
	if err := mapstructure.WeakDecode(s, &kl); err != nil {
		return nil, err
	}
	return &kl, nil
}

// KlineStreamName returns symbol@kline_interval for combined streams.
func KlineStreamName(symbol, interval string) string {
	return fmt.Sprintf("%s@kline_%s", symbol, interval)
}

// DecodeKlineStream unmarshals a WebSocket frame into Kline and whether the candle closed.
func DecodeKlineStream(data []byte) (*Kline, bool, error) {
	eventType, err := streamEventType(data)
	if err != nil {
		return nil, false, err
	}
	if eventType != "kline" {
		return nil, false, nil
	}
	var ev struct {
		EventType string      `json:"e"`
		EventTime int64       `json:"E"`
		Symbol    string      `json:"s"`
		Kline     KlineStream `json:"k"`
	}
	if err := json.Unmarshal(data, &ev); err != nil {
		return nil, false, err
	}
	kl, err := ev.Kline.ToKline()
	if err != nil {
		return nil, false, err
	}
	return kl, ev.Kline.IsClosed, nil
}

func streamEventType(data []byte) (string, error) {
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return "", err
	}
	s, _ := parsed["e"].(string)
	return s, nil
}
