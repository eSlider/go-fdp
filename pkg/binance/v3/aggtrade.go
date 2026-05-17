package v3

import (
	"errors"
	"github.com/eslider/go-binance-fdp/pkg/data"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/goccy/go-json" // Fast drop-in replacement
)

// AggTrade - binance aggregated trade data
// CSV columns order (as in Binance public data files):
// 0: a (Aggregate tradeId)
// 1: p (Price)
// 2: q (Quantity)
// 3: f (First tradeId)
// 4: l (Last tradeId)
// 5: T (Timestamp in milliseconds)
// 6: m (Is buyer the market maker)
// 7: M (IsBestPriceMatch)
//
// Example row:
//
//	743,309.77000000,"0.35856000","804",805,1502958744048,False,True
//
// Ref: https://data.binance.vision/?prefix=data/spot/daily/aggTrades/
// Example ZIP-File: https://data.binance.vision/data/spot/daily/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2025-12-10.zip
// and Spot API docs: https://binance-docs.github.io/apidocs/spot/en/#compressed-aggregate-trades-list
type AggTrade struct {
	AggTradeID       int64   `json:"a" csv:"0"` // Aggregate tradeId
	Price            float64 `json:"p" csv:"1"` // Price (as string)
	Quantity         float64 `json:"q" csv:"2"` // Quantity (as string)
	FirstTradeID     int64   `json:"f" csv:"3"` // First tradeId
	LastTradeID      int64   `json:"l" csv:"4"` // Last tradeId
	Timestamp        int64   `json:"T" csv:"5"` // Timestamp
	IsBuyerMaker     bool    `json:"m" csv:"6"` // Is buyer the market maker?
	IsBestPriceMatch bool    `json:"M" csv:"7"` // Is this the best price match?
}

type AggTradeParquet struct {
	AggTradeID       int64   `parquet:"name=agg_trade_id,type=INT64,convertedtype=UINT_64"`
	Price            float64 `parquet:"name=price,type=DOUBLE"`
	Quantity         float64 `parquet:"name=quantity,type=DOUBLE"`
	FirstTradeID     int64   `parquet:"name=first_trade_id,type=INT64,convertedtype=UINT_64"`
	LastTradeID      int64   `parquet:"name=last_trade_id,type=INT64,convertedtype=UINT_64"`
	Time             int32   `parquet:"name=open_time,type=INT32, convertedtype=TIME_MILLIS"` // Stroing time only, without date
	IsBuyerMaker     bool    `parquet:"name=is_buyer_maker,type=BOOLEAN"`
	IsBestPriceMatch bool    `parquet:"name=is_best_price_match,type=BOOLEAN"`
}

func (a *AggTrade) UnmarshalJSON(data []byte) error {
	// Use decoder to unmarshal JSON into struct and 'Quantity' and 'Price' are "strings" but containing floats,
	//they need to be converted to floats
	var parsed map[string]any
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	// Create new mapstructure decoder with "json" tag, not "mapstructure"
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName:          "json",
		Result:           &a,
		WeaklyTypedInput: true,
	})
	if err != nil {
		return err
	}
	return decoder.Decode(&parsed)
}

func (a *AggTrade) Parquet() (*AggTradeParquet, error) {
	if a == nil {
		return nil, errors.New("a is nil")
	}
	if a.Timestamp == 0 {
		return nil, errors.New("timestamp is zero")
	}

	timestamp := data.AnyTimestampToTime(a.Timestamp)
	if timestamp == nil {
		return nil, errors.New("invalid timestamp")
	}

	// Get time, from midnight without date (only this day milliseconds) truncated.
	timeMs := int32(
		timestamp.UnixMilli() - timestamp.Truncate(24*time.Hour).UnixMilli(),
	)

	return &AggTradeParquet{
		Time:             timeMs,
		AggTradeID:       a.AggTradeID,
		Price:            a.Price,
		Quantity:         a.Quantity,
		FirstTradeID:     a.FirstTradeID,
		LastTradeID:      a.LastTradeID,
		IsBuyerMaker:     a.IsBuyerMaker,
		IsBestPriceMatch: a.IsBestPriceMatch,
	}, nil
}

// ToAggTrade - convert parquet aggTrade back to AggTrade
func (p *AggTradeParquet) ToAggTrade(date time.Time) *AggTrade {
	// Reconstruct timestamp
	// date should be midnight of the day
	timestamp := date.Add(time.Duration(p.Time) * time.Millisecond)

	return &AggTrade{
		AggTradeID:       p.AggTradeID,
		Price:            p.Price,
		Quantity:         p.Quantity,
		FirstTradeID:     p.FirstTradeID,
		LastTradeID:      p.LastTradeID,
		Timestamp:        timestamp.UnixMilli(),
		IsBuyerMaker:     p.IsBuyerMaker,
		IsBestPriceMatch: p.IsBestPriceMatch,
	}
}
