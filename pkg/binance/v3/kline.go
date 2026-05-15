package v3

import (
	"encoding/json"
	"strconv"
)

// Kline is a candle returned by GET /api/v3/klines.
type Kline struct {
	OpenTime       int64
	OpenPrice      float64
	HighPrice      float64
	LowPrice       float64
	ClosePrice     float64
	Volume         float64
	CloseTime      int64
	QuoteVolume    float64
	NumberOfTrades int64
	TakerBuyVolume float64
	TakerBuyQuote  float64
	Ignore         float64
}

func parseFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

func (k *Kline) UnmarshalJSON(data []byte) error {
	var tmp []any
	if err := json.Unmarshal(data, &tmp); err != nil {
		return err
	}

	k.OpenTime = int64(tmp[0].(float64))
	k.OpenPrice = parseFloat(tmp[1].(string))
	k.HighPrice = parseFloat(tmp[2].(string))
	k.LowPrice = parseFloat(tmp[3].(string))
	k.ClosePrice = parseFloat(tmp[4].(string))
	k.Volume = parseFloat(tmp[5].(string))
	k.CloseTime = int64(tmp[6].(float64))
	k.QuoteVolume = parseFloat(tmp[7].(string))
	k.NumberOfTrades = int64(tmp[8].(float64))
	k.TakerBuyVolume = parseFloat(tmp[9].(string))
	k.TakerBuyQuote = parseFloat(tmp[10].(string))
	k.Ignore = parseFloat(tmp[11].(string))

	return nil
}
