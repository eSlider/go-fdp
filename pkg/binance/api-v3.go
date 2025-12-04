package binance

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	_ "sync-v3/pkg/data"

	"github.com/go-playground/validator/v10"
	"github.com/google/go-querystring/query"
)

type AggTradeResponseV3 struct {
	AggTradeId     int    `json:"a"` // Aggregate tradeId
	Price          string `json:"p"` // Price
	Quantity       string `json:"q"` // Quantity
	FirstTradeId   int    `json:"f"` // First tradeId
	LastTradeId    int    `json:"l"` // Last tradeId
	Timestamp      int64  `json:"T"` // Timestamp, Example: 1498793709153
	BuyerMaker     bool   `json:"m"` // Was the buyer the maker?
	BestPriceMatch bool   `json:"M"` // Was the trade the best price match?
}

type CandleRequestV3 struct {
	Symbol string `url:"symbol" validate:"required"`

	// Interval - Supported  kline intervals (case-sensitive):
	// 	- seconds	1s
	// 	- minutes	1m, 3m, 5m, 15m, 30m
	// 	- hours	1h, 2h, 4h, 6h, 8h, 12h
	// 	- days	1d, 3d
	// 	- weeks	1w
	// 	- months	1M
	Interval string `url:"interval" validate:"required"` // ENUM

	StartTime *int64 `url:"startTime,omitempty"` // Millisecond timestamp
	EndTime   *int64 `url:"endTime,omitempty"`   // Millisecond timestamp

	TimeZone *string `url:"timeZone,omitempty"` // Default: 0 (UTC)
	Limit    int64   `url:"limit,omitempty"`    // 	Default: 500; Maximum: 1000.
}

func (r *CandleRequestV3) Validate() error {
	val := validator.New()
	err := val.Struct(r)
	if err != nil {
		return err
	}

	return nil
}

func (r *CandleRequestV3) GetURlParams() string {
	v, _ := query.Values(r)
	return v.Encode()
}

func GetCurrentCandles(cr *CandleRequestV3) ([]*Kline, error) {
	url := fmt.Sprintf("https://api.binance.com/api/v3/klines?%s", cr.GetURlParams())

	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var r = make([]*Kline, cr.Limit)
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	// for _, k := range r {
	// 	k.OpenTimeDate = data.AnyTimestampToTime(k.OpenTime)
	// 	k.CloseTimeDate = data.AnyTimestampToTime(k.CloseTime)
	// }

	return r, nil
}
