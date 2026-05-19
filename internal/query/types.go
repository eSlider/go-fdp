package query

import (
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/eslider/go-binance-fdp/pkg/etl"
)

type MarketType string

const (
	Spot    MarketType = "spot"
	Futures MarketType = "futures"
	Option  MarketType = "option"
)

func (m MarketType) String() string { return string(m) }

func NewMarketType(s string) MarketType {
	switch s {
	case "futures":
		return Futures
	case "option":
		return Option
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

func (i Indicator) String() string { return string(i) }

type Candle struct {
	OpenTime  time.Time `json:"openTime"`
	CloseTime time.Time `json:"closeTime"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
}

type AggTrade struct {
	ID           int64     `json:"id"`
	Price        float64   `json:"price"`
	Quantity     float64   `json:"quantity"`
	FirstTradeID int64     `json:"firstTradeId"`
	LastTradeID  int64     `json:"lastTradeId"`
	Time         time.Time `json:"time"`
	IsBuyerMaker bool      `json:"isBuyerMaker"`
}

// Query is a market data read request (HTTP and parquet reads).
type Query struct {
	From       time.Time
	To         time.Time
	Market     string
	Exchange   string
	Source     etl.Source
	MarketType MarketType
	Frame      data.Frame
	Indicator  Indicator
}

func (r *Query) IsToday() bool {
	now := time.Now().UTC()
	return r.To.Year() == now.Year() && r.To.Month() == now.Month() && r.To.Day() == now.Day()
}

func (r Query) ETLJob(day time.Time) etl.Job {
	src := r.Source
	if src == "" {
		src = etl.SourceBinance
	}
	return etl.Job{
		Source:     src,
		MarketType: r.MarketType.String(),
		Market:     r.Market,
		Indicator:  r.Indicator.String(),
		Frame:      r.Frame,
		Date:       day,
	}
}
