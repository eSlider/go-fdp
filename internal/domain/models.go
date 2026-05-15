package domain

import (
	"time"

	"sync-v3/pkg/data"
)

type MarketType string

const (
	Spot    MarketType = "spot"    // Spot - trading with immediate settlement
	Futures            = "futures" // Futures - trading with delayed settlement
	Option             = "option"  // Option - trading with the right, but not the obligation to buy or sell
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

type Candle struct {
	OpenTime  time.Time `json:"openTime"`
	CloseTime time.Time `json:"closeTime"`
	Open      float64   `json:"open"`
	High      float64   `json:"high"`
	Low       float64   `json:"low"`
	Close     float64   `json:"close"`
	Volume    float64   `json:"volume"`
}

type Trade struct {
	ID           int64     `json:"id"`
	Price        float64   `json:"price"`
	Quantity     float64   `json:"quantity"`
	Time         time.Time `json:"time"`
	IsBuyerMaker bool      `json:"isBuyerMaker"`
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

// Request parameters DTO
type MarketDataRequest struct {
	From       time.Time
	To         time.Time
	Market     string
	Exchange   string
	MarketType MarketType
	Frame      data.Frame
	Indicator  Indicator
}

func (r *MarketDataRequest) IsToday() bool {
	now := time.Now().UTC()
	return r.To.Year() == now.Year() && r.To.Month() == now.Month() && r.To.Day() == now.Day()
}
