package binance

import "time"

// Event - event to store daily informations
type Event struct {
	Date   time.Time
	Klines []ParquetKline
	Info   string
}
