package etl

import (
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
)

// Job is the exchange-neutral ETL unit.
type Job struct {
	Source     Source
	MarketType string
	Market     string
	Indicator  string
	Frame      data.Frame
	Date       time.Time
}
