package main

import (
	"context"
	"log"
	"sync-v3/pkg/binance"
	"time"
)

func main() {
	ctx := context.Background()
	srv, err := binance.NewHistoryConsumer(ctx)

	if err != nil {
		log.Fatalf("could not initialize binance service: %s", err.Error())
	}

	fromDate := "2024-01-01"
	date := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	// transform string to time.Time
	parse, err := time.Parse("2006-01-02", fromDate)
	if err != nil {
		log.Fatalf("could not parse date: %s", err.Error())
	}

	if date.Before(parse) {
		log.Fatalf("date is before %s", fromDate)
	}

	asset := &binance.HistoryAsset{
		MarketType: binance.Spot,
		Frequency:  binance.Monthly,
		//Frame:      binance.OneMinute,
		Indicator: binance.Klines,
		Date:      date,
		Market:    "ETHUSDT",
	}
	for info := range srv.GetAsset(asset) {
		if info.Err != nil {
			log.Fatalf("could not ETL binance history asset: %s", err.Error())
		}
	}

	log.Printf("ETL completed")
}
