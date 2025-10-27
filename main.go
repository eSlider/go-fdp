package main

import (
	"context"
	"fmt"
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

	fromDate := "2018-01-01"
	date := time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC)

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
		Frame:      binance.OneMinute,
		Indicator:  binance.Klines,
		Date:       date,
		Market:     "ETHUSDT",
	}

	// Download and transform
	infoCh, errCh := srv.DownloadAndTransform(asset)

MainLoop:
	for {
		select {
		case err, ok := <-errCh:
			if !ok {
				break MainLoop
			}
			fmt.Println(err)
		case info, ok := <-infoCh:
			if !ok {
				break MainLoop
			}
			fmt.Printf("Asset %s is %s", info.Path, info.Status.String())
			if info.Status == binance.StatusError {
				fmt.Printf("Error %v", info.Err)
			}
		default:
			<-time.After(time.Millisecond * 100)
			fmt.Println("timeout")
		}
	}

	log.Printf("ETL completed")
	//for info := range srv.GetAsset(asset) {
	//	fmt.Println(info.Path)
	//}
}
