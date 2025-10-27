package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync-v3/pkg/binance"
	"sync-v3/pkg/fs"
	"time"
)

func main() {
	srv, err := binance.NewHistoryConsumer(context.Background())
	if err != nil {
		log.Fatalf("could not initialize binance service: %s", err.Error())
	}

	asset := &binance.HistoryAsset{
		MarketType: binance.Spot,
		Frequency:  binance.Monthly,
		Frame:      binance.OneMinute,
		Indicator:  binance.Klines,
		Date:       time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC),
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
			if errors.Is(err, fs.ErrFileExists) {
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
			//default:
			//	<-time.After(time.Millisecond * 100)
			//	fmt.Println("timeout")
		}
	}

	log.Printf("ETL completed")
}
