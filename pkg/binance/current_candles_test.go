package binance

import (
	"fmt"
	"testing"
	"time"
)

// Test normalization by getting a zip file
func TestCurrentCandles(t *testing.T) {

	t.Run("Parsing and creation history asset links", func(t *testing.T) {
		now := time.Now().UTC()
		todayMidnight := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
		// -1 microsecond
		todayMidnight = todayMidnight.Add(-1 * time.Microsecond)

		fmt.Println(todayMidnight)

		hoursCountBefore := now.Sub(todayMidnight).Hours()
		fmt.Println(hoursCountBefore)

		for _, market := range []string{"BTCUSDT", "ETHUSDT"} {
			for _, frame := range []string{
				"1m",
				// "1h",
			} {
				for _, indicator := range []Indicator{Klines} {

					cr := new(CandleRequestV3)
					cr.Symbol = market
					cr.Interval = frame
					now := time.Now()

					startTime := now.Add(-1 * time.Hour * 24).UnixMicro()

					if frame == "1m" {
						startTime = now.Add(-1 * time.Minute).UnixMicro()

					}

					if frame == "1h" {
						startTime = now.Add(-1 * time.Hour).UnixMicro()

					}
					cr.StartTime = &startTime

					// End time should be now
					endTime := now.UnixMicro()
					cr.EndTime = &endTime

					fmt.Println(market, frame, indicator)

					kandles, err := GetCurrentCandles(cr)
					if err != nil {
						t.Fatal(err)
					}
					if len(kandles) < 1 {
						t.Fatal("no kline data returned")
					}

					if frame == "1m" && len(kandles) != 1 {
						t.Fatal("expected 1 minute kline")
					}
				}
			}
		}
	})
}
