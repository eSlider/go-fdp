package binance

import (
	"time"

	"github.com/eslider/go-binance-fdp/pkg/data"
	"github.com/eslider/go-binance-fdp/pkg/fs"
	"github.com/eslider/go-binance-fdp/pkg/integrity"
)

// BuildHourlyTargets builds audit targets for UTC hours [fromHour, toHour] on day.
func BuildHourlyTargets(asset *HistoryAsset, day time.Time, fromHour, toHour int, checkMissing func(hour int) bool) []integrity.HourlyTarget {
	midnight := time.Date(day.Year(), day.Month(), day.Day(), 0, 0, 0, 0, time.UTC)
	if fromHour < 0 {
		fromHour = 0
	}
	if toHour > 23 {
		toHour = 23
	}
	targets := make([]integrity.HourlyTarget, 0, toHour-fromHour+1)
	for hour := fromHour; hour <= toHour; hour++ {
		start := midnight.Add(time.Duration(hour) * time.Hour)
		end := start.Add(time.Hour)
		cm := true
		if checkMissing != nil {
			cm = checkMissing(hour)
		}
		targets = append(targets, integrity.HourlyTarget{
			Hour:         hour,
			Path:         fs.GetModuleRelativePath(asset.HourlyParquetPath(hour)),
			Midnight:     midnight,
			HourStart:    start,
			HourEnd:      end,
			CheckMissing: cm,
		})
	}
	return targets
}

// BuildAuditTargetsForRange builds audit targets for [from, to] (hourly files for today, daily hive otherwise).
func BuildAuditTargetsForRange(asset *HistoryAsset, from, to time.Time) []integrity.HourlyTarget {
	from = from.UTC()
	to = to.UTC()
	var targets []integrity.HourlyTarget
	now := time.Now().UTC()
	currentHour := now.Hour()

	fromDay := time.Date(from.Year(), from.Month(), from.Day(), 0, 0, 0, 0, time.UTC)
	toDay := time.Date(to.Year(), to.Month(), to.Day(), 0, 0, 0, 0, time.UTC)

	for d := fromDay; !d.After(toDay); d = d.AddDate(0, 0, 1) {
		if data.IsToday(d) {
			fromH, toH := 0, 23
			if d.Equal(fromDay) {
				fromH = from.Hour()
			}
			if d.Equal(toDay) {
				toH = to.Hour()
			}
			if toH > currentHour {
				toH = currentHour
			}
			ts := BuildHourlyTargets(asset, d, fromH, toH, func(h int) bool {
				return h < currentHour
			})
			for i := range ts {
				fullHourEnd := ts[i].HourStart.Add(time.Hour)
				if d.Equal(fromDay) && from.After(ts[i].HourStart) {
					ts[i].HourStart = from
				}
				if d.Equal(toDay) && to.Before(ts[i].HourEnd) {
					ts[i].HourEnd = to
				}
				if !ts[i].HourEnd.Equal(fullHourEnd) || ts[i].Hour == currentHour {
					ts[i].CheckMissing = false
				}
			}
			targets = append(targets, ts...)
			continue
		}

		dayAsset := *asset
		dayAsset.Date = d
		winStart := d
		winEnd := d.Add(24 * time.Hour)
		if d.Equal(fromDay) && from.After(winStart) {
			winStart = from
		}
		if d.Equal(toDay) && to.Before(winEnd) {
			winEnd = to
		}
		targets = append(targets, integrity.HourlyTarget{
			Hour:         -1,
			Path:         fs.GetModuleRelativePath(dayAsset.ParquetPath()),
			Midnight:     d,
			HourStart:    winStart,
			HourEnd:      winEnd,
			CheckMissing: true,
		})
	}
	return targets
}
