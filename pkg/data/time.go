package data

import "time"

func ToMicroseconds(ts int64) int64 {
	switch TypeOfTimestamp(ts) {
	case TimestampInMicros:
		return ts
	case TimestampInSeconds:
		return ts * 1000 * 1000
	case TimestampInMillis:
		return ts * 1000
	}
	return 0
}

type TimestampType int

const (
	TimestampInSeconds TimestampType = iota + 1
	TimestampInMillis
	TimestampInMicros
)

// TypeOfTimestamp returns the type of timestamp: seconds, milliseconds or microseconds
func TypeOfTimestamp(ts int64) TimestampType {
	switch {
	case ts > 1e15:
		return TimestampInMicros
	case ts > 1e12:
		return TimestampInMillis
	default:
		return TimestampInSeconds
	}
}

// AnyTimestampToTime converts variants of timestamp to time.Time:
//   - 1: microseconds
//   - 2: seconds
//   - 3: milliseconds
func AnyTimestampToTime(ts int64) *time.Time {
	switch TypeOfTimestamp(ts) {
	case TimestampInMicros:
		micro := time.UnixMicro(ts)
		return &micro
	case TimestampInSeconds:
		unix := time.Unix(ts, 0)
		return &unix
	case TimestampInMillis:
		milli := time.UnixMilli(ts)
		return &milli
	}
	return nil
}

// IsToday checks if the given time is today
func IsToday(t time.Time) bool {
	// Variant 1:
	// Check if the required asset.Date is before, then yesterday midnight 24:00
	// midnightToday := time.Now().UTC().Truncate(24 * time.Hour)
	// t.After(midnightToday) ||t.Equal(midnightToday)

	// Variant 2:
	// now := time.Now().UTC()
	// todayMidnight := time.Date(
	// 	now.Year(), now.Month(), now.Day(),
	// 	0, 0, 0, 0,
	// 	now.Location())
	// .Add(-1 * time.Microsecond) // -1 microsecond

	// isToday := q.Date.After(todayMidnight) || q.Date.Equal(todayMidnight)
	// return isToday

	// Variant 3:
	// Check if required asset.Date is after now
	return t.Before(LastMomentOfYesterday())
}

// LastMomentOfYesterday return's midnight.
func LastMomentOfYesterday() time.Time {
	return time.Now().
		UTC().
		Truncate(24 * time.Hour).
		Add(-1 * time.Nanosecond)
}
