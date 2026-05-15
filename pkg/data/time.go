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

// IsToday checks if the given time is today (at or after midnight UTC today)
func IsToday(t time.Time) bool {
	// A date is "today" if it's after the last moment of yesterday
	// This means we should use the API instead of historical data archives
	return !t.Before(LastMomentOfYesterday())
}

// LastMomentOfYesterday return's midnight.
func LastMomentOfYesterday() time.Time {
	return time.Now().
		UTC().
		Truncate(24 * time.Hour).
		Add(-1 * time.Nanosecond)
}

// Frame is a candle aggregation interval (duration-based for arithmetic).
type Frame time.Duration

const (
	NoFrame     Frame = 0
	Nanosecond  Frame = 1
	Microsecond       = 1000 * Nanosecond
	Millisecond       = 1000 * Microsecond
	Second            = 1000 * Millisecond
	Minute            = 60 * Second
	ThreeMinute       = 3 * Minute
	FiveMinute        = 5 * Minute
	FifteenMin        = 15 * Minute
	Hour              = 60 * Minute
	TwoHour           = 2 * Hour
	OneDay            = 24 * Hour
	OneWeek           = 7 * OneDay
)

// Frames maps Binance interval strings to Frame values.
var Frames = map[string]Frame{
	"":    NoFrame,
	"1ns": Nanosecond,
	"1us": Microsecond,
	"1s":  Second,
	"1m":  Minute,
	"3m":  ThreeMinute,
	"5m":  FiveMinute,
	"15m": FifteenMin,
	"1h":  Hour,
	"2h":  TwoHour,
	"4h":  4 * Hour,
	"1d":  OneDay,
	"1w":  OneWeek,
}

func StringToFrame(s string) Frame {
	if f, ok := Frames[s]; ok {
		return f
	}
	return NoFrame
}

func (f Frame) String() string {
	for k, v := range Frames {
		if v == f {
			return k
		}
	}
	return ""
}

// NewFrame parses an interval string; empty string defaults to 1m.
func NewFrame(s string) Frame {
	if s == "" {
		return Minute
	}
	return StringToFrame(s)
}
