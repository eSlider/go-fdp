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

func TypeOfTimestamp(ts int64) TimestampType {
	switch {
	case ts > 1e18:
		return TimestampInMicros
	case ts > 1e12:
		return TimestampInMillis
	default:
		return TimestampInSeconds
	}
}

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
