package data

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTypeOfTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		ts       int64
		expected TimestampType
	}{
		{
			name:     "timestamp in seconds - small value",
			ts:       1000000000, // 1e9 - seconds
			expected: TimestampInSeconds,
		},
		{
			name:     "timestamp in seconds - boundary",
			ts:       1000000000000, // 1e12 - exactly at boundary
			expected: TimestampInSeconds,
		},
		{
			name:     "timestamp in milliseconds - just above seconds boundary",
			ts:       1000000000001, // 1e12 + 1 - milliseconds
			expected: TimestampInMillis,
		},
		{
			name:     "timestamp in milliseconds - typical value",
			ts:       1609459200000, // 2021-01-01 00:00:00 UTC in milliseconds
			expected: TimestampInMillis,
		},
		{
			name:     "timestamp in milliseconds - boundary",
			ts:       1000000000000000, // 1e15 - exactly at boundary
			expected: TimestampInMillis,
		},
		{
			name:     "timestamp in microseconds - just above milliseconds boundary",
			ts:       1000000000000001, // 1e15 + 1 - microseconds
			expected: TimestampInMicros,
		},
		{
			name:     "timestamp in microseconds - typical value",
			ts:       1609459200000000, // 2021-01-01 00:00:00 UTC in microseconds
			expected: TimestampInMicros,
		},
		{
			name:     "zero timestamp",
			ts:       0,
			expected: TimestampInSeconds,
		},
		{
			name:     "negative timestamp - should be treated as seconds",
			ts:       -1,
			expected: TimestampInSeconds,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := TypeOfTimestamp(tt.ts)
			if result != tt.expected {
				t.Errorf("TypeOfTimestamp(%d) = %v, want %v", tt.ts, result, tt.expected)
			}
		})
	}
}

func TestAnyTimestampToTime(t *testing.T) {
	// Reference time: 2021-01-01 00:00:00 UTC
	refTime := time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name     string
		ts       int64
		expected time.Time
	}{
		{
			name:     "timestamp in seconds",
			ts:       refTime.Unix(), // 1609459200
			expected: refTime,
		},
		{
			name:     "timestamp in milliseconds",
			ts:       refTime.UnixMilli(), // 1609459200000
			expected: refTime,
		},
		{
			name:     "timestamp in microseconds",
			ts:       refTime.UnixMicro(), // 1609459200000000
			expected: refTime,
		},
		{
			name:     "zero timestamp - should be Unix epoch",
			ts:       0,
			expected: time.Unix(0, 0),
		},
		{
			name:     "negative timestamp in seconds",
			ts:       -1,
			expected: time.Unix(-1, 0),
		},
		{
			name:     "timestamp with fractional microseconds in millis boundary",
			ts:       1609459200000 + 500,                                   // 2021-01-01 00:00:00.5 UTC in milliseconds
			expected: time.Date(2021, 1, 1, 0, 0, 0, 500*1000000, time.UTC), // 500ms = 500*1000000 nanoseconds
		},
		{
			name:     "timestamp with fractional microseconds in micros",
			ts:       1609459200000000 + 500000,                             // 2021-01-01 00:00:00.5 UTC in microseconds
			expected: time.Date(2021, 1, 1, 0, 0, 0, 500000*1000, time.UTC), // 500000 microseconds = 500000*1000 nanoseconds
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := AnyTimestampToTime(tt.ts)
			if result == nil {
				t.Fatalf("AnyTimestampToTime(%d) returned nil", tt.ts)
			}

			if !result.Equal(tt.expected) {
				t.Errorf("AnyTimestampToTime(%d) = %v, want %v", tt.ts, result, tt.expected)
			}
		})
	}
}

func TestFrameStringRoundTrip(t *testing.T) {
	for _, s := range []string{"1m", "1h", "1d", "5m", "15m"} {
		t.Run(s, func(t *testing.T) {
			f := StringToFrame(s)
			assert.Equal(t, s, f.String())
		})
	}
	assert.Equal(t, "", StringToFrame("invalid").String())
	assert.Equal(t, "1m", NewFrame("").String())
}

func TestAnyTimestampToTimeIntegration(t *testing.T) {
	t.Run("round trip conversion maintains timestamp type detection", func(t *testing.T) {
		testCases := []struct {
			originalTs int64
			desc       string
		}{
			{1609459200, "seconds timestamp"},            // 2021-01-01 00:00:00 UTC
			{1609459200000, "milliseconds timestamp"},    // 2021-01-01 00:00:00 UTC
			{1609459200000000, "microseconds timestamp"}, // 2021-01-01 00:00:00 UTC
		}

		for _, tc := range testCases {
			t.Run(tc.desc, func(t *testing.T) {
				// Convert to time
				convertedTime := AnyTimestampToTime(tc.originalTs)
				if convertedTime == nil {
					t.Fatalf("AnyTimestampToTime(%d) returned nil", tc.originalTs)
				}

				// Convert back to timestamp in the same unit
				var backToTs int64
				tsType := TypeOfTimestamp(tc.originalTs)
				switch tsType {
				case TimestampInSeconds:
					backToTs = convertedTime.Unix()
				case TimestampInMillis:
					backToTs = convertedTime.UnixMilli()
				case TimestampInMicros:
					backToTs = convertedTime.UnixMicro()
				}

				// Should match original (allowing for precision loss in conversion)
				if backToTs != tc.originalTs {
					t.Errorf("Round trip failed: original %d -> time -> %d (type: %v)",
						tc.originalTs, backToTs, tsType)
				}
			})
		}
	})
}
