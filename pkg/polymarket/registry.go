package polymarket

import (
	"fmt"
	"time"

	"github.com/eslider/go-fdp/pkg/data"
)

// AllFrames are prediction frames supported by the poller and API.
var AllFrames = []data.Frame{
	data.Minute,
	data.FiveMinute,
	data.FifteenMin,
	data.Hour,
	4 * data.Hour,
}

// AlignWindowStart returns UTC window start for frame-aligned slug epochs.
func AlignWindowStart(t time.Time, frame data.Frame) time.Time {
	t = t.UTC()
	d := time.Duration(frame)
	if d <= 0 {
		return t.Truncate(time.Minute)
	}
	unix := t.Unix()
	secs := int64(d / time.Second)
	if secs <= 0 {
		return t
	}
	aligned := (unix / secs) * secs
	return time.Unix(aligned, 0).UTC()
}

// SlugForWindow builds the primary event slug for a frame and window start.
func SlugForWindow(frame data.Frame, windowStart time.Time) string {
	epoch := AlignWindowStart(windowStart, frame).Unix()
	switch frame {
	case data.Minute:
		// 1m uses the enclosing 5m market slug for discovery.
		epoch = AlignWindowStart(windowStart, data.FiveMinute).Unix()
		return fmt.Sprintf("btc-updown-5m-%d", epoch)
	case data.FiveMinute:
		return fmt.Sprintf("btc-updown-5m-%d", epoch)
	case data.FifteenMin:
		return fmt.Sprintf("btc-updown-15m-%d", epoch)
	case data.Hour:
		return fmt.Sprintf("btc-updown-1h-%d", epoch)
	case 4 * data.Hour:
		return fmt.Sprintf("btc-updown-4h-%d", epoch)
	default:
		return fmt.Sprintf("btc-updown-5m-%d", AlignWindowStart(windowStart, data.FiveMinute).Unix())
	}
}

// NativeFrameDuration is the Polymarket market window for a frame when native slug exists.
func NativeFrameDuration(frame data.Frame) time.Duration {
	switch frame {
	case data.Minute:
		return 5 * time.Minute
	case data.FiveMinute:
		return 5 * time.Minute
	case data.FifteenMin:
		return 15 * time.Minute
	case data.Hour:
		return time.Hour
	case 4 * data.Hour:
		return 4 * time.Hour
	default:
		return 5 * time.Minute
	}
}

// WindowsInRange yields frame-aligned window starts in [from, to).
func WindowsInRange(from, to time.Time, frame data.Frame) []time.Time {
	from = from.UTC()
	to = to.UTC()
	d := NativeFrameDuration(frame)
	if d <= 0 {
		d = time.Duration(frame)
	}
	start := AlignWindowStart(from, frame)
	var out []time.Time
	for cur := start; cur.Before(to); cur = cur.Add(d) {
		if !cur.Before(from.Add(-d)) {
			out = append(out, cur)
		}
	}
	return out
}
