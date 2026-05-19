package integrity

import "time"

// IsError reports error severity.
func (i Issue) IsError() bool {
	return i.Severity == SeverityError
}

// IsStructural reports duplicate, ordering, or step errors.
func (i Issue) IsStructural() bool {
	if !i.IsError() {
		return false
	}
	switch i.Code {
	case CodeDuplicateOpenTime, CodeOpenTimeNotIncreasing, CodeWrongStep:
		return true
	default:
		return false
	}
}

// BlocksRead reports gaps that must block API responses.
func (i Issue) BlocksRead() bool {
	if !i.IsError() {
		return false
	}
	switch i.Code {
	case CodeMissingInterval, CodeWrongStep, CodeHourBoundaryGap,
		CodeDuplicateOpenTime, CodeOpenTimeNotIncreasing:
		return true
	default:
		return false
	}
}

// NeedsRepair reports issues that should trigger ETL or seal repair.
func (i Issue) NeedsRepair() bool {
	if !i.IsError() {
		return false
	}
	switch i.Code {
	case CodeMissingInterval, CodeWrongStep, CodeHourBoundaryGap, CodeCountMismatch,
		CodeDuplicateOpenTime, CodeOpenTimeNotIncreasing:
		return true
	default:
		return false
	}
}

// AffectsRange reports whether the issue impacts [from, to).
func (i Issue) AffectsRange(from, to time.Time) bool {
	fromMs, toMs := from.UnixMilli(), to.UnixMilli()
	switch i.Code {
	case CodeOutOfWindow:
		return false
	case CodeCountMismatch:
		if i.OpenTime < 0 || i.OpenTime > 23 {
			return true
		}
		hourStart := time.Date(from.Year(), from.Month(), from.Day(), int(i.OpenTime), 0, 0, 0, time.UTC).UnixMilli()
		hourEnd := hourStart + int64(time.Hour/time.Millisecond)
		return hourEnd > fromMs && hourStart < toMs
	default:
		if i.OpenTime == 0 {
			return true
		}
		return i.OpenTime >= fromMs && i.OpenTime < toMs
	}
}
