package integrity

// Severity classifies how serious a gap issue is.
type Severity string

const (
	SeverityError   Severity = "error"
	SeverityWarning Severity = "warning"
)

// Code identifies the kind of time-series integrity problem.
type Code string

const (
	CodeDuplicateOpenTime     Code = "duplicate_open_time"
	CodeOpenTimeNotIncreasing Code = "open_time_not_increasing"
	CodeWrongStep             Code = "wrong_step"
	CodeMissingInterval       Code = "missing_interval"
	CodeOutOfWindow           Code = "out_of_window"
	CodePaginationSuspect     Code = "pagination_suspect"
	CodeCountMismatch         Code = "count_mismatch"
	CodeHourBoundaryGap       Code = "hour_boundary_gap"
)

// Issue describes a single integrity finding.
type Issue struct {
	Code     Code
	Severity Severity
	OpenTime int64
	Detail   string
}
