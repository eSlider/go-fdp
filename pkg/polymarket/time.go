package polymarket

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Time is a UTC-normalized timestamp for Polymarket Gamma JSON fields.
type Time time.Time

func (t *Time) UnmarshalJSON(b []byte) error {
	if len(b) == 0 {
		return errors.New("empty time")
	}
	switch string(b) {
	case "null":
		*t = Time{}
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("time string: %w", err)
	}
	if s == "" {
		*t = Time{}
		return nil
	}
	parsed, err := parseRFC3339(s)
	if err != nil {
		return err
	}
	*t = Time(parsed.UTC())
	return nil
}

func parseRFC3339(s string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, s)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339Nano, s)
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid time format: %w", err)
	}
	return parsed, nil
}

// IsZero reports whether the time is unset.
func (t Time) IsZero() bool {
	return time.Time(t).IsZero()
}

// Time returns the underlying time.Time in UTC.
func (t Time) Time() time.Time {
	return time.Time(t).UTC()
}
