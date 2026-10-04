package report

import (
	"errors"
	"fmt"
	"time"
)

// Range is the half-open time range [From, To) every report answers for:
// a fact at exactly From is in, a fact at exactly To is out.
type Range struct {
	From time.Time
	To   time.Time
}

// DefaultWindow is the range used when from/to are omitted: the 30 days
// ending at "now". MaxWindow caps a caller-chosen range. They are functions,
// not constants, so mutation testing can reach (and the tests pin) the
// arithmetic.
func DefaultWindow() time.Duration { return 30 * 24 * time.Hour }

// MaxWindow is the longest range a caller may ask for: 366 days.
func MaxWindow() time.Duration { return 366 * 24 * time.Hour }

// Range errors; the HTTP adapter maps each to an RFC 7807 400.
var (
	ErrInvalidFrom   = errors.New("from must be an RFC 3339 timestamp")
	ErrInvalidTo     = errors.New("to must be an RFC 3339 timestamp")
	ErrEmptyRange    = errors.New("from must be strictly before to")
	ErrRangeTooLarge = errors.New("the range must not exceed 366 days")
)

// ParseRange validates the raw `from` / `to` query values (RFC 3339, "" =
// omitted) against now. Omitted bounds default so the range is the 30 days
// ending at now: no bounds -> [now-30d, now); only `to` -> [to-30d, to);
// only `from` -> [from, now). The result is in UTC and is rejected when
// empty/inverted (from >= to) or longer than MaxWindow.
func ParseRange(fromRaw, toRaw string, now time.Time) (Range, error) {
	to := now.UTC()
	if toRaw != "" {
		t, err := time.Parse(time.RFC3339, toRaw)
		if err != nil {
			return Range{}, fmt.Errorf("%w: %q", ErrInvalidTo, toRaw)
		}
		to = t.UTC()
	}
	from := to.Add(-DefaultWindow())
	if fromRaw != "" {
		t, err := time.Parse(time.RFC3339, fromRaw)
		if err != nil {
			return Range{}, fmt.Errorf("%w: %q", ErrInvalidFrom, fromRaw)
		}
		from = t.UTC()
	}
	if from.Compare(to) >= 0 {
		return Range{}, ErrEmptyRange
	}
	if to.Sub(from) > MaxWindow() {
		return Range{}, ErrRangeTooLarge
	}
	return Range{From: from, To: to}, nil
}
