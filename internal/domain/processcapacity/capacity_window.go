package processcapacity

import (
	"errors"
	"time"
)

// ErrInvalidWindow is returned when a CapacityWindow's end does not come
// strictly after its start. A zero-length window is rejected along with
// any inverted one -- a capacity window with no duration cannot host a
// meaningful rate.
var ErrInvalidWindow = errors.New("processcapacity: capacity window end must be strictly after start")

// CapacityWindow is the [start, end) period a capacity value is valid for.
// A capacity number with no window is incomplete by definition (see
// domain-model.md).
type CapacityWindow struct {
	start time.Time
	end   time.Time
}

// NewCapacityWindow constructs a CapacityWindow, rejecting end <= start.
func NewCapacityWindow(start, end time.Time) (CapacityWindow, error) {
	if !end.After(start) {
		return CapacityWindow{}, ErrInvalidWindow
	}
	return CapacityWindow{start: start, end: end}, nil
}

// Start returns the window's inclusive start.
func (w CapacityWindow) Start() time.Time { return w.start }

// End returns the window's exclusive end.
func (w CapacityWindow) End() time.Time { return w.end }

// Duration returns the window's length (end - start).
func (w CapacityWindow) Duration() time.Duration { return w.end.Sub(w.start) }

// Covers reports whether w covers other: w starts no later than other and
// ends no earlier than other, so [other.start, other.end) lies entirely
// inside [w.start, w.end). A window covers itself (both bounds are
// inclusive of equality). This is the rule a registered constraint's window
// uses to apply to a planning window (docs/adr/0003).
func (w CapacityWindow) Covers(other CapacityWindow) bool {
	return !w.start.After(other.start) && !w.end.Before(other.end)
}

// compareNewestFirst orders windows newest first: the LATER start comes
// first; on equal starts the NARROWER window (earlier end) comes first.
// Negative when a precedes b.
func compareNewestFirst(a, b CapacityWindow) int {
	if c := b.start.Compare(a.start); c != 0 {
		return c
	}
	return a.end.Compare(b.end)
}
