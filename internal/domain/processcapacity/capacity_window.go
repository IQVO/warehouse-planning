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
