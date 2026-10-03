package processcapacity

import (
	"errors"
	"testing"
	"time"
)

func TestNewCapacityWindow_RejectsEndBeforeStart(t *testing.T) {
	start := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	if _, err := NewCapacityWindow(start, end); !errors.Is(err, ErrInvalidWindow) {
		t.Fatalf("expected ErrInvalidWindow, got %v", err)
	}
}

func TestNewCapacityWindow_RejectsZeroLengthWindow(t *testing.T) {
	instant := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	if _, err := NewCapacityWindow(instant, instant); !errors.Is(err, ErrInvalidWindow) {
		t.Fatalf("expected ErrInvalidWindow for a zero-length window, got %v", err)
	}
}

func TestNewCapacityWindow_AcceptsStrictlyIncreasingWindow(t *testing.T) {
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)
	window, err := NewCapacityWindow(start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !window.Start().Equal(start) || !window.End().Equal(end) {
		t.Fatalf("expected start=%v end=%v, got start=%v end=%v", start, end, window.Start(), window.End())
	}
}

func TestCapacityWindow_Duration(t *testing.T) {
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)
	window, err := NewCapacityWindow(start, end)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if window.Duration() != time.Hour {
		t.Fatalf("expected duration of 1h, got %v", window.Duration())
	}
}
