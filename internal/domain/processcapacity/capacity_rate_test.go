package processcapacity

import (
	"errors"
	"testing"
	"time"
)

func TestNewCapacityRate_RejectsNegativeQuantity(t *testing.T) {
	_, err := NewCapacityRate(-1, UnitUnit, time.Hour)
	if !errors.Is(err, ErrNegativeQuantity) {
		t.Fatalf("expected ErrNegativeQuantity, got %v", err)
	}
}

func TestNewCapacityRate_AcceptsExactZeroQuantity(t *testing.T) {
	rate, err := NewCapacityRate(0, UnitUnit, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rate.Quantity() != 0 {
		t.Fatalf("expected quantity 0, got %v", rate.Quantity())
	}
}

func TestNewCapacityRate_RejectsNonPositivePeriod(t *testing.T) {
	cases := []time.Duration{0, -time.Hour}
	for _, period := range cases {
		if _, err := NewCapacityRate(100, UnitUnit, period); !errors.Is(err, ErrNonPositivePeriod) {
			t.Fatalf("period %v: expected ErrNonPositivePeriod, got %v", period, err)
		}
	}
}

func TestCapacityRate_LessThan_DistinctNonZeroValues(t *testing.T) {
	low, err := NewCapacityRate(3500, UnitUnit, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	high, err := NewCapacityRate(5000, UnitUnit, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !low.LessThan(high) {
		t.Fatalf("expected %v to be less than %v", low, high)
	}
	if high.LessThan(low) {
		t.Fatalf("did not expect %v to be less than %v", high, low)
	}
	if low.LessThan(low) {
		t.Fatalf("did not expect a rate to be less than itself")
	}
}

// TestCapacityRate_LessThan_AcrossDifferentPeriods proves the per-second
// normalization: 100/hour is a SMALLER throughput than 100/30min, even
// though both carry the same raw quantity, because the 30-minute period
// delivers that quantity twice as fast.
func TestCapacityRate_LessThan_AcrossDifferentPeriods(t *testing.T) {
	perHour, err := NewCapacityRate(100, UnitUnit, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	per30Min, err := NewCapacityRate(100, UnitUnit, 30*time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !perHour.LessThan(per30Min) {
		t.Fatalf("expected 100/hour to be less than 100/30min once normalized per-second")
	}
	if per30Min.LessThan(perHour) {
		t.Fatalf("did not expect 100/30min to be less than 100/hour")
	}
}
