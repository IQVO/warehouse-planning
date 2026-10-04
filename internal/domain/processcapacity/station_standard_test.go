package processcapacity

import (
	"errors"
	"testing"
	"time"
)

func TestNewStationStandard_AcceptsEveryNormalizableUnit(t *testing.T) {
	for _, unit := range []CapacityUnit{UnitUnit, UnitPackage, UnitOrder} {
		std, err := NewStationStandard("SIM1", "PACK", mustRate(t, 180, unit, time.Hour))
		if err != nil {
			t.Fatalf("unit %s: unexpected error: %v", unit, err)
		}
		if std.Location() != "SIM1" || std.ProcessType() != "PACK" {
			t.Fatalf("unit %s: got key %s/%s, want SIM1/PACK", unit, std.Location(), std.ProcessType())
		}
		if got := std.PerStation(); got.Quantity() != 180 || got.Unit() != unit || got.Period() != time.Hour {
			t.Fatalf("unit %s: got %v %s per %v, want 180 per hour", unit, got.Quantity(), got.Unit(), got.Period())
		}
	}
}

func TestNewStationStandard_Rejections(t *testing.T) {
	tests := []struct {
		name     string
		location string
		process  ProcessType
		rate     CapacityRate
		want     error
	}{
		{"blank location", "", "PACK", mustRate(t, 180, UnitPackage, time.Hour), ErrStationStandardRequiredField},
		{"blank process", "SIM1", "", mustRate(t, 180, UnitPackage, time.Hour), ErrStationStandardRequiredField},
		{"zero throughput", "SIM1", "PACK", mustRate(t, 0, UnitPackage, time.Hour), ErrNonPositiveStationStandard},
		{"LINE cannot be normalized", "SIM1", "PACK", mustRate(t, 180, UnitLine, time.Hour), ErrUnsupportedNormalizationUnit},
		{"unknown unit", "SIM1", "PACK", mustRate(t, 180, CapacityUnit("PALLET"), time.Hour), ErrUnsupportedNormalizationUnit},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewStationStandard(tc.location, tc.process, tc.rate); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

// The smallest positive throughput is valid: the guard is "must be positive".
func TestNewStationStandard_SmallestPositiveQuantityIsAccepted(t *testing.T) {
	if _, err := NewStationStandard("SIM1", "PACK", mustRate(t, 0.5, UnitPackage, time.Hour)); err != nil {
		t.Fatalf("0.5 per hour must be accepted, got %v", err)
	}
}

func TestStationStandard_CapacityFor_MultipliesPerStationRate(t *testing.T) {
	std, err := NewStationStandard("SIM1", "PACK", mustRate(t, 180.5, UnitPackage, 30*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	got, err := std.CapacityFor(7)
	if err != nil {
		t.Fatal(err)
	}
	if got.Quantity() != 1263.5 || got.Unit() != UnitPackage || got.Period() != 30*time.Minute {
		t.Fatalf("7 x 180.5 PACKAGE/30min = %v %s per %v, want 1263.5 PACKAGE per 30m", got.Quantity(), got.Unit(), got.Period())
	}
}

func TestStationStandard_CapacityFor_RejectsNegativeCount(t *testing.T) {
	std, _ := NewStationStandard("SIM1", "PACK", mustRate(t, 180, UnitPackage, time.Hour))
	if _, err := std.CapacityFor(-1); !errors.Is(err, ErrNegativeQuantity) {
		t.Fatalf("got %v, want ErrNegativeQuantity", err)
	}
}
