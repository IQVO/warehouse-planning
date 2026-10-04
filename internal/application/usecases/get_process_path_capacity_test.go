package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// f64ptr is a small helper so fixtures can express "this optional
// WorkloadProfile factor is set to exactly this value" without a
// throwaway local variable at every call site.
func f64ptr(v float64) *float64 { return &v }

// seedPickRebinPackPathCapacity registers the design doc's Pick -> Rebin
// -> Pack worked example (PICK=4000 UNIT/HOUR, REBIN=2500 UNIT/HOUR,
// PACK=1800 PACKAGE/HOUR, all at PATH-ZONE-A) plus the matching
// ProcessPath, through the SAME use cases + in-memory repos the HTTP
// handler wires -- proving the two layers agree.
func seedPickRebinPackPathCapacity(t *testing.T, ctx context.Context, pcRepo *memory.ProcessCapacityRepo, ppRepo *memory.ProcessPathRepo, start, end time.Time) {
	t.Helper()
	pcUC := &RegisterProcessCapacityConstraint{Repo: pcRepo}

	registrations := []struct {
		processType processcapacity.ProcessType
		quantity    float64
		unit        processcapacity.CapacityUnit
	}{
		{"PICK", 4000, processcapacity.UnitUnit},
		{"REBIN", 2500, processcapacity.UnitUnit},
		{"PACK", 1800, processcapacity.UnitPackage},
	}
	for _, reg := range registrations {
		if _, err := pcUC.Handle(ctx, RegisterProcessCapacityConstraintCommand{
			ProcessType:    reg.processType,
			Location:       "PATH-ZONE-A",
			WindowStart:    start,
			WindowEnd:      end,
			ConstraintType: processcapacity.ConstraintLabor,
			Quantity:       reg.quantity,
			Unit:           reg.unit,
			Period:         time.Hour,
		}); err != nil {
			t.Fatalf("unexpected error registering %v: %v", reg.processType, err)
		}
	}

	ppUC := &RegisterProcessPath{Repo: ppRepo}
	if _, err := ppUC.Handle(ctx, RegisterProcessPathCommand{
		ID:    "pick-rebin-pack",
		Name:  "Pick-Rebin-Pack",
		Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"},
	}); err != nil {
		t.Fatalf("unexpected error registering the ProcessPath: %v", err)
	}
}

// TestGetProcessPathCapacity_WorkedExample reproduces, byte-for-byte, the
// design doc's Pick -> Rebin -> Pack worked example through the use case +
// in-memory repos: the normalized path capacity must be 1000 ORDER/HOUR
// with REBIN as the bottleneck.
func TestGetProcessPathCapacity_WorkedExample(t *testing.T) {
	pcRepo := memory.NewProcessCapacityRepo()
	ppRepo := memory.NewProcessPathRepo()
	ctx := context.Background()
	start, end := pickZoneAWindowTimes()

	seedPickRebinPackPathCapacity(t, ctx, pcRepo, ppRepo, start, end)

	uc := &GetProcessPathCapacity{ProcessPaths: ppRepo, ProcessCapacities: pcRepo, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()}
	result, err := uc.Handle(ctx, GetProcessPathCapacityCommand{
		ProcessPathID:    "pick-rebin-pack",
		Location:         "PATH-ZONE-A",
		WindowStart:      start,
		WindowEnd:        end,
		UnitsPerOrder:    f64ptr(2.5),
		PackagesPerOrder: f64ptr(1),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.BottleneckStep != "REBIN" {
		t.Fatalf("expected REBIN to be the bottleneck, got %v", result.BottleneckStep)
	}
	if result.NormalizedRate.Quantity() != 1000 || result.NormalizedRate.Unit() != processcapacity.UnitOrder {
		t.Fatalf("expected 1000 ORDER/HOUR, got %v %v", result.NormalizedRate.Quantity(), result.NormalizedRate.Unit())
	}
}

func TestGetProcessPathCapacity_ProcessPathNotFound(t *testing.T) {
	pcRepo := memory.NewProcessCapacityRepo()
	ppRepo := memory.NewProcessPathRepo()
	start, end := pickZoneAWindowTimes()

	uc := &GetProcessPathCapacity{ProcessPaths: ppRepo, ProcessCapacities: pcRepo, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()}
	_, err := uc.Handle(context.Background(), GetProcessPathCapacityCommand{
		ProcessPathID: "nowhere",
		Location:      "PATH-ZONE-A",
		WindowStart:   start,
		WindowEnd:     end,
	})
	if !errors.Is(err, ErrProcessPathNotFound) {
		t.Fatalf("expected ErrProcessPathNotFound, got %v", err)
	}
}

// TestGetProcessPathCapacity_MissingStepCapacity proves a path step with
// no registered ProcessCapacity for the requested location/window
// surfaces processcapacity.ErrMissingStepCapacity, naming the step --
// never a false zero bottleneck.
func TestGetProcessPathCapacity_MissingStepCapacity(t *testing.T) {
	pcRepo := memory.NewProcessCapacityRepo()
	ppRepo := memory.NewProcessPathRepo()
	ctx := context.Background()
	start, end := pickZoneAWindowTimes()

	pcUC := &RegisterProcessCapacityConstraint{Repo: pcRepo}
	if _, err := pcUC.Handle(ctx, RegisterProcessCapacityConstraintCommand{
		ProcessType:    "PICK",
		Location:       "PATH-ZONE-B",
		WindowStart:    start,
		WindowEnd:      end,
		ConstraintType: processcapacity.ConstraintLabor,
		Quantity:       4000,
		Unit:           processcapacity.UnitUnit,
		Period:         time.Hour,
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	ppUC := &RegisterProcessPath{Repo: ppRepo}
	if _, err := ppUC.Handle(ctx, RegisterProcessPathCommand{
		ID:    "pick-only",
		Name:  "Pick-Only",
		Steps: []processpath.ProcessType{"PICK", "REBIN"},
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	uc := &GetProcessPathCapacity{ProcessPaths: ppRepo, ProcessCapacities: pcRepo, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()}
	_, err := uc.Handle(ctx, GetProcessPathCapacityCommand{
		ProcessPathID:    "pick-only",
		Location:         "PATH-ZONE-B",
		WindowStart:      start,
		WindowEnd:        end,
		UnitsPerOrder:    f64ptr(2.5),
		PackagesPerOrder: f64ptr(1),
	})
	if !errors.Is(err, processcapacity.ErrMissingStepCapacity) {
		t.Fatalf("expected ErrMissingStepCapacity, got %v", err)
	}
}

func TestGetProcessPathCapacity_RejectsInvalidWorkloadProfile(t *testing.T) {
	pcRepo := memory.NewProcessCapacityRepo()
	ppRepo := memory.NewProcessPathRepo()
	ctx := context.Background()
	start, end := pickZoneAWindowTimes()

	seedPickRebinPackPathCapacity(t, ctx, pcRepo, ppRepo, start, end)

	uc := &GetProcessPathCapacity{ProcessPaths: ppRepo, ProcessCapacities: pcRepo, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()}
	_, err := uc.Handle(ctx, GetProcessPathCapacityCommand{
		ProcessPathID:    "pick-rebin-pack",
		Location:         "PATH-ZONE-A",
		WindowStart:      start,
		WindowEnd:        end,
		UnitsPerOrder:    f64ptr(0), // invalid: zero conversion factor
		PackagesPerOrder: f64ptr(1),
	})
	if !errors.Is(err, processcapacity.ErrNonPositiveConversionFactor) {
		t.Fatalf("expected ErrNonPositiveConversionFactor, got %v", err)
	}
}
