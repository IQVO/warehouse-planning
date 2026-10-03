package usecases

import (
	"context"
	"errors"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// ErrProcessPathNotFound is returned when no ProcessPath is registered
// under the requested id.
var ErrProcessPathNotFound = errors.New("usecases: no ProcessPath registered for this id")

// GetProcessPathCapacityCommand carries everything needed to compute one
// ProcessPath's normalized, end-to-end capacity for one location and
// window.
//
// UnitsPerOrder/PackagesPerOrder carry the caller's WorkloadProfile
// factors directly on the request. PHASE 2 SIMPLIFICATION: there is no
// dedicated WorkloadProfile persistence yet -- a later phase will load a
// warehouse's profile from its own store instead of requiring the caller
// to pass it on every request.
type GetProcessPathCapacityCommand struct {
	ProcessPathID string
	Location      string
	WindowStart   time.Time
	WindowEnd     time.Time

	UnitsPerOrder    *float64
	PackagesPerOrder *float64
}

// GetProcessPathCapacityResult reports the normalized rate and its
// bottleneck step.
type GetProcessPathCapacityResult struct {
	NormalizedRate processcapacity.CapacityRate
	BottleneckStep processcapacity.ProcessType
}

// GetProcessPathCapacity resolves a ProcessPath's steps' ProcessCapacity
// plus the requested WorkloadProfile, and computes the path's normalized
// capacity and bottleneck step (see domain-model.md's ProcessPathCapacity
// definition and processcapacity.ComputeProcessPathCapacity).
type GetProcessPathCapacity struct {
	ProcessPaths      ports.ProcessPathRepository
	ProcessCapacities ports.ProcessCapacityRepository
}

// Handle loads the ProcessPath identified by cmd.ProcessPathID, loads each
// of its steps' ProcessCapacity for cmd's location and window (reusing the
// existing ProcessCapacityRepository), builds the WorkloadProfile from
// cmd's factors, and delegates to
// processcapacity.ComputeProcessPathCapacity. Returns ErrProcessPathNotFound
// if no ProcessPath is registered under cmd.ProcessPathID. A step with no
// registered ProcessCapacity is left absent from the capacities map on
// purpose -- ComputeProcessPathCapacity itself returns the explicit
// ErrMissingStepCapacity for any step absent there, so this use case never
// needs to special-case "not found" per step itself.
func (uc *GetProcessPathCapacity) Handle(ctx context.Context, cmd GetProcessPathCapacityCommand) (GetProcessPathCapacityResult, error) {
	path, err := uc.ProcessPaths.FindByID(ctx, cmd.ProcessPathID)
	if err != nil {
		return GetProcessPathCapacityResult{}, err
	}
	if path == nil {
		return GetProcessPathCapacityResult{}, ErrProcessPathNotFound
	}

	profile, err := processcapacity.NewWorkloadProfile(cmd.UnitsPerOrder, cmd.PackagesPerOrder)
	if err != nil {
		return GetProcessPathCapacityResult{}, err
	}

	steps := path.Steps()
	capacities := make(map[processcapacity.ProcessType]*processcapacity.ProcessCapacity, len(steps))
	for _, step := range steps {
		processType := processcapacity.ProcessType(step)
		pc, err := uc.ProcessCapacities.FindByProcessLocationWindow(ctx, processType, cmd.Location, cmd.WindowStart, cmd.WindowEnd)
		if err != nil {
			return GetProcessPathCapacityResult{}, err
		}
		if pc != nil {
			capacities[processType] = pc
		}
	}

	rate, bottleneck, err := processcapacity.ComputeProcessPathCapacity(*path, capacities, profile)
	if err != nil {
		return GetProcessPathCapacityResult{}, err
	}

	return GetProcessPathCapacityResult{NormalizedRate: rate, BottleneckStep: bottleneck}, nil
}
