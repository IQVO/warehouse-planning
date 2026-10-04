package usecases

import (
	"context"
	"errors"
	"fmt"
	"strings"
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

// GetProcessPathCapacityResult reports the normalized rate, its bottleneck
// step and the constraint type binding that step, every step's composed
// result in path order, and the composition warnings.
type GetProcessPathCapacityResult struct {
	NormalizedRate       processcapacity.CapacityRate
	BottleneckStep       processcapacity.ProcessType
	BottleneckConstraint processcapacity.ConstraintType
	Steps                []processcapacity.StepResult
	Warnings             []string
}

// GetProcessPathCapacity resolves a ProcessPath's steps' capacity and the
// requested WorkloadProfile, and computes the path's normalized capacity,
// bottleneck step and binding constraint (see domain-model.md's
// ProcessPathCapacity definition and
// processcapacity.ComposeProcessPathCapacity).
//
// Capacity is composed AT READ TIME (ADR 0002): per step the candidates are
// the constraints of the ProcessCapacity aggregates of (process, location)
// whose window COVERS the requested window (ADR 0003: newest window start wins
// per constraint type), plus a derived STATION constraint = the stations
// tallied across the site's zones x the operator-declared StationStandard.
// Nothing derived is ever stored, so late declarations and facility changes
// are picked up on the next read.
type GetProcessPathCapacity struct {
	ProcessPaths      ports.ProcessPathRepository
	ProcessCapacities ports.ProcessCapacityRepository
	StationStandards  ports.StationStandardRepository
	Tally             ports.StorageTallyReader
}

// Handle loads the ProcessPath identified by cmd.ProcessPathID, gathers each
// step's covering ProcessCapacity aggregates for cmd's location and window, its
// tallied station count and its StationStandard, builds the WorkloadProfile
// from cmd's factors, and delegates to
// processcapacity.ComposeProcessPathCapacity. Returns ErrProcessPathNotFound
// if no ProcessPath is registered under cmd.ProcessPathID and
// processcapacity.ErrInvalidWindow when the window's end is not after its
// start (a covering lookup of an inverted window would match nonsense). A step
// with no candidate at all is left without a covering capacity on purpose --
// ComposeProcessPathCapacity itself returns the explicit
// ErrMissingStepCapacity, so this use case never special-cases "not found"
// per step.
func (uc *GetProcessPathCapacity) Handle(ctx context.Context, cmd GetProcessPathCapacityCommand) (GetProcessPathCapacityResult, error) {
	path, err := uc.ProcessPaths.FindByID(ctx, cmd.ProcessPathID)
	if err != nil {
		return GetProcessPathCapacityResult{}, err
	}
	if path == nil {
		return GetProcessPathCapacityResult{}, ErrProcessPathNotFound
	}

	if _, err := processcapacity.NewCapacityWindow(cmd.WindowStart, cmd.WindowEnd); err != nil {
		return GetProcessPathCapacityResult{}, err
	}

	profile, err := processcapacity.NewWorkloadProfile(cmd.UnitsPerOrder, cmd.PackagesPerOrder)
	if err != nil {
		return GetProcessPathCapacityResult{}, err
	}

	steps := path.Steps()
	inputs := make(map[processcapacity.ProcessType]processcapacity.StepInput, len(steps))
	for _, step := range steps {
		processType := processcapacity.ProcessType(step)
		input, err := uc.stepInput(ctx, processType, cmd)
		if err != nil {
			return GetProcessPathCapacityResult{}, err
		}
		inputs[processType] = input
	}

	composed, err := processcapacity.ComposeProcessPathCapacity(*path, inputs, profile)
	if errors.Is(err, processcapacity.ErrMissingStepCapacity) {
		// Say WHY the step has no data: the window is not covered (ADR 0003).
		return GetProcessPathCapacityResult{}, fmt.Errorf("no registered capacity window at %s covers [%s, %s) and no station constraint applies: %w",
			cmd.Location, cmd.WindowStart.UTC().Format(time.RFC3339), cmd.WindowEnd.UTC().Format(time.RFC3339), err)
	}
	if err != nil {
		return GetProcessPathCapacityResult{}, err
	}
	return GetProcessPathCapacityResult{
		NormalizedRate:       composed.Rate,
		BottleneckStep:       composed.BottleneckStep,
		BottleneckConstraint: composed.BottleneckConstraint,
		Steps:                composed.Steps,
		Warnings:             composed.Warnings,
	}, nil
}

// stepInput gathers one step's candidates: the ProcessCapacity aggregates of
// (step, location) covering the window (ProcessCapacityRepository.FindCovering),
// the site's tallied station count for the
// step's activity (the step name, upper-cased like the tally keys) and -- only
// when stations exist -- the declared StationStandard.
func (uc *GetProcessPathCapacity) stepInput(ctx context.Context, step processcapacity.ProcessType, cmd GetProcessPathCapacityCommand) (processcapacity.StepInput, error) {
	input := processcapacity.StepInput{Location: cmd.Location}

	covering, err := uc.ProcessCapacities.FindCovering(ctx, step, cmd.Location, cmd.WindowStart, cmd.WindowEnd)
	if err != nil {
		return processcapacity.StepInput{}, err
	}
	input.Covering = covering

	count, err := uc.Tally.StationCount(ctx, cmd.Location, strings.ToUpper(string(step)))
	if err != nil {
		return processcapacity.StepInput{}, err
	}
	input.StationCount = count
	if count > 0 {
		standard, err := uc.StationStandards.Find(ctx, cmd.Location, step)
		if err != nil {
			return processcapacity.StepInput{}, err
		}
		input.Standard = standard
	}
	return input, nil
}
