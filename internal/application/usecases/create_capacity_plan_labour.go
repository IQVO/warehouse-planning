package usecases

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// resolveLabour computes the labour each step of the plan's path requires
// (docs/adr/0022 decisions 4 and 5), in path order. A step's process type
// names its task type when they are equal; the standard in force is read from
// the local copy for the plan's site (a facility-layout site code, the
// standards' site scope) at the window start. The requirement is computed
// now and stored with the plan, so a later revision never rewrites it.
func (uc *CreateCapacityPlan) resolveLabour(
	ctx context.Context,
	siteID string,
	window processcapacity.CapacityWindow,
	demand capacityplan.LabourDemand,
	steps []processcapacity.StepResult,
) ([]capacityplan.LabourRequirement, error) {
	out := make([]capacityplan.LabourRequirement, 0, len(steps))
	for _, step := range steps {
		standard, err := uc.standardFor(ctx, siteID, step.Step, window.Start())
		if err != nil {
			return nil, err
		}
		out = append(out, capacityplan.RequireLabour(step.Step, standard, demand, window))
	}
	return out, nil
}

// standardFor reads the standard of one step's task type in force for the
// site at the window start; no copy wired or no standard is not found.
func (uc *CreateCapacityPlan) standardFor(ctx context.Context, siteID string, step processcapacity.ProcessType, at time.Time) (capacityplan.StepStandard, error) {
	if uc.LaborStandards == nil {
		return capacityplan.StepStandard{}, nil
	}
	s, found, err := uc.LaborStandards.InForce(ctx, siteID, string(step), at)
	if err != nil {
		return capacityplan.StepStandard{}, fmt.Errorf("read labour standard: %w", err)
	}
	if !found {
		return capacityplan.StepStandard{}, nil
	}
	return capacityplan.StepStandard{Found: true, Seconds: float64(s.ExpectedSeconds()), SiteScoped: s.SiteScoped()}, nil
}

// taskFactors keys the request's tasks-per-order factors by process type; nil
// stays nil (no factor for any step).
func taskFactors(raw map[string]float64) map[processcapacity.ProcessType]float64 {
	if raw == nil {
		return nil
	}
	out := make(map[processcapacity.ProcessType]float64, len(raw))
	for step, factor := range raw {
		out[processcapacity.ProcessType(step)] = factor
	}
	return out
}
