package processcapacity

import (
	"errors"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ErrMissingStepCapacity is returned by ComputeProcessPathCapacity when a
// ProcessPath step has no entry in the capacities map. A missing step is
// never silently treated as zero or infinite capacity -- that would either
// falsely report a zero-throughput bottleneck or (worse) skip the step
// entirely and understate how constrained the path really is.
var ErrMissingStepCapacity = errors.New("processcapacity: missing capacity data for step")

// PathCapacityResult is a composed ProcessPathCapacity: the end-to-end rate
// (ORDER), the bottleneck step and the constraint type binding it, every
// step's own composed result in path order, and the warnings raised.
type PathCapacityResult struct {
	Rate                 CapacityRate
	BottleneckStep       ProcessType
	BottleneckConstraint ConstraintType
	Steps                []StepResult
	Warnings             []string
}

// ComposeProcessPathCapacity is a domain SERVICE -- a plain function, not a
// stored aggregate -- that computes a ProcessPath's end-to-end,
// WorkloadProfile-normalized capacity (see domain-model.md's
// ProcessPathCapacity definition). Each step is composed by
// ComposeStepCapacity from its StepInput (a step absent from inputs has no
// capacity data at all); the path's rate is the MINIMUM normalized step rate
// and its bottleneck is the earliest step producing it, together with that
// step's binding constraint type.
//
// Returns ErrMissingStepCapacity, naming the step, for a step with no
// candidate constraint, and propagates any normalization error.
func ComposeProcessPathCapacity(path processpath.ProcessPath, inputs map[ProcessType]StepInput, profile WorkloadProfile) (PathCapacityResult, error) {
	steps := path.Steps()
	result := PathCapacityResult{Steps: make([]StepResult, 0, len(steps))}
	for i, pathStep := range steps {
		step := ProcessType(pathStep)
		composed, err := ComposeStepCapacity(step, inputs[step], profile)
		if err != nil {
			return PathCapacityResult{}, err
		}
		result.Steps = append(result.Steps, composed)
		result.Warnings = append(result.Warnings, composed.Warnings...)
		if i == 0 || composed.Rate.LessThan(result.Rate) {
			result.Rate = composed.Rate
			result.BottleneckStep = step
			result.BottleneckConstraint = composed.Binding
		}
	}
	return result, nil
}

// ComputeProcessPathCapacity is ComposeProcessPathCapacity over registered
// ProcessCapacity constraints alone (no station composition): for each step
// in path it takes that step's ProcessCapacity from capacities, normalizes
// every constraint into ORDER via profile and returns the MINIMUM across
// steps plus which step produced it (the bottleneck). It is the design-doc
// section 31 worked example's entry point.
//
// Returns ErrMissingStepCapacity, naming the missing step, if any step in
// path has no entry in capacities. Propagates any NormalizeToOrderRate error.
func ComputeProcessPathCapacity(path processpath.ProcessPath, capacities map[ProcessType]*ProcessCapacity, profile WorkloadProfile) (CapacityRate, ProcessType, error) {
	inputs := make(map[ProcessType]StepInput, len(capacities))
	for step, pc := range capacities {
		inputs[step] = StepInput{Registered: pc}
	}
	result, err := ComposeProcessPathCapacity(path, inputs, profile)
	if err != nil {
		return CapacityRate{}, "", err
	}
	return result.Rate, result.BottleneckStep, nil
}
