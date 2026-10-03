package processcapacity

import (
	"errors"
	"fmt"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ErrMissingStepCapacity is returned by ComputeProcessPathCapacity when a
// ProcessPath step has no entry in the capacities map. A missing step is
// never silently treated as zero or infinite capacity -- that would either
// falsely report a zero-throughput bottleneck or (worse) skip the step
// entirely and understate how constrained the path really is.
var ErrMissingStepCapacity = errors.New("processcapacity: missing capacity data for step")

// ComputeProcessPathCapacity is a domain SERVICE -- a plain function, not a
// stored aggregate -- that computes a ProcessPath's end-to-end,
// WorkloadProfile-normalized capacity (see domain-model.md's
// ProcessPathCapacity definition). For each step in path, it looks up that
// step's ProcessCapacity in capacities, takes its EffectiveRate(),
// normalizes that rate into ORDER/period via profile, and returns the
// MINIMUM normalized rate across every step plus which step produced it
// (the bottleneck).
//
// Returns ErrMissingStepCapacity, naming the missing step, if any step in
// path has no entry in capacities. Propagates any error EffectiveRate or
// NormalizeToOrderRate returns.
func ComputeProcessPathCapacity(path processpath.ProcessPath, capacities map[ProcessType]*ProcessCapacity, profile WorkloadProfile) (CapacityRate, ProcessType, error) {
	steps := path.Steps()

	var (
		bottleneckRate CapacityRate
		bottleneckStep ProcessType
	)
	for i, pathStep := range steps {
		step := ProcessType(pathStep)

		pc, ok := capacities[step]
		if !ok || pc == nil {
			return CapacityRate{}, "", fmt.Errorf("%w: %s", ErrMissingStepCapacity, step)
		}

		effective, _, err := pc.EffectiveRate()
		if err != nil {
			return CapacityRate{}, "", err
		}

		normalized, err := profile.NormalizeToOrderRate(effective)
		if err != nil {
			return CapacityRate{}, "", err
		}

		if i == 0 || normalized.LessThan(bottleneckRate) {
			bottleneckRate = normalized
			bottleneckStep = step
		}
	}

	return bottleneckRate, bottleneckStep, nil
}
