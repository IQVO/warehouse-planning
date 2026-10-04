package processcapacity

import "fmt"

// StepInput is everything known about ONE path step at one location and
// window, handed to ComposeStepCapacity:
//
//   - Registered: the ProcessCapacity constraints registered at exactly
//     (process, location, window) -- LABOR, EQUIPMENT, ... as before. Nil when
//     nothing is registered.
//   - StationCount / Standard: the stations facility-layout tallied for the
//     process's activity across the site, and the operator-declared
//     StationStandard (nil when none is declared). Neither is a constraint of
//     its own: only their product is (see ComposeStepCapacity).
type StepInput struct {
	Registered   *ProcessCapacity
	Location     string
	StationCount int
	Standard     *StationStandard
}

// StepResult is one step's composed capacity: the MINIMUM of its candidate
// constraints after normalization to ORDER, which constraint type binds, and
// any warnings raised while composing.
type StepResult struct {
	Step     ProcessType
	Rate     CapacityRate // ORDER over the candidates' period
	Binding  ConstraintType
	Warnings []string
}

// ComposeStepCapacity is a domain SERVICE (read-time composition, see ADR
// 0002). A step's candidate constraints are
//
//	(a) every constraint registered on in.Registered, exactly as before, plus
//	(b) a DERIVED STATION constraint = StationCount x Standard.PerStation,
//	    only when BOTH a station count > 0 and a standard exist.
//
// EVERY candidate is normalized to ORDER with profile BEFORE comparing (units
// per hour and packages per hour are not comparable raw -- design doc rule 8;
// ORDER passes through unchanged); the step's effective rate is the minimum
// and its binding constraint type is reported. A candidate in a unit the
// profile cannot normalize (LINE) is rejected with the profile's error. Ties
// go to the earliest candidate: registered constraints in registration order,
// then the derived STATION constraint.
//
// With stations tallied but NO standard declared the step is composed from
// (a) only and a warning says so -- a throughput is never invented. With no
// candidate at all the step has no capacity data: ErrMissingStepCapacity.
//
// The stored ProcessCapacity aggregate is not touched: its single-native-unit
// invariant stays as is, the composition happens on transient values.
func ComposeStepCapacity(step ProcessType, in StepInput, profile WorkloadProfile) (StepResult, error) {
	var (
		candidates []ConstraintEntry
		warnings   []string
	)
	if in.Registered != nil {
		candidates = append(candidates, in.Registered.Constraints()...)
	}
	if in.StationCount > 0 {
		if in.Standard == nil {
			warnings = append(warnings, fmt.Sprintf(
				"no station standard declared for %s at %s: %d stations are tallied but their throughput is unknown, so no STATION constraint was applied",
				step, in.Location, in.StationCount))
		} else {
			derived, err := in.Standard.CapacityFor(in.StationCount)
			if err != nil {
				return StepResult{}, err
			}
			candidates = append(candidates, ConstraintEntry{Type: ConstraintStation, Rate: derived})
		}
	}
	if len(candidates) == 0 {
		return StepResult{}, fmt.Errorf("%w: %s", ErrMissingStepCapacity, step)
	}

	var (
		best    CapacityRate
		binding ConstraintType
	)
	for i, c := range candidates {
		normalized, err := profile.NormalizeToOrderRate(c.Rate)
		if err != nil {
			return StepResult{}, err
		}
		if i == 0 || normalized.LessThan(best) {
			best, binding = normalized, c.Type
		}
	}
	return StepResult{Step: step, Rate: best, Binding: binding, Warnings: warnings}, nil
}
