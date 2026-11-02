package mcp

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

type registerProcessPathInput struct {
	ID    string   `json:"id" jsonschema:"the process path's id, e.g. tote-path; registering an existing id replaces its name and steps"`
	Name  string   `json:"name" jsonschema:"a human-readable name"`
	Steps []string `json:"steps" jsonschema:"the ordered, non-empty list of step process types, e.g. [PICK, REBIN, PACK]"`
}

type processPathOutput struct {
	ID    string   `json:"id"`
	Name  string   `json:"name"`
	Steps []string `json:"steps"`
}

func (d Deps) registerProcessPath(ctx context.Context, in registerProcessPathInput) (processPathOutput, error) {
	steps := make([]processpath.ProcessType, 0, len(in.Steps))
	for _, step := range in.Steps {
		steps = append(steps, processpath.ProcessType(step))
	}
	path, err := d.RegisterProcessPath.Handle(ctx, usecases.RegisterProcessPathCommand{ID: in.ID, Name: in.Name, Steps: steps})
	if err != nil {
		return processPathOutput{}, mapError(err)
	}
	out := make([]string, 0, len(path.Steps()))
	for _, step := range path.Steps() {
		out = append(out, string(step))
	}
	return processPathOutput{ID: path.ID(), Name: path.Name(), Steps: out}, nil
}

type getPathCapacityInput struct {
	ID               string   `json:"id" jsonschema:"the registered process path's id"`
	Location         string   `json:"location" jsonschema:"the warehouse location every step's capacity is looked up under, e.g. FC01"`
	WindowStart      string   `json:"window_start" jsonschema:"window start, RFC3339 timestamp; a registered capacity window applies to a step when it COVERS [window_start, window_end), not only when equal"`
	WindowEnd        string   `json:"window_end" jsonschema:"window end, RFC3339 timestamp; must be after window_start"`
	UnitsPerOrder    *float64 `json:"units_per_order,omitempty" jsonschema:"workload conversion factor: UNIT per ORDER; required if any step is measured in UNIT"`
	PackagesPerOrder *float64 `json:"packages_per_order,omitempty" jsonschema:"workload conversion factor: PACKAGE per ORDER; required if any step is measured in PACKAGE"`
}

// pathCapacityOutput is the REST body of GET /process-paths/{id}/capacity.
// step_breakdown (each step's normalized ORDER-per-hour rate and the
// constraint type binding it) and warnings (never null) are additive.
type pathCapacityOutput struct {
	NormalizedRate float64             `json:"normalized_rate"`
	NormalizedUnit string              `json:"normalized_unit"`
	BottleneckStep string              `json:"bottleneck_step"`
	StepBreakdown  []stepBreakdownView `json:"step_breakdown"`
	Warnings       []string            `json:"warnings"`
}

type stepBreakdownView struct {
	Step              string  `json:"step"`
	NormalizedRate    float64 `json:"normalized_rate"`
	BindingConstraint string  `json:"binding_constraint"`
}

func (d Deps) getProcessPathCapacity(ctx context.Context, in getPathCapacityInput) (pathCapacityOutput, error) {
	windowStart, windowEnd, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return pathCapacityOutput{}, err
	}
	result, err := d.GetProcessPathCapacity.Handle(ctx, usecases.GetProcessPathCapacityCommand{
		ProcessPathID:    in.ID,
		Location:         in.Location,
		WindowStart:      windowStart,
		WindowEnd:        windowEnd,
		UnitsPerOrder:    in.UnitsPerOrder,
		PackagesPerOrder: in.PackagesPerOrder,
	})
	if err != nil {
		return pathCapacityOutput{}, mapError(err)
	}
	breakdown := make([]stepBreakdownView, 0, len(result.Steps))
	for _, step := range result.Steps {
		breakdown = append(breakdown, stepBreakdownView{
			Step:              string(step.Step),
			NormalizedRate:    step.Rate.Quantity() / step.Rate.Period().Hours(),
			BindingConstraint: string(step.Binding),
		})
	}
	return pathCapacityOutput{
		NormalizedRate: result.NormalizedRate.Quantity(),
		NormalizedUnit: string(result.NormalizedRate.Unit()),
		BottleneckStep: string(result.BottleneckStep),
		StepBreakdown:  breakdown,
		Warnings:       nonNilStrings(result.Warnings),
	}, nil
}
