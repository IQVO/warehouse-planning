package mcp

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

type registerConstraintInput struct {
	ProcessType    string  `json:"process_type" jsonschema:"the process step the constraint limits, e.g. PICK, REBIN, PACK"`
	Location       string  `json:"location" jsonschema:"the warehouse location the capacity applies to, e.g. FC01"`
	WindowStart    string  `json:"window_start" jsonschema:"inclusive window start, RFC3339 timestamp, e.g. 2026-10-05T08:00:00Z"`
	WindowEnd      string  `json:"window_end" jsonschema:"exclusive window end, RFC3339 timestamp; must be after window_start"`
	ConstraintType string  `json:"constraint_type" jsonschema:"the kind of constraint: LABOR, LOCATION, EQUIPMENT, STATION, CONVEYOR, BUFFER or REPLENISHMENT"`
	Quantity       float64 `json:"quantity" jsonschema:"the constraint's capacity quantity per period; must not be negative"`
	Unit           string  `json:"unit" jsonschema:"the capacity unit: UNIT, LINE, ORDER or PACKAGE; must match the unit of constraints already registered for this process/location/window"`
	PeriodSeconds  float64 `json:"period_seconds" jsonschema:"the period the quantity is measured over, in seconds; must be positive (3600 = per hour)"`
}

type constraintView struct {
	ConstraintType string  `json:"constraint_type"`
	Quantity       float64 `json:"quantity"`
	Unit           string  `json:"unit"`
	PeriodSeconds  float64 `json:"period_seconds"`
}

type effectiveCapacityOutput struct {
	EffectiveRate     float64          `json:"effective_rate"`
	EffectiveUnit     string           `json:"effective_unit"`
	BindingConstraint string           `json:"binding_constraint"`
	Constraints       []constraintView `json:"constraints,omitempty"`
}

func (d Deps) registerProcessCapacityConstraint(ctx context.Context, in registerConstraintInput) (effectiveCapacityOutput, error) {
	windowStart, windowEnd, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return effectiveCapacityOutput{}, err
	}
	if in.PeriodSeconds <= 0 {
		return effectiveCapacityOutput{}, toolError("non-positive-period", "period_seconds must be positive")
	}

	result, err := d.RegisterProcessCapacityConstraint.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
		ProcessType:    processcapacity.ProcessType(in.ProcessType),
		Location:       in.Location,
		WindowStart:    windowStart,
		WindowEnd:      windowEnd,
		ConstraintType: processcapacity.ConstraintType(in.ConstraintType),
		Quantity:       in.Quantity,
		Unit:           processcapacity.CapacityUnit(in.Unit),
		Period:         time.Duration(in.PeriodSeconds * float64(time.Second)),
	})
	if err != nil {
		return effectiveCapacityOutput{}, mapError(err)
	}
	return effectiveCapacityOutput{
		EffectiveRate:     result.EffectiveRate.Quantity(),
		EffectiveUnit:     string(result.EffectiveRate.Unit()),
		BindingConstraint: string(result.BindingConstraint),
	}, nil
}

type getEffectiveCapacityInput struct {
	ProcessType string `json:"process_type" jsonschema:"the process step to read, e.g. PICK, REBIN, PACK"`
	Location    string `json:"location" jsonschema:"the warehouse location, e.g. FC01"`
	WindowStart string `json:"window_start" jsonschema:"window start, RFC3339 timestamp; must equal the registered window exactly"`
	WindowEnd   string `json:"window_end" jsonschema:"window end, RFC3339 timestamp; must equal the registered window exactly"`
}

func (d Deps) getEffectiveProcessCapacity(ctx context.Context, in getEffectiveCapacityInput) (effectiveCapacityOutput, error) {
	windowStart, windowEnd, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return effectiveCapacityOutput{}, err
	}

	pc, err := d.ProcessCapacities.FindByProcessLocationWindow(ctx, processcapacity.ProcessType(in.ProcessType), in.Location, windowStart, windowEnd)
	if err != nil {
		return effectiveCapacityOutput{}, mapError(err)
	}
	if pc == nil {
		return effectiveCapacityOutput{}, toolError("process-capacity-not-found", "no ProcessCapacity registered for the given process_type/location/window")
	}

	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		return effectiveCapacityOutput{}, mapError(err)
	}

	entries := pc.Constraints()
	constraints := make([]constraintView, 0, len(entries))
	for _, entry := range entries {
		constraints = append(constraints, constraintView{
			ConstraintType: string(entry.Type),
			Quantity:       entry.Rate.Quantity(),
			Unit:           string(entry.Rate.Unit()),
			PeriodSeconds:  entry.Rate.Period().Seconds(),
		})
	}
	return effectiveCapacityOutput{
		EffectiveRate:     effective.Quantity(),
		EffectiveUnit:     string(effective.Unit()),
		BindingConstraint: string(binding),
		Constraints:       constraints,
	}, nil
}
