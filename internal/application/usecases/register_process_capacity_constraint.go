package usecases

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// RegisterProcessCapacityConstraintCommand carries everything needed to
// upsert one constraint on a ProcessCapacity aggregate, creating the
// aggregate on first registration for a given process+location+window.
type RegisterProcessCapacityConstraintCommand struct {
	ProcessType    processcapacity.ProcessType
	Location       string
	WindowStart    time.Time
	WindowEnd      time.Time
	ConstraintType processcapacity.ConstraintType
	Quantity       float64
	Unit           processcapacity.CapacityUnit
	Period         time.Duration
}

// RegisterProcessCapacityConstraintResult reports the effective rate and
// its binding constraint after the command's constraint has been applied.
type RegisterProcessCapacityConstraintResult struct {
	EffectiveRate     processcapacity.CapacityRate
	BindingConstraint processcapacity.ConstraintType
}

// RegisterProcessCapacityConstraint upserts one constraint on a
// ProcessCapacity aggregate, recomputes and persists the effective rate
// (see domain-model.md).
type RegisterProcessCapacityConstraint struct {
	Repo ports.ProcessCapacityRepository
}

// Handle loads the ProcessCapacity for cmd's identity (creating it if this
// is the first constraint ever registered for that process+location+window),
// applies AddConstraint, persists the result, and returns the new
// effective rate and binding constraint.
func (uc *RegisterProcessCapacityConstraint) Handle(ctx context.Context, cmd RegisterProcessCapacityConstraintCommand) (RegisterProcessCapacityConstraintResult, error) {
	window, err := processcapacity.NewCapacityWindow(cmd.WindowStart, cmd.WindowEnd)
	if err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}

	rate, err := processcapacity.NewCapacityRate(cmd.Quantity, cmd.Unit, cmd.Period)
	if err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}

	pc, err := uc.Repo.FindByProcessLocationWindow(ctx, cmd.ProcessType, cmd.Location, cmd.WindowStart, cmd.WindowEnd)
	if err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}
	if pc == nil {
		pc = processcapacity.NewProcessCapacity(cmd.ProcessType, cmd.Location, window)
	}

	if err := pc.AddConstraint(cmd.ConstraintType, rate); err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}

	if err := uc.Repo.Save(ctx, pc); err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}

	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}

	return RegisterProcessCapacityConstraintResult{EffectiveRate: effective, BindingConstraint: binding}, nil
}
