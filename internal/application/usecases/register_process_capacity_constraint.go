package usecases

import (
	"context"
	"errors"
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

// commandValidationErrors are the typed domain errors Handle can return
// because the COMMAND itself is unacceptable (bad window, negative
// quantity, non-positive period, unit differing from the aggregate's native
// unit). They are deterministic: retrying the same command can never
// succeed. Everything else Handle returns originates in the repository
// (Find/Save) and is infrastructure.
var commandValidationErrors = []error{
	processcapacity.ErrInvalidWindow,
	processcapacity.ErrNegativeQuantity,
	processcapacity.ErrNonPositivePeriod,
	processcapacity.ErrUnitMismatch,
}

// IsDomainValidationError reports whether err (as returned by Handle) is a
// deterministic domain rejection of the command, as opposed to a
// transient infrastructure failure from the repository. Inbound adapters
// use it to decide between "skip this message, retrying is pointless" and
// "fail so the whole unit of work rolls back and the message is retried".
// It is an allow-list on purpose: an error it does not recognise is treated
// as infrastructure, so the safe failure mode is retry, never data loss.
func IsDomainValidationError(err error) bool {
	for _, target := range commandValidationErrors {
		if errors.Is(err, target) {
			return true
		}
	}
	return false
}
