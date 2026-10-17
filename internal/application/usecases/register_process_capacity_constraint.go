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
// (see domain-model.md), and -- when Outbox, Encoder and UnitOfWork are
// wired -- persists the events the aggregate raised
// (ProcessCapacityRegistered on the first constraint,
// ProcessCapacityChanged when the effective rate moved) in the SAME unit
// of work as the save, through the transactional outbox.
type RegisterProcessCapacityConstraint struct {
	Repo ports.ProcessCapacityRepository

	// Outbox, Encoder and UnitOfWork publish the aggregate's events. All
	// three are optional and consulted together: a constructor that wires
	// none of them (unit tests of the HTTP surface, the in-memory MCP
	// binary) registers constraints and publishes nothing, exactly as
	// before the events existed. Production wires all three (cmd/api,
	// cmd/mcp): the save and the outbox rows then commit or roll back as
	// one.
	Outbox     ports.OutboxRepository
	Encoder    ports.EventEncoder
	UnitOfWork ports.UnitOfWork

	// Now supplies the domain time stamped on raised events (utcNow when
	// nil), injectable for tests.
	Now func() time.Time
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

	now := uc.Now
	if now == nil {
		now = utcNow
	}

	if err := pc.AddConstraint(cmd.ConstraintType, rate, now()); err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}

	if uc.Outbox == nil || uc.Encoder == nil {
		// No event publishing wired; the state change stands on its own.
		if err := uc.Repo.Save(ctx, pc); err != nil {
			return RegisterProcessCapacityConstraintResult{}, err
		}
	} else if err := uc.saveWithEvents(ctx, pc); err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}

	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		return RegisterProcessCapacityConstraintResult{}, err
	}

	return RegisterProcessCapacityConstraintResult{EffectiveRate: effective, BindingConstraint: binding}, nil
}

// saveWithEvents persists the aggregate and its already-encoded events as
// one atomic unit: inside a UnitOfWork they commit or roll back together,
// and a relay publishes the events only after the commit (ADR 0007). A nil
// UnitOfWork (memory adapters are not transactional) runs the two writes
// sequentially, which is the best it can do.
func (uc *RegisterProcessCapacityConstraint) saveWithEvents(ctx context.Context, pc *processcapacity.ProcessCapacity) error {
	persist := func(ctx context.Context) error {
		if err := uc.Repo.Save(ctx, pc); err != nil {
			return err
		}
		msgs, err := uc.Encoder.EncodeProcessCapacityEvents(pc.PullEvents()...)
		if err != nil {
			return err
		}
		return uc.Outbox.Insert(ctx, msgs...)
	}
	if uc.UnitOfWork == nil {
		return persist(ctx)
	}
	return uc.UnitOfWork.Do(ctx, persist)
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
