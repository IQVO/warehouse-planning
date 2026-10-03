package usecases

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

func TestIsDomainValidationError(t *testing.T) {
	infra := errors.New("connection reset by peer")
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"invalid window", processcapacity.ErrInvalidWindow, true},
		{"negative quantity", processcapacity.ErrNegativeQuantity, true},
		{"non-positive period", processcapacity.ErrNonPositivePeriod, true},
		{"unit mismatch", processcapacity.ErrUnitMismatch, true},
		{"wrapped domain error", fmt.Errorf("ctx: %w", processcapacity.ErrUnitMismatch), true},
		{"plain infrastructure error", infra, false},
		{"wrapped infrastructure error", fmt.Errorf("begin: %w", infra), false},
		{"unrelated domain error is NOT a command validation error", processcapacity.ErrNoConstraints, false},
		{"context cancelled", context.Canceled, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsDomainValidationError(tt.err); got != tt.want {
				t.Fatalf("IsDomainValidationError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// failingRepo lets a test make Find and/or Save return an infrastructure
// error so Handle's error classification can be checked at the right place.
type failingRepo struct {
	*memory.ProcessCapacityRepo
	findErr, saveErr error
}

func (f failingRepo) Save(ctx context.Context, pc *processcapacity.ProcessCapacity) error {
	if f.saveErr != nil {
		return f.saveErr
	}
	return f.ProcessCapacityRepo.Save(ctx, pc)
}

func (f failingRepo) FindByProcessLocationWindow(ctx context.Context, pt processcapacity.ProcessType, loc string, s, e time.Time) (*processcapacity.ProcessCapacity, error) {
	if f.findErr != nil {
		return nil, f.findErr
	}
	return f.ProcessCapacityRepo.FindByProcessLocationWindow(ctx, pt, loc, s, e)
}

// TestRegisterProcessCapacityConstraint_ErrorClassification pins WHERE the
// domain/infrastructure split happens: command rejections surface a typed
// domain error (IsDomainValidationError true); repository failures surface
// untouched and are NOT classified as domain validation.
func TestRegisterProcessCapacityConstraint_ErrorClassification(t *testing.T) {
	start, end := pickZoneAWindowTimes()
	good := RegisterProcessCapacityConstraintCommand{
		ProcessType: "PICK", Location: "Z", WindowStart: start, WindowEnd: end,
		ConstraintType: processcapacity.ConstraintLabor, Quantity: 10, Unit: processcapacity.UnitUnit, Period: time.Hour,
	}
	with := func(mut func(*RegisterProcessCapacityConstraintCommand)) RegisterProcessCapacityConstraintCommand {
		c := good
		mut(&c)
		return c
	}
	boom := errors.New("db down")

	tests := []struct {
		name       string
		repo       failingRepo
		cmd        RegisterProcessCapacityConstraintCommand
		wantErr    error
		wantDomain bool
	}{
		{"bad window", failingRepo{}, with(func(c *RegisterProcessCapacityConstraintCommand) { c.WindowEnd = c.WindowStart }), processcapacity.ErrInvalidWindow, true},
		{"negative quantity", failingRepo{}, with(func(c *RegisterProcessCapacityConstraintCommand) { c.Quantity = -1 }), processcapacity.ErrNegativeQuantity, true},
		{"zero period", failingRepo{}, with(func(c *RegisterProcessCapacityConstraintCommand) { c.Period = 0 }), processcapacity.ErrNonPositivePeriod, true},
		{"find fails", failingRepo{findErr: boom}, good, boom, false},
		{"save fails", failingRepo{saveErr: boom}, good, boom, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.repo.ProcessCapacityRepo = memory.NewProcessCapacityRepo()
			_, err := (&RegisterProcessCapacityConstraint{Repo: tt.repo}).Handle(context.Background(), tt.cmd)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if got := IsDomainValidationError(err); got != tt.wantDomain {
				t.Fatalf("IsDomainValidationError = %v, want %v", got, tt.wantDomain)
			}
		})
	}

	t.Run("unit mismatch against the aggregate's native unit", func(t *testing.T) {
		repo := memory.NewProcessCapacityRepo()
		uc := &RegisterProcessCapacityConstraint{Repo: repo}
		if _, err := uc.Handle(context.Background(), good); err != nil {
			t.Fatal(err)
		}
		_, err := uc.Handle(context.Background(), with(func(c *RegisterProcessCapacityConstraintCommand) {
			c.ConstraintType = processcapacity.ConstraintStation
			c.Unit = processcapacity.UnitLine
		}))
		if !errors.Is(err, processcapacity.ErrUnitMismatch) || !IsDomainValidationError(err) {
			t.Fatalf("err = %v, want a domain ErrUnitMismatch", err)
		}
	})
}
