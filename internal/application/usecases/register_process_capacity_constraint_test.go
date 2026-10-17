package usecases

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

func pickZoneAWindowTimes() (time.Time, time.Time) {
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := time.Date(2026, 10, 5, 9, 0, 0, 0, time.UTC)
	return start, end
}

// TestRegisterProcessCapacityConstraint_WorkedExample reproduces the design
// doc's PICK/PICK-ZONE-A worked example through the use case + in-memory
// repo: four sequential registrations must leave LOCATION=3500 UNIT/HOUR
// binding.
func TestRegisterProcessCapacityConstraint_WorkedExample(t *testing.T) {
	repo := memory.NewProcessCapacityRepo()
	uc := &RegisterProcessCapacityConstraint{Repo: repo}
	start, end := pickZoneAWindowTimes()
	ctx := context.Background()

	registrations := []struct {
		constraintType processcapacity.ConstraintType
		quantity       float64
	}{
		{processcapacity.ConstraintLabor, 4000},
		{processcapacity.ConstraintLocation, 3500},
		{processcapacity.ConstraintEquipment, 5000},
		{processcapacity.ConstraintConveyor, 3800},
	}

	var result RegisterProcessCapacityConstraintResult
	for _, reg := range registrations {
		var err error
		result, err = uc.Handle(ctx, RegisterProcessCapacityConstraintCommand{
			ProcessType:    "PICK",
			Location:       "PICK-ZONE-A",
			WindowStart:    start,
			WindowEnd:      end,
			ConstraintType: reg.constraintType,
			Quantity:       reg.quantity,
			Unit:           processcapacity.UnitUnit,
			Period:         time.Hour,
		})
		if err != nil {
			t.Fatalf("unexpected error registering %v: %v", reg.constraintType, err)
		}
	}

	if result.BindingConstraint != processcapacity.ConstraintLocation {
		t.Fatalf("expected LOCATION to be binding, got %v", result.BindingConstraint)
	}
	if result.EffectiveRate.Quantity() != 3500 {
		t.Fatalf("expected effective rate 3500, got %v", result.EffectiveRate.Quantity())
	}

	stored, err := repo.FindByProcessLocationWindow(ctx, "PICK", "PICK-ZONE-A", start, end)
	if err != nil {
		t.Fatalf("unexpected error reading back: %v", err)
	}
	if stored == nil {
		t.Fatal("expected the aggregate to have been persisted")
	}
	if got := len(stored.Constraints()); got != 4 {
		t.Fatalf("expected 4 persisted constraints, got %d", got)
	}
}

func TestRegisterProcessCapacityConstraint_RejectsInvalidWindow(t *testing.T) {
	repo := memory.NewProcessCapacityRepo()
	uc := &RegisterProcessCapacityConstraint{Repo: repo}
	start, _ := pickZoneAWindowTimes()

	_, err := uc.Handle(context.Background(), RegisterProcessCapacityConstraintCommand{
		ProcessType:    "PICK",
		Location:       "PICK-ZONE-A",
		WindowStart:    start,
		WindowEnd:      start, // zero-length window
		ConstraintType: processcapacity.ConstraintLabor,
		Quantity:       100,
		Unit:           processcapacity.UnitUnit,
		Period:         time.Hour,
	})
	if !errors.Is(err, processcapacity.ErrInvalidWindow) {
		t.Fatalf("expected ErrInvalidWindow, got %v", err)
	}
}

func TestRegisterProcessCapacityConstraint_RejectsMismatchedUnit(t *testing.T) {
	repo := memory.NewProcessCapacityRepo()
	uc := &RegisterProcessCapacityConstraint{Repo: repo}
	start, end := pickZoneAWindowTimes()
	ctx := context.Background()

	_, err := uc.Handle(ctx, RegisterProcessCapacityConstraintCommand{
		ProcessType:    "PICK",
		Location:       "PICK-ZONE-A",
		WindowStart:    start,
		WindowEnd:      end,
		ConstraintType: processcapacity.ConstraintLabor,
		Quantity:       100,
		Unit:           processcapacity.UnitUnit,
		Period:         time.Hour,
	})
	if err != nil {
		t.Fatalf("unexpected error on first registration: %v", err)
	}

	_, err = uc.Handle(ctx, RegisterProcessCapacityConstraintCommand{
		ProcessType:    "PICK",
		Location:       "PICK-ZONE-A",
		WindowStart:    start,
		WindowEnd:      end,
		ConstraintType: processcapacity.ConstraintEquipment,
		Quantity:       50,
		Unit:           processcapacity.UnitPackage,
		Period:         time.Hour,
	})
	if !errors.Is(err, processcapacity.ErrUnitMismatch) {
		t.Fatalf("expected ErrUnitMismatch, got %v", err)
	}
}

// fakeFailingRepo lets a test force FindByProcessLocationWindow/Save to
// fail, proving Handle propagates repository errors instead of masking
// them.
type fakeFailingRepo struct {
	findErr error
	saveErr error
}

func (f *fakeFailingRepo) Save(context.Context, *processcapacity.ProcessCapacity) error {
	return f.saveErr
}

func (f *fakeFailingRepo) FindByProcessLocationWindow(context.Context, processcapacity.ProcessType, string, time.Time, time.Time) (*processcapacity.ProcessCapacity, error) {
	return nil, f.findErr
}

func (f *fakeFailingRepo) FindCovering(context.Context, processcapacity.ProcessType, string, time.Time, time.Time) ([]*processcapacity.ProcessCapacity, error) {
	return nil, f.findErr
}

func TestRegisterProcessCapacityConstraint_PropagatesRepositoryErrors(t *testing.T) {
	start, end := pickZoneAWindowTimes()
	ctx := context.Background()
	cmd := RegisterProcessCapacityConstraintCommand{
		ProcessType:    "PICK",
		Location:       "PICK-ZONE-A",
		WindowStart:    start,
		WindowEnd:      end,
		ConstraintType: processcapacity.ConstraintLabor,
		Quantity:       100,
		Unit:           processcapacity.UnitUnit,
		Period:         time.Hour,
	}

	findErr := errors.New("find boom")
	uc := &RegisterProcessCapacityConstraint{Repo: &fakeFailingRepo{findErr: findErr}}
	if _, err := uc.Handle(ctx, cmd); !errors.Is(err, findErr) {
		t.Fatalf("expected find error to propagate, got %v", err)
	}

	saveErr := errors.New("save boom")
	uc = &RegisterProcessCapacityConstraint{Repo: &fakeFailingRepo{saveErr: saveErr}}
	if _, err := uc.Handle(ctx, cmd); !errors.Is(err, saveErr) {
		t.Fatalf("expected save error to propagate, got %v", err)
	}
}

// The events tests need the REAL CloudEvents encoder (the same one the
// kafka adapter's golden tests pin): the use cases never import an
// outbound adapter, tests may.

var pcEventsNow = time.Date(2026, 10, 5, 7, 0, 0, 0, time.UTC)

// pcEventsUC builds the use case wired the way cmd/api wires it.
func pcEventsUC() (*RegisterProcessCapacityConstraint, *memory.OutboxRepo) {
	repo, outbox := memory.NewProcessCapacityRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(repo, outbox)
	return &RegisterProcessCapacityConstraint{
		Repo: repo, Outbox: outbox,
		Encoder:    outboundkafka.NewEncoder(),
		UnitOfWork: uow,
		Now:        func() time.Time { return pcEventsNow },
	}, outbox
}

func pcEventsCmd(constraint processcapacity.ConstraintType, quantity float64) RegisterProcessCapacityConstraintCommand {
	start, end := pickZoneAWindowTimes()
	return RegisterProcessCapacityConstraintCommand{
		ProcessType: "PICK", Location: "OUTBOX-ZONE",
		WindowStart: start, WindowEnd: end,
		ConstraintType: constraint, Quantity: quantity,
		Unit: processcapacity.UnitUnit, Period: time.Hour,
	}
}

func outboxEventTypes(outbox *memory.OutboxRepo) []string {
	var got []string
	for _, m := range outbox.Messages() {
		got = append(got, m.EventType)
	}
	return got
}

// TestRegisterProcessCapacityConstraint_OutboxesEvents pins the event
// sequence of three registrations: Registered, Changed (the effective rate
// dropped), then nothing (a non-binding constraint).
func TestRegisterProcessCapacityConstraint_OutboxesEvents(t *testing.T) {
	ctx := context.Background()
	uc, outbox := pcEventsUC()

	if _, err := uc.Handle(ctx, pcEventsCmd(processcapacity.ConstraintLabor, 4000)); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Handle(ctx, pcEventsCmd(processcapacity.ConstraintLocation, 3500)); err != nil {
		t.Fatal(err)
	}
	if _, err := uc.Handle(ctx, pcEventsCmd(processcapacity.ConstraintEquipment, 5000)); err != nil {
		t.Fatal(err) // non-binding: no event
	}
	want := []string{
		"com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityRegistered",
		"com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityChanged",
	}
	if got := outboxEventTypes(outbox); strings.Join(got, "|") != strings.Join(want, "|") {
		t.Errorf("outbox types = %v, want %v", got, want)
	}
	for _, m := range outbox.Messages() {
		if m.Topic != "warehouse.warehouse-planning.events" {
			t.Errorf("topic = %q", m.Topic)
		}
		if m.Subject != "PICK/OUTBOX-ZONE/2026-10-05T08:00:00Z/2026-10-05T09:00:00Z" {
			t.Errorf("subject = %q", m.Subject)
		}
	}
}

// A constructor that wires no outbox (the HTTP unit tests, the in-memory
// MCP binary) registers constraints and publishes nothing.
func TestRegisterProcessCapacityConstraint_WithoutOutboxPublishesNothing(t *testing.T) {
	uc := &RegisterProcessCapacityConstraint{Repo: memory.NewProcessCapacityRepo()}
	if _, err := uc.Handle(context.Background(), pcEventsCmd(processcapacity.ConstraintLabor, 4000)); err != nil {
		t.Fatal(err)
	}
}

// The save and the outbox rows are one unit: an encoder failure rolls the
// registration back entirely.
func TestRegisterProcessCapacityConstraint_EncoderFailureRollsBack(t *testing.T) {
	ctx := context.Background()
	repo, outbox := memory.NewProcessCapacityRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(repo, outbox)
	uc := &RegisterProcessCapacityConstraint{
		Repo: repo, Outbox: outbox,
		Encoder:    failingEncoder{},
		UnitOfWork: uow,
		Now:        func() time.Time { return pcEventsNow },
	}
	if _, err := uc.Handle(ctx, pcEventsCmd(processcapacity.ConstraintLabor, 4000)); err == nil {
		t.Fatal("want the encoder error")
	}
	if msgs := outbox.Messages(); len(msgs) != 0 {
		t.Errorf("rolled-back registration left %d outbox rows", len(msgs))
	}
	start, end := pickZoneAWindowTimes()
	found, err := repo.FindByProcessLocationWindow(ctx, "PICK", "OUTBOX-ZONE", start, end)
	if err != nil {
		t.Fatal(err)
	}
	if found != nil {
		t.Error("rolled-back registration left an aggregate behind")
	}
}

// failingEncoder implements ports.EventEncoder by failing, to prove the
// unit of work rolls the save back with the outbox insert.
type failingEncoder struct{}

func (failingEncoder) EncodeCapacityPlanEvents(events ...capacityplan.Event) ([]outbox.Message, error) {
	return nil, errors.New("encoder down")
}

func (failingEncoder) EncodeProcessCapacityEvents(events ...processcapacity.Event) ([]outbox.Message, error) {
	return nil, errors.New("encoder down")
}
