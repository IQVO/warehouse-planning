package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var _ ports.UnitOfWork = (*memory.UnitOfWork)(nil)

type uowFixture struct {
	uow       *memory.UnitOfWork
	pcs       *memory.ProcessCapacityRepo
	processed *memory.ProcessedEventRepo
	tally     *memory.StorageTallyRepo
}

func newUoWFixture() uowFixture {
	pcs, processed, tally := memory.NewProcessCapacityRepo(), memory.NewProcessedEventRepo(), memory.NewStorageTallyRepo()
	return uowFixture{memory.NewUnitOfWork(pcs, processed, tally), pcs, processed, tally}
}

// touchEverything mutates all three repos.
func (f uowFixture) touchEverything(t *testing.T, ctx context.Context) {
	t.Helper()
	if claimed, err := f.processed.Claim(ctx, "c", "e1"); err != nil || !claimed {
		t.Fatalf("Claim = %v, %v", claimed, err)
	}
	if _, err := f.tally.RegisterSlot(ctx, "LOC-1", "Z", "LOCATION", []string{"BULK"}); err != nil {
		t.Fatalf("RegisterSlot: %v", err)
	}
	w, _ := processcapacity.NewCapacityWindow(time.Unix(0, 0), time.Unix(3600, 0))
	pc := processcapacity.NewProcessCapacity("PICK", "WH1", w)
	r, _ := processcapacity.NewCapacityRate(5, processcapacity.UnitUnit, time.Hour)
	if err := pc.AddConstraint(processcapacity.ConstraintLabor, r); err != nil {
		t.Fatalf("AddConstraint: %v", err)
	}
	if err := f.pcs.Save(ctx, pc); err != nil {
		t.Fatalf("Save: %v", err)
	}
}

func (f uowFixture) pcExists(t *testing.T) bool {
	t.Helper()
	pc, err := f.pcs.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", time.Unix(0, 0), time.Unix(3600, 0))
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	return pc != nil
}

func TestUnitOfWork_ErrorRollsBackEveryParticipant(t *testing.T) {
	f := newUoWFixture()
	boom := errors.New("boom")
	err := f.uow.Do(context.Background(), func(ctx context.Context) error {
		f.touchEverything(t, ctx)
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do err = %v, want boom", err)
	}
	if f.processed.Has("c", "e1") || f.tally.Count("Z", "LOCATION", "BULK") != 0 || f.pcExists(t) {
		t.Fatal("state survived a rolled-back unit of work")
	}
}

func TestUnitOfWork_PanicRollsBackAndRepanics(t *testing.T) {
	f := newUoWFixture()
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected the panic to propagate")
			}
		}()
		_ = f.uow.Do(context.Background(), func(ctx context.Context) error {
			f.touchEverything(t, ctx)
			panic("kaboom")
		})
	}()
	if f.processed.Has("c", "e1") || f.pcExists(t) {
		t.Fatal("state survived a panicking unit of work")
	}
}

func TestUnitOfWork_NilCommits(t *testing.T) {
	f := newUoWFixture()
	if err := f.uow.Do(context.Background(), func(ctx context.Context) error {
		f.touchEverything(t, ctx)
		return nil
	}); err != nil {
		t.Fatalf("Do: %v", err)
	}
	if !f.processed.Has("c", "e1") || f.tally.Count("Z", "LOCATION", "BULK") != 1 || !f.pcExists(t) {
		t.Fatal("committed state missing")
	}
}

func TestUnitOfWork_NestedDoJoinsOuter(t *testing.T) {
	f := newUoWFixture()
	boom := errors.New("boom")
	err := f.uow.Do(context.Background(), func(ctx context.Context) error {
		// The inner Do succeeds, but the OUTER fails afterwards: the inner
		// work must be rolled back with it (and must not deadlock).
		if err := f.uow.Do(ctx, func(ctx context.Context) error {
			f.touchEverything(t, ctx)
			return nil
		}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do err = %v", err)
	}
	if f.processed.Has("c", "e1") || f.pcExists(t) {
		t.Fatal("inner work survived an outer rollback")
	}
}

func TestUnitOfWork_NoParticipantsJustRunsFn(t *testing.T) {
	uow := memory.NewUnitOfWork()
	repo := memory.NewProcessedEventRepo()
	boom := errors.New("boom")
	err := uow.Do(context.Background(), func(ctx context.Context) error {
		_, _ = repo.Claim(ctx, "c", "e")
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	// Documented behaviour: without participants nothing is rolled back.
	if !repo.Has("c", "e") {
		t.Fatal("participant-less UnitOfWork must not roll anything back")
	}
}

func TestProcessCapacityRepo_StoresCopiesNotAliases(t *testing.T) {
	f := newUoWFixture()
	f.touchEverything(t, context.Background())
	pc, _ := f.pcs.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", time.Unix(0, 0), time.Unix(3600, 0))
	r, _ := processcapacity.NewCapacityRate(1, processcapacity.UnitUnit, time.Hour)
	if err := pc.AddConstraint(processcapacity.ConstraintEquipment, r); err != nil {
		t.Fatal(err)
	}
	again, _ := f.pcs.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", time.Unix(0, 0), time.Unix(3600, 0))
	if len(again.Constraints()) != 1 {
		t.Fatalf("mutating a found aggregate leaked into the store without Save: %d constraints", len(again.Constraints()))
	}
}
