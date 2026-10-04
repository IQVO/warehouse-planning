package memory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var (
	_ ports.CapacityPlanRepository = (*memory.CapacityPlanRepo)(nil)
	_ ports.OutboxRepository       = (*memory.OutboxRepo)(nil)
)

func newPlan(t *testing.T, id string) *capacityplan.CapacityPlan {
	t.Helper()
	w, err := processcapacity.NewCapacityWindow(time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC), time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	rate, err := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	p, err := capacityplan.Create(capacityplan.CreateParams{
		ID: id, WarehouseID: "WH-1", Location: "Z", Window: w, ProcessPathID: "p",
		AssignedDemand: 12000, PathRate: rate, BottleneckStep: "REBIN",
	}, time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestCapacityPlanRepo_SaveFindRoundTripAndMiss(t *testing.T) {
	repo := memory.NewCapacityPlanRepo()
	ctx := context.Background()
	if got, err := repo.FindByID(ctx, "x"); got != nil || err != nil {
		t.Fatalf("miss = %v, %v; want nil, nil", got, err)
	}
	plan := newPlan(t, "plan-1")
	if err := repo.Save(ctx, plan); err != nil {
		t.Fatal(err)
	}
	got, err := repo.FindByID(ctx, "plan-1")
	if err != nil || got == nil {
		t.Fatalf("Find = %v, %v", got, err)
	}
	if got.Shortage() != 4000 || got.BottleneckStep() != "REBIN" || got.Status() != capacityplan.StatusDraft || got.CapacityOverWindow() != 8000 {
		t.Errorf("round trip lost state: %+v", got)
	}
	if len(got.PullEvents()) != 0 {
		t.Error("a loaded plan must carry no pending events")
	}
	// Mutating the loaded copy must not leak into the store until Save.
	if err := got.Publish(time.Now()); err != nil {
		t.Fatal(err)
	}
	again, _ := repo.FindByID(ctx, "plan-1")
	if again.Status() != capacityplan.StatusDraft {
		t.Error("unsaved mutation leaked into the store")
	}
}

func TestUnitOfWork_RollsBackCapacityPlanAndOutbox(t *testing.T) {
	plans, ob := memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(plans, ob)
	boom := errors.New("boom")
	err := uow.Do(context.Background(), func(ctx context.Context) error {
		if err := plans.Save(ctx, newPlan(t, "plan-1")); err != nil {
			return err
		}
		if err := ob.Insert(ctx, outbox.Message{EventID: "e1", EventType: "T"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Do err = %v", err)
	}
	if got, _ := plans.FindByID(context.Background(), "plan-1"); got != nil {
		t.Error("plan survived rollback")
	}
	if len(ob.Messages()) != 0 {
		t.Error("outbox row survived rollback")
	}
}

func TestOutboxRepo_DrainSendsInOrderAndMarksPublished(t *testing.T) {
	ob := memory.NewOutboxRepo()
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if err := ob.Insert(ctx, outbox.Message{EventID: id}); err != nil {
			t.Fatal(err)
		}
	}
	var sent []string
	n, err := ob.Drain(ctx, 2, func(_ context.Context, m outbox.Message) error {
		sent = append(sent, m.EventID)
		return nil
	})
	if err != nil || n != 2 || len(sent) != 2 || sent[0] != "a" || sent[1] != "b" {
		t.Fatalf("first drain = %d %v %v", n, sent, err)
	}
	if ob.Unpublished() != 1 {
		t.Fatalf("Unpublished = %d, want 1", ob.Unpublished())
	}
	n, err = ob.Drain(ctx, 10, func(_ context.Context, m outbox.Message) error {
		sent = append(sent, m.EventID)
		return nil
	})
	if err != nil || n != 1 || sent[2] != "c" || ob.Unpublished() != 0 {
		t.Fatalf("second drain = %d %v %v", n, sent, err)
	}
}

func TestOutboxRepo_DrainStopsAtFirstFailureAndKeepsRow(t *testing.T) {
	ob := memory.NewOutboxRepo()
	ctx := context.Background()
	for _, id := range []string{"a", "b"} {
		_ = ob.Insert(ctx, outbox.Message{EventID: id, EventType: "T"})
	}
	boom := errors.New("broker down")
	calls := 0
	n, err := ob.Drain(ctx, 10, func(_ context.Context, m outbox.Message) error {
		calls++
		if m.EventID == "a" {
			return boom
		}
		return nil
	})
	if !errors.Is(err, boom) || n != 0 || calls != 1 {
		t.Fatalf("drain = %d, %v after %d sends; want 0, boom, 1", n, err, calls)
	}
	if ob.Unpublished() != 2 || ob.LastError(0) == "" {
		t.Errorf("unpublished = %d lastError = %q", ob.Unpublished(), ob.LastError(0))
	}
	// Retry republishes the same message (same id).
	var ids []string
	if _, err := ob.Drain(ctx, 10, func(_ context.Context, m outbox.Message) error { ids = append(ids, m.EventID); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "a" || ob.LastError(0) != "" {
		t.Errorf("retry sent %v (lastError %q)", ids, ob.LastError(0))
	}
}
