package memory_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

var (
	_ ports.ProcessPathLister  = (*memory.ProcessPathRepo)(nil)
	_ ports.CapacityPlanLister = (*memory.CapacityPlanRepo)(nil)
)

func listPath(t *testing.T, id string, steps ...processpath.ProcessType) processpath.ProcessPath {
	t.Helper()
	p, err := processpath.NewProcessPath(id, "name of "+id, steps)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestProcessPathRepo_ListOrderedByIDAndNonNilWhenEmpty(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewProcessPathRepo()

	got, err := repo.List(ctx)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("List on empty = %#v, %v; want empty non-nil slice, nil", got, err)
	}

	for _, p := range []processpath.ProcessPath{
		listPath(t, "pick-pack", "PICK", "PACK"),
		listPath(t, "a-first", "PICK"),
		listPath(t, "pick-rebin-pack", "PICK", "REBIN", "PACK"),
	} {
		if err := repo.Save(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	got, err = repo.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, p := range got {
		ids = append(ids, p.ID())
	}
	if fmt.Sprint(ids) != "[a-first pick-pack pick-rebin-pack]" {
		t.Fatalf("ids = %v, want ordered by id", ids)
	}
	if steps := got[2].Steps(); fmt.Sprint(steps) != "[PICK REBIN PACK]" {
		t.Errorf("steps order lost: %v", steps)
	}
}

func listPlan(t *testing.T, id, location string, createdAt time.Time) *capacityplan.CapacityPlan {
	t.Helper()
	window, err := processcapacity.NewCapacityWindow(
		time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC), time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	rate, err := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := capacityplan.Create(capacityplan.CreateParams{
		ID: id, WarehouseID: "WH-1", Location: location, Window: window, ProcessPathID: "pick-rebin-pack",
		AssignedDemand: 6000, PathRate: rate, BottleneckStep: "REBIN",
	}, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func listedIDs(plans []*capacityplan.CapacityPlan) string {
	ids := make([]string, 0, len(plans))
	for _, p := range plans {
		ids = append(ids, p.ID())
	}
	return fmt.Sprint(ids)
}

func TestCapacityPlanRepo_ListRecentNewestFirstFilteredAndLimited(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewCapacityPlanRepo()
	base := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)

	got, err := repo.ListRecent(ctx, "", 10)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("ListRecent on empty = %#v, %v; want empty non-nil slice, nil", got, err)
	}

	for i, s := range []struct{ id, location string }{
		{"p-oldest", "SIM1"}, {"p-other-site", "SIM2"}, {"p-middle", "SIM1"}, {"p-newest", "SIM1"},
	} {
		if err := repo.Save(ctx, listPlan(t, s.id, s.location, base.Add(time.Duration(i)*time.Minute))); err != nil {
			t.Fatal(err)
		}
	}

	all, _ := repo.ListRecent(ctx, "", 10)
	if listedIDs(all) != "[p-newest p-middle p-other-site p-oldest]" {
		t.Errorf("all locations = %s, want newest first", listedIDs(all))
	}
	sim1, _ := repo.ListRecent(ctx, "SIM1", 10)
	if listedIDs(sim1) != "[p-newest p-middle p-oldest]" {
		t.Errorf("SIM1 = %s, want only SIM1, newest first", listedIDs(sim1))
	}
	limited, _ := repo.ListRecent(ctx, "SIM1", 2)
	if listedIDs(limited) != "[p-newest p-middle]" {
		t.Errorf("limit 2 = %s, want the 2 newest", listedIDs(limited))
	}
	exact, _ := repo.ListRecent(ctx, "SIM1", 3)
	if len(exact) != 3 {
		t.Errorf("limit == matches returned %d, want 3", len(exact))
	}
	none, err := repo.ListRecent(ctx, "NOWHERE", 10)
	if err != nil || none == nil || len(none) != 0 {
		t.Errorf("unknown location = %#v, %v; want empty non-nil slice", none, err)
	}
}

func TestCapacityPlanRepo_ListRecentTiesBrokenByIDDescendingAndReturnsCopies(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewCapacityPlanRepo()
	at := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	for _, id := range []string{"plan-a", "plan-c", "plan-b"} {
		if err := repo.Save(ctx, listPlan(t, id, "SIM1", at)); err != nil {
			t.Fatal(err)
		}
	}
	got, _ := repo.ListRecent(ctx, "SIM1", 10)
	if listedIDs(got) != "[plan-c plan-b plan-a]" {
		t.Fatalf("tie order = %s, want id descending", listedIDs(got))
	}

	// A listed plan is a copy: publishing it does not touch the stored one.
	if err := got[0].Publish(at.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	stored, _ := repo.FindByID(ctx, "plan-c")
	if stored.Status() != capacityplan.StatusDraft {
		t.Errorf("stored status = %s after mutating a listed copy, want DRAFT", stored.Status())
	}
}
