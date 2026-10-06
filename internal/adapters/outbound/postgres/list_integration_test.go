//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

var (
	_ ports.ProcessPathLister  = (*postgres.ProcessPathRepo)(nil)
	_ ports.CapacityPlanLister = (*postgres.CapacityPlanRepo)(nil)
)

func TestProcessPathRepo_ListOrderedByIDKeepsStepOrder(t *testing.T) {
	pool := newMigratedPool(t)
	ctx := context.Background()
	repo := postgres.NewProcessPathRepo(pool)

	got, err := repo.List(ctx)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("List on empty = %#v, %v; want empty non-nil slice", got, err)
	}

	for _, p := range []processpath.ProcessPath{
		mustPath(t, "pick-rebin-pack", "Pick-Rebin-Pack", "PICK", "REBIN", "PACK", "PICK"),
		mustPath(t, "a-first", "First", "PACK"),
	} {
		if err := repo.Save(ctx, p); err != nil {
			t.Fatal(err)
		}
	}
	got, err = repo.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID() != "a-first" || got[1].ID() != "pick-rebin-pack" {
		t.Fatalf("List = %v, want ordered by id", got)
	}
	if fmt.Sprint(got[1].Steps()) != "[PICK REBIN PACK PICK]" || got[1].Name() != "Pick-Rebin-Pack" {
		t.Errorf("path = %v %v, want declared step order and name", got[1].Name(), got[1].Steps())
	}
}

func savePlan(t *testing.T, repo *postgres.CapacityPlanRepo, id, location string, createdAt time.Time) {
	t.Helper()
	window, err := processcapacity.NewCapacityWindow(planStart, planEnd)
	if err != nil {
		t.Fatal(err)
	}
	rate, err := processcapacity.NewCapacityRate(1000, processcapacity.UnitOrder, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := capacityplan.Create(capacityplan.CreateParams{
		ID: id, WarehouseID: "WH-1", SiteID: "SIM1", Location: location, Window: window, ProcessPathID: "pick-rebin-pack",
		AssignedDemand: 12000, PathRate: rate, BottleneckStep: "REBIN",
		BottleneckConstraint: processcapacity.ConstraintLabor, Warnings: []string{"no station standard for PACK"},
		DemandSource: capacityplan.DemandSourceOrders,
	}, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(context.Background(), plan); err != nil {
		t.Fatal(err)
	}
}

func TestCapacityPlanRepo_ListRecentNewestFirstFilteredLimitedAndFaithful(t *testing.T) {
	pool := newMigratedPool(t)
	ctx := context.Background()
	repo := postgres.NewCapacityPlanRepo(pool)

	got, err := repo.ListRecent(ctx, "", 10)
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("ListRecent on empty = %#v, %v; want empty non-nil slice", got, err)
	}

	base := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	savePlan(t, repo, "p-oldest", "SIM1", base)
	savePlan(t, repo, "p-other", "SIM2", base.Add(time.Minute))
	savePlan(t, repo, "p-middle", "SIM1", base.Add(2*time.Minute))
	savePlan(t, repo, "p-tie-a", "SIM1", base.Add(3*time.Minute))
	savePlan(t, repo, "p-tie-b", "SIM1", base.Add(3*time.Minute))

	ids := func(plans []*capacityplan.CapacityPlan) string {
		out := make([]string, 0, len(plans))
		for _, p := range plans {
			out = append(out, p.ID())
		}
		return fmt.Sprint(out)
	}
	all, _ := repo.ListRecent(ctx, "", 10)
	if ids(all) != "[p-tie-b p-tie-a p-middle p-other p-oldest]" {
		t.Errorf("all = %s, want newest first with id-descending ties", ids(all))
	}
	sim1, _ := repo.ListRecent(ctx, "SIM1", 10)
	if ids(sim1) != "[p-tie-b p-tie-a p-middle p-oldest]" {
		t.Errorf("SIM1 = %s", ids(sim1))
	}
	limited, _ := repo.ListRecent(ctx, "SIM1", 2)
	if ids(limited) != "[p-tie-b p-tie-a]" {
		t.Errorf("limit 2 = %s", ids(limited))
	}
	none, err := repo.ListRecent(ctx, "NOWHERE", 10)
	if err != nil || none == nil || len(none) != 0 {
		t.Errorf("unknown location = %#v, %v; want empty non-nil slice", none, err)
	}

	// A listed plan is the same plan FindByID loads (every persisted field).
	listed := sim1[3]
	found, err := repo.FindByID(ctx, "p-oldest")
	if err != nil || found == nil {
		t.Fatalf("FindByID = %v, %v", found, err)
	}
	if listed.ID() != found.ID() || listed.Location() != found.Location() ||
		listed.AssignedDemand() != found.AssignedDemand() || listed.Shortage() != found.Shortage() ||
		listed.Status() != found.Status() || !listed.CreatedAt().Equal(found.CreatedAt()) ||
		listed.DemandSource() != found.DemandSource() || listed.BottleneckConstraint() != found.BottleneckConstraint() ||
		fmt.Sprint(listed.Warnings()) != fmt.Sprint(found.Warnings()) ||
		!listed.Window().Start().Equal(found.Window().Start()) || !listed.Window().End().Equal(found.Window().End()) {
		t.Errorf("listed plan differs from FindByID:\n list: %+v\n find: %+v", listed, found)
	}
	if listed.DemandSource() != capacityplan.DemandSourceOrders || fmt.Sprint(listed.Warnings()) != "[no station standard for PACK]" {
		t.Errorf("additive fields lost: source=%s warnings=%v", listed.DemandSource(), listed.Warnings())
	}
}
