package memory_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var _ ports.CapacityPlanLister = (*memory.CapacityPlanRepo)(nil)

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
		ID: id, WarehouseID: "WH-1", SiteID: "SIM1", Location: location, Window: window, ProcessPathID: "pick-rebin-pack",
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

	got, err := repo.ListRecent(ctx, capacityplan.ListFilter{}, nil, 10)
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

	all, _ := repo.ListRecent(ctx, capacityplan.ListFilter{}, nil, 10)
	if listedIDs(all) != "[p-newest p-middle p-other-site p-oldest]" {
		t.Errorf("all locations = %s, want newest first", listedIDs(all))
	}
	sim1, _ := repo.ListRecent(ctx, capacityplan.ListFilter{Location: "SIM1"}, nil, 10)
	if listedIDs(sim1) != "[p-newest p-middle p-oldest]" {
		t.Errorf("SIM1 = %s, want only SIM1, newest first", listedIDs(sim1))
	}
	limited, _ := repo.ListRecent(ctx, capacityplan.ListFilter{Location: "SIM1"}, nil, 2)
	if listedIDs(limited) != "[p-newest p-middle]" {
		t.Errorf("limit 2 = %s, want the 2 newest", listedIDs(limited))
	}
	exact, _ := repo.ListRecent(ctx, capacityplan.ListFilter{Location: "SIM1"}, nil, 3)
	if len(exact) != 3 {
		t.Errorf("limit == matches returned %d, want 3", len(exact))
	}
	none, err := repo.ListRecent(ctx, capacityplan.ListFilter{Location: "NOWHERE"}, nil, 10)
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
	got, _ := repo.ListRecent(ctx, capacityplan.ListFilter{Location: "SIM1"}, nil, 10)
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

func TestCapacityPlanRepo_ListRecentWalksTiesWithoutDuplicatesOrSkips(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewCapacityPlanRepo()
	at := time.Date(2026, 10, 4, 9, 0, 0, 123456789, time.UTC)
	for i := 0; i < 7; i++ { // five of the seven share one created_at
		when := at
		if i >= 5 {
			when = at.Add(time.Duration(i) * time.Minute)
		}
		if err := repo.Save(ctx, listPlan(t, fmt.Sprintf("plan-%d", i), "SIM1", when)); err != nil {
			t.Fatal(err)
		}
	}
	seen := map[string]bool{}
	var after *pagecursor.Cursor
	for {
		rows, err := repo.ListRecent(ctx, capacityplan.ListFilter{Location: "SIM1"}, after, 2)
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 {
			break
		}
		for _, p := range rows {
			if seen[p.ID()] {
				t.Fatalf("duplicate %s", p.ID())
			}
			seen[p.ID()] = true
		}
		last := rows[len(rows)-1]
		after = &pagecursor.Cursor{Sort: last.CreatedAt().UTC().Format(time.RFC3339Nano), ID: last.ID()}
	}
	if len(seen) != 7 {
		t.Fatalf("skipped rows: saw %d of 7", len(seen))
	}
}
