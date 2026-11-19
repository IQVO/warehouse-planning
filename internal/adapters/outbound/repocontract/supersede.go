package repocontract

import (
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// docs/adr/0024 decision 3 on the store: one PUBLISHED plan per
// capacityplan.SupersedeKey (Postgres: migration 0017's partial unique
// index), a SUPERSEDED plan reads back with superseded_by, and FindPublished
// finds the plan a publish supersedes.

// publishedPlan is spec's plan, published at T0 + minute minutes.
func publishedPlan(t *testing.T, s planSpecOf, minute int) *capacityplan.CapacityPlan {
	t.Helper()
	plan := storedPlan(t, s)
	noErr(t, plan.Publish(T0.Add(time.Duration(minute)*time.Minute)))
	return plan
}

// The plans of one key (siteA, pathP, morning) and of three neighbours.
var (
	keyFirst   = planSpecOf{id: "plan-a", site: siteA, path: pathP, start: morning, minute: 1}
	keySecond  = planSpecOf{id: "plan-b", site: siteA, path: pathP, start: morning, minute: 2}
	otherSite  = planSpecOf{id: "plan-d", site: siteB, path: pathP, start: morning, minute: 4}
	otherPath  = planSpecOf{id: "plan-e", site: siteA, path: pathQ, start: morning, minute: 5}
	otherStart = planSpecOf{id: "plan-f", site: siteA, path: pathP, start: afternoon, minute: 6}
)

func findPublished(t *testing.T, fx Fixture, of *capacityplan.CapacityPlan, exclude string) string {
	t.Helper()
	got, err := fx.CapacityPlans.FindPublished(ctx, of.SupersedeKey(), exclude)
	noErr(t, err)
	if got == nil {
		return ""
	}
	return got.ID()
}

var supersedeCases = []named{
	{"a second PUBLISHED plan of one key is a version conflict and is not stored", func(t *testing.T, fx Fixture) {
		noErr(t, fx.CapacityPlans.Save(ctx, publishedPlan(t, keyFirst, 10)))
		wantErr(t, fx.CapacityPlans.Save(ctx, publishedPlan(t, keySecond, 11)), ports.ErrVersionConflict)
		second, err := fx.CapacityPlans.FindByID(ctx, "plan-b")
		noErr(t, err)
		if second != nil {
			t.Fatalf("the refused plan was stored as %s", second.Status())
		}
		noErr(t, fx.CapacityPlans.Save(ctx, storedPlan(t, keySecond)))       // a DRAFT of the key is a scenario
		noErr(t, fx.CapacityPlans.Save(ctx, publishedPlan(t, keyFirst, 12))) // re-saving the PUBLISHED plan itself
		for _, neighbour := range []planSpecOf{otherSite, otherPath, otherStart} {
			noErr(t, fx.CapacityPlans.Save(ctx, publishedPlan(t, neighbour, 13)))
		}
	}},
	{"superseding the published plan first lets the newer one be published", func(t *testing.T, fx Fixture) {
		older := publishedPlan(t, keyFirst, 10)
		noErr(t, fx.CapacityPlans.Save(ctx, older))
		noErr(t, older.Supersede("plan-b", T0.Add(20*time.Minute)))
		noErr(t, fx.CapacityPlans.Save(ctx, older))
		noErr(t, fx.CapacityPlans.Save(ctx, publishedPlan(t, keySecond, 20)))

		stored, err := fx.CapacityPlans.FindByID(ctx, "plan-a")
		noErr(t, err)
		eq(t, "status", stored.Status(), capacityplan.StatusSuperseded)
		eq(t, "superseded_by", stored.SupersededBy(), "plan-b")
		eq(t, "published_at kept", stored.PublishedAt().Equal(T0.Add(10*time.Minute)), true)
		newer, err := fx.CapacityPlans.FindByID(ctx, "plan-b")
		noErr(t, err)
		eq(t, "newer status", newer.Status(), capacityplan.StatusPublished)
		eq(t, "newer superseded_by", newer.SupersededBy(), "")
		listed, err := fx.CapacityPlans.ListRecent(ctx, capacityplan.ListFilter{SiteID: siteA, PathID: pathP}, nil, 5)
		noErr(t, err)
		eq(t, "listed", len(listed), 2)
		eq(t, "listed superseded_by", listed[1].SupersededBy(), "plan-b")
	}},
	{"FindPublished finds the PUBLISHED plan of the key, never the excluded one", func(t *testing.T, fx Fixture) {
		noErr(t, fx.CapacityPlans.Save(ctx, publishedPlan(t, keyFirst, 10)))
		noErr(t, fx.CapacityPlans.Save(ctx, storedPlan(t, keySecond)))
		noErr(t, fx.CapacityPlans.Save(ctx, publishedPlan(t, otherPath, 11)))
		draft := storedPlan(t, keySecond)
		eq(t, "for the draft of the key", findPublished(t, fx, draft, "plan-b"), "plan-a")
		eq(t, "excluding the published one", findPublished(t, fx, draft, "plan-a"), "")
		eq(t, "another path", findPublished(t, fx, storedPlan(t, otherPath), "plan-x"), "plan-e")
		eq(t, "another site", findPublished(t, fx, storedPlan(t, otherSite), "plan-x"), "")
		eq(t, "another window", findPublished(t, fx, storedPlan(t, otherStart), "plan-x"), "")
	}},
	{"FindPublished ignores DRAFT and SUPERSEDED plans of the key", func(t *testing.T, fx Fixture) {
		older := publishedPlan(t, keyFirst, 10)
		noErr(t, older.Supersede("plan-b", T0.Add(20*time.Minute)))
		noErr(t, fx.CapacityPlans.Save(ctx, older))
		noErr(t, fx.CapacityPlans.Save(ctx, storedPlan(t, keySecond)))
		eq(t, "no published plan", findPublished(t, fx, storedPlan(t, keySecond), "plan-x"), "")
	}},
}
