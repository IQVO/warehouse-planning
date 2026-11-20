package repocontract

import (
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// CapacityPlanStore is the CapacityPlan store under test: the repository and
// its list side (both adapters implement the two ports on one type).
type CapacityPlanStore interface {
	ports.CapacityPlanRepository
	ports.CapacityPlanLister
}

// The windows the capacity plan cases work with.
var (
	morning   = T0.Add(2 * time.Hour)
	afternoon = T0.Add(10 * time.Hour)
)

// planSpecOf names a stored capacity plan: its identity, scope and labels.
type planSpecOf struct {
	id, site, path string
	start          time.Time
	minute         int // created at T0 + minute minutes
	scenario       string
	forecastPath   string
}

// storedPlan rebuilds a plan of spec the way a repository would read it back
// (an 8h window, a forecast source when it names a forecast path).
func storedPlan(t *testing.T, s planSpecOf) *capacityplan.CapacityPlan {
	t.Helper()
	window, err := processcapacity.NewCapacityWindow(s.start, s.start.Add(8*time.Hour))
	noErr(t, err)
	source := capacityplan.DemandSourceRequest
	if s.forecastPath != "" {
		source = capacityplan.DemandSourceForecast
	}
	return capacityplan.Rehydrate(capacityplan.RehydrateParams{
		ID: s.id, WarehouseID: "WH-1", SiteID: s.site, Location: "SIM1", Window: window, ProcessPathID: s.path,
		AssignedDemand: 100, PathCapacity: 10, BottleneckStep: processcapacity.ProcessType("PICK"), CapacityOverWindow: 80, Shortage: 20,
		Status: capacityplan.StatusDraft, CreatedAt: T0.Add(time.Duration(s.minute) * time.Minute),
		DemandSource: source, ForecastPathID: s.forecastPath, Scenario: s.scenario,
		BottleneckConstraint: processcapacity.ConstraintLabor, Warnings: []string{},
	})
}

func saveCapacityPlans(t *testing.T, fx Fixture, specs ...planSpecOf) {
	t.Helper()
	for _, s := range specs {
		noErr(t, fx.CapacityPlans.Save(ctx, storedPlan(t, s)))
	}
}

// listed renders the ids ListRecent returns for filter, in order.
func listed(t *testing.T, fx Fixture, filter capacityplan.ListFilter) string {
	t.Helper()
	got, err := fx.CapacityPlans.ListRecent(ctx, filter, nil, 50)
	noErr(t, err)
	if got == nil {
		t.Fatal("ListRecent returned a nil slice; want an empty, non-nil one")
	}
	ids := make([]string, 0, len(got))
	for _, p := range got {
		ids = append(ids, p.ID())
	}
	return fmt.Sprint(ids)
}

// scenarioPlans are three scenarios of (siteA, pathP, morning) and their
// neighbours: another site, another path, another window.
var scenarioPlans = []planSpecOf{
	{id: "plan-a", site: siteA, path: pathP, start: morning, minute: 1, scenario: "baseline"},
	{id: "plan-b", site: siteA, path: pathP, start: morning, minute: 2, scenario: "second pack line open"},
	{id: "plan-c", site: siteA, path: pathP, start: morning, minute: 3, forecastPath: "PICK"},
	{id: "plan-d", site: siteB, path: pathP, start: morning, minute: 4},
	{id: "plan-e", site: siteA, path: pathQ, start: morning, minute: 5},
	{id: "plan-f", site: siteA, path: pathP, start: afternoon, minute: 6},
}

var capacityPlanCases = []named{
	{"a plan reads back its scenario and forecast path, an unlabelled one neither", func(t *testing.T, fx Fixture) {
		saveCapacityPlans(t, fx, scenarioPlans[1], scenarioPlans[2])
		labelled, err := fx.CapacityPlans.FindByID(ctx, "plan-b")
		noErr(t, err)
		eq(t, "scenario", labelled.Scenario(), "second pack line open")
		eq(t, "forecast path of a request plan", labelled.ForecastPathID(), "")
		fromForecast, err := fx.CapacityPlans.FindByID(ctx, "plan-c")
		noErr(t, err)
		eq(t, "unlabelled scenario", fromForecast.Scenario(), "")
		eq(t, "forecast path", fromForecast.ForecastPathID(), "PICK")
		eq(t, "demand source", fromForecast.DemandSource(), capacityplan.DemandSourceForecast)
	}},
	{"the list filters are additive and keep newest first", func(t *testing.T, fx Fixture) {
		saveCapacityPlans(t, fx, scenarioPlans...)
		end := morning.Add(8 * time.Hour)
		inZone := morning.In(time.FixedZone("BRT", -3*60*60))
		noPlanStarts := morning.Add(time.Minute)
		for _, c := range []struct {
			name   string
			filter capacityplan.ListFilter
			want   string
		}{
			{"no filter", capacityplan.ListFilter{}, "[plan-f plan-e plan-d plan-c plan-b plan-a]"},
			{"site", capacityplan.ListFilter{SiteID: siteA}, "[plan-f plan-e plan-c plan-b plan-a]"},
			{"site and path", capacityplan.ListFilter{SiteID: siteA, PathID: pathP}, "[plan-f plan-c plan-b plan-a]"},
			{"site, path and window", capacityplan.ListFilter{SiteID: siteA, PathID: pathP, WindowStart: &morning, WindowEnd: &end}, "[plan-c plan-b plan-a]"},
			{"window start alone, in another zone", capacityplan.ListFilter{WindowStart: &inZone}, "[plan-e plan-d plan-c plan-b plan-a]"},
			{"window end alone", capacityplan.ListFilter{WindowEnd: &end}, "[plan-e plan-d plan-c plan-b plan-a]"},
			{"location with the others", capacityplan.ListFilter{Location: "SIM1", SiteID: siteB}, "[plan-d]"},
			{"a filter nothing matches", capacityplan.ListFilter{SiteID: siteB, PathID: pathQ}, "[]"},
			{"a window start that is no plan's", capacityplan.ListFilter{WindowStart: &noPlanStarts}, "[]"},
		} {
			eq(t, c.name, listed(t, fx, c.filter), c.want)
		}
	}},
	{"a listed plan carries its scenario", func(t *testing.T, fx Fixture) {
		saveCapacityPlans(t, fx, scenarioPlans[0])
		got, err := fx.CapacityPlans.ListRecent(ctx, capacityplan.ListFilter{SiteID: siteA}, nil, 1)
		noErr(t, err)
		if len(got) != 1 {
			t.Fatalf("listed %d plans, want 1", len(got))
		}
		eq(t, "listed scenario", got[0].Scenario(), "baseline")
	}},
}
