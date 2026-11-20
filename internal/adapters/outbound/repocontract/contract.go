// Package repocontract is the repository contract suite for the versioned
// aggregates of docs/adr/0023 (WorkloadForecast, LabourPlan) and the
// committed heads of its labour plan drift: the behaviour every
// implementation of ports.WorkloadForecastRepository,
// ports.LabourPlanRepository and ports.CommittedHeadsRepository must show,
// run against the in-memory doubles
// (internal/adapters/outbound/memory, always) and against Postgres
// (internal/adapters/outbound/postgres, -tags=integration). It is test
// support (it imports "testing") but not a _test.go file so that both adapter
// packages can run it; cmd/** must never import it. The shape is the fleet's
// (delivery-promise, customer-ordering: repocontract.Run over a Fixture).
package repocontract

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

// Fixture is one implementation under test, empty at the start of each case.
// UoW must be the unit of work the repositories join. Commitments is the
// committed side of the labour plan drift (docs/adr/0023 decision 3).
type Fixture struct {
	UoW         ports.UnitOfWork
	Forecasts   ports.WorkloadForecastRepository
	Plans       ports.LabourPlanRepository
	Commitments ports.CommittedHeadsRepository
	// CapacityPlans is the CapacityPlan store: its scenario label, forecast
	// path and additive list filters (docs/adr/0024).
	CapacityPlans CapacityPlanStore
}

type named struct {
	name string
	fn   func(*testing.T, Fixture)
}

// Run executes every case of the suite, each on a fresh Fixture.
func Run(t *testing.T, newFixture func(t *testing.T) Fixture) {
	t.Helper()
	groups := []struct {
		name  string
		cases []named
	}{
		{"workload forecasts", versionedCases(forecasts)},
		{"labour plans", versionedCases(plans)},
		{"labour plan list", listCases},
		{"unit of work", unitOfWorkCases},
		{"committed heads", committedHeadsCases},
		{"capacity plans", capacityPlanCases},
		{"superseded capacity plans", supersedeCases},
	}
	for _, g := range groups {
		for _, c := range g.cases {
			t.Run(g.name+"/"+c.name, func(t *testing.T) { c.fn(t, newFixture(t)) })
		}
	}
}

// The identities every case works with.
const (
	siteA  = "SIM1"
	siteB  = "SIM2"
	shift1 = "2026-11-18-day"
	shift2 = "2026-11-18-night"
	// Path codes are canonical (trimmed, upper case): the only form the
	// domain hands a repository.
	pathP = "PICK-REBIN-PACK"
	pathQ = "PICK-PACK"
)

var ctx = context.Background()

// T0 is the fixed instant every builder works from: whole seconds, because a
// timestamptz column keeps microseconds.
var T0 = time.Date(2026, 11, 18, 6, 0, 0, 0, time.UTC)

func noErr(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func wantErr(t *testing.T, err, want error) {
	t.Helper()
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
}

func eq[T comparable](t *testing.T, name string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s = %v, want %v", name, got, want)
	}
}
