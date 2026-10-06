//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// Window coverage against a REAL Postgres (docs/adr/0003): FindCovering's
// range predicate (window_start <= requested start AND window_end >= requested
// end) and its ORDER BY (window_start DESC, window_end ASC).

func covAt(h, m, s int) time.Time { return time.Date(2026, 10, 5, h, m, s, 0, time.UTC) }

func savePCAt(t *testing.T, repo *postgres.ProcessCapacityRepo, process processcapacity.ProcessType, location string,
	start, end time.Time, ctype processcapacity.ConstraintType, qty float64, unit processcapacity.CapacityUnit) {
	t.Helper()
	window, err := processcapacity.NewCapacityWindow(start, end)
	if err != nil {
		t.Fatal(err)
	}
	rate, err := processcapacity.NewCapacityRate(qty, unit, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pc := processcapacity.NewProcessCapacity(process, location, window)
	if err := pc.AddConstraint(ctype, rate); err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(context.Background(), pc); err != nil {
		t.Fatalf("save %s %s [%v, %v): %v", process, location, start, end, err)
	}
}

type win struct{ start, end time.Time }

func windowsOf(pcs []*processcapacity.ProcessCapacity) []win {
	out := make([]win, 0, len(pcs))
	for _, pc := range pcs {
		out = append(out, win{pc.Window().Start(), pc.Window().End()})
	}
	return out
}

func sameWindows(got, want []win) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if !got[i].start.Equal(want[i].start) || !got[i].end.Equal(want[i].end) {
			return false
		}
	}
	return true
}

// The requested window is [10:00:00, 14:00:00). Windows that cover it, windows
// that miss it by exactly one second on either side, and rows of another
// location / process that would otherwise match.
func TestProcessCapacityRepo_Postgres_FindCovering_OrderingAndBoundaries(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewProcessCapacityRepo(pool)
	ctx := context.Background()
	reqStart, reqEnd := covAt(10, 0, 0), covAt(14, 0, 0)

	labor := processcapacity.ConstraintLabor
	unit := processcapacity.UnitUnit
	// Covering, inserted in an order unrelated to the expected result order.
	savePCAt(t, repo, "PICK", "SIM1", covAt(8, 0, 0), covAt(20, 0, 0), labor, 1100, unit)  // widest, oldest start
	savePCAt(t, repo, "PICK", "SIM1", covAt(9, 0, 0), covAt(20, 0, 0), labor, 1200, unit)  // later start, wider end
	savePCAt(t, repo, "PICK", "SIM1", covAt(10, 0, 0), covAt(14, 0, 0), labor, 1300, unit) // exactly the requested window
	savePCAt(t, repo, "PICK", "SIM1", covAt(9, 0, 0), covAt(15, 0, 0), labor, 1400, unit)  // same start as 1200, narrower end
	// Do NOT cover: one second off on either boundary, partial overlaps, disjoint.
	savePCAt(t, repo, "PICK", "SIM1", covAt(10, 0, 1), covAt(14, 0, 0), labor, 2100, unit)   // starts one second late
	savePCAt(t, repo, "PICK", "SIM1", covAt(10, 0, 0), covAt(13, 59, 59), labor, 2200, unit) // ends one second early
	savePCAt(t, repo, "PICK", "SIM1", covAt(11, 0, 0), covAt(20, 0, 0), labor, 2300, unit)   // overlaps only the tail
	savePCAt(t, repo, "PICK", "SIM1", covAt(15, 0, 0), covAt(20, 0, 0), labor, 2400, unit)   // disjoint
	// Same window, other location / other process.
	savePCAt(t, repo, "PICK", "SIM2", covAt(8, 0, 0), covAt(20, 0, 0), labor, 3100, unit)
	savePCAt(t, repo, "PACK", "SIM1", covAt(8, 0, 0), covAt(20, 0, 0), labor, 3200, processcapacity.UnitPackage)

	got, err := repo.FindCovering(ctx, "PICK", "SIM1", reqStart, reqEnd)
	if err != nil {
		t.Fatalf("FindCovering: %v", err)
	}
	// Newest start first; on equal starts the narrower end first.
	want := []win{
		{covAt(10, 0, 0), covAt(14, 0, 0)},
		{covAt(9, 0, 0), covAt(15, 0, 0)},
		{covAt(9, 0, 0), covAt(20, 0, 0)},
		{covAt(8, 0, 0), covAt(20, 0, 0)},
	}
	if !sameWindows(windowsOf(got), want) {
		t.Fatalf("FindCovering windows = %v, want %v", windowsOf(got), want)
	}
	// Each aggregate comes back whole: its constraint rows round-trip.
	for i, wantQty := range []float64{1300, 1400, 1200, 1100} {
		entries := got[i].Constraints()
		if len(entries) != 1 || entries[0].Type != labor || entries[0].Rate.Quantity() != wantQty ||
			entries[0].Rate.Unit() != unit || entries[0].Rate.Period() != time.Hour {
			t.Errorf("covering[%d] constraints = %+v, want one LABOR %v UNIT/h", i, entries, wantQty)
		}
		if got[i].ProcessType() != "PICK" || got[i].Location() != "SIM1" || got[i].NativeUnit() != unit {
			t.Errorf("covering[%d] identity = %s/%s/%s", i, got[i].ProcessType(), got[i].Location(), got[i].NativeUnit())
		}
	}

	// Nothing covers a window that starts before every registered window.
	none, err := repo.FindCovering(ctx, "PICK", "SIM1", covAt(7, 59, 59), covAt(9, 0, 0))
	if err != nil || none == nil || len(none) != 0 {
		t.Errorf("uncovered window = %v, %v, want an empty non-nil slice and no error", none, err)
	}
	// An unknown location is empty, not an error.
	if none, err := repo.FindCovering(ctx, "PICK", "NOWHERE", reqStart, reqEnd); err != nil || len(none) != 0 {
		t.Errorf("unknown location = %v, %v, want empty", none, err)
	}
	// An inverted or zero-length requested window is rejected before any query.
	if _, err := repo.FindCovering(ctx, "PICK", "SIM1", reqEnd, reqStart); !errors.Is(err, processcapacity.ErrInvalidWindow) {
		t.Errorf("inverted window error = %v, want ErrInvalidWindow", err)
	}
	if _, err := repo.FindCovering(ctx, "PICK", "SIM1", reqStart, reqStart); !errors.Is(err, processcapacity.ErrInvalidWindow) {
		t.Errorf("zero-length window error = %v, want ErrInvalidWindow", err)
	}

	// The exact-key lookup is unchanged: it does NOT find a covering window.
	if exact, err := repo.FindByProcessLocationWindow(ctx, "PICK", "SIM1", covAt(9, 30, 0), covAt(14, 0, 0)); err != nil || exact != nil {
		t.Errorf("exact lookup of an unregistered key = %v, %v, want nil, nil", exact, err)
	}
}

// Boundaries one at a time, each against ONE row, so a wrong comparison
// operator in the SQL flips exactly one of these.
func TestProcessCapacityRepo_Postgres_FindCovering_EachBoundaryAlone(t *testing.T) {
	pool := newMigratedPool(t)
	repo := postgres.NewProcessCapacityRepo(pool)
	ctx := context.Background()
	labor, unit := processcapacity.ConstraintLabor, processcapacity.UnitUnit

	cases := []struct {
		name       string
		start, end time.Time
		covers     bool
	}{
		{"equal window covers", covAt(10, 0, 0), covAt(14, 0, 0), true},
		{"start one second earlier covers", covAt(9, 59, 59), covAt(14, 0, 0), true},
		{"end one second later covers", covAt(10, 0, 0), covAt(14, 0, 1), true},
		{"start one second later does not cover", covAt(10, 0, 1), covAt(14, 0, 0), false},
		{"end one second earlier does not cover", covAt(10, 0, 0), covAt(13, 59, 59), false},
	}
	for i, c := range cases {
		location := "LOC" + string(rune('A'+i))
		savePCAt(t, repo, "PICK", location, c.start, c.end, labor, 1000, unit)
		got, err := repo.FindCovering(ctx, "PICK", location, covAt(10, 0, 0), covAt(14, 0, 0))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if covers := len(got) == 1; covers != c.covers {
			t.Errorf("%s: covered = %v (%d rows), want %v", c.name, covers, len(got), c.covers)
		}
	}
}

// liveStack wires the real use cases over the Postgres capacity/tally/standard
// repositories (the path repository stays in memory, as everywhere in this
// package's integration tests).
type liveStack struct {
	pool     *pgxpool.Pool
	pcs      *postgres.ProcessCapacityRepo
	register *usecases.RegisterProcessCapacityConstraint
	getCap   *usecases.GetProcessPathCapacity
	create   *usecases.CreateCapacityPlan
}

func newLiveStack(t *testing.T) *liveStack {
	t.Helper()
	pool := newMigratedPool(t)
	pcs, paths := postgres.NewProcessCapacityRepo(pool), memory.NewProcessPathRepo()
	getCap := &usecases.GetProcessPathCapacity{
		ProcessPaths: paths, ProcessCapacities: pcs,
		StationStandards: postgres.NewStationStandardRepo(pool), Tally: postgres.NewStorageTallyRepo(pool),
	}
	if _, err := (&usecases.RegisterProcessPath{Repo: paths}).Handle(context.Background(), usecases.RegisterProcessPathCommand{
		ID: "pick-rebin-pack", Name: "Pick-Rebin-Pack", Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"},
	}); err != nil {
		t.Fatalf("register path: %v", err)
	}
	return &liveStack{
		pool: pool, pcs: pcs, register: &usecases.RegisterProcessCapacityConstraint{Repo: pcs}, getCap: getCap,
		create: &usecases.CreateCapacityPlan{
			PathCapacity: getCap, Plans: postgres.NewCapacityPlanRepo(pool), Outbox: postgres.NewOutboxRepo(pool),
			Encoder: outboundkafka.NewEncoder(), UnitOfWork: postgres.NewUnitOfWork(pool),
		},
	}
}

// registerLive writes the rows the labor consumer wrote at SIM1: ONE shared
// start, ends +32h (PICK), +8h (REBIN), +24h (PACK).
func (s *liveStack) registerLive(t *testing.T, location string) {
	t.Helper()
	start := covAt(8, 0, 0)
	for _, reg := range []struct {
		process processcapacity.ProcessType
		qty     float64
		unit    processcapacity.CapacityUnit
		hours   time.Duration
	}{
		{"PICK", 4000, processcapacity.UnitUnit, 32},
		{"REBIN", 2500, processcapacity.UnitUnit, 8},
		{"PACK", 1800, processcapacity.UnitPackage, 24},
	} {
		if _, err := s.register.Handle(context.Background(), usecases.RegisterProcessCapacityConstraintCommand{
			ProcessType: reg.process, Location: location, WindowStart: start, WindowEnd: start.Add(reg.hours * time.Hour),
			ConstraintType: processcapacity.ConstraintLabor, Quantity: reg.qty, Unit: reg.unit, Period: time.Hour,
		}); err != nil {
			t.Fatalf("register %s: %v", reg.process, err)
		}
	}
}

// The live SIM1 shape end to end over Postgres: no exact (start, end) exists
// for the path, yet a window inside the intersection resolves to
// 1000 ORDER/h bound by REBIN, a plan over it computes the shortage from the
// REQUESTED 6h window, and a window past REBIN's end is a missing-coverage
// error naming REBIN.
func TestWindowCoverage_Postgres_LiveShapedPathCapacityAndPlan(t *testing.T) {
	s := newLiveStack(t)
	ctx := context.Background()
	s.registerLive(t, "SIM1")
	upo, ppo := 2.5, 1.0

	got, err := s.getCap.Handle(ctx, usecases.GetProcessPathCapacityCommand{
		ProcessPathID: "pick-rebin-pack", Location: "SIM1", WindowStart: covAt(9, 0, 0), WindowEnd: covAt(15, 0, 0),
		UnitsPerOrder: &upo, PackagesPerOrder: &ppo,
	})
	if err != nil {
		t.Fatalf("GetProcessPathCapacity over live-shaped windows: %v", err)
	}
	if got.NormalizedRate.Quantity() != 1000 || got.NormalizedRate.Unit() != processcapacity.UnitOrder ||
		got.NormalizedRate.Period() != time.Hour || got.BottleneckStep != "REBIN" ||
		got.BottleneckConstraint != processcapacity.ConstraintLabor || len(got.Steps) != 3 {
		t.Errorf("path capacity = %v %s/%v bound by %s/%s (%d steps), want 1000 ORDER/h bound by REBIN/LABOR (3 steps)",
			got.NormalizedRate.Quantity(), got.NormalizedRate.Unit(), got.NormalizedRate.Period(),
			got.BottleneckStep, got.BottleneckConstraint, len(got.Steps))
	}

	plan, err := s.create.Handle(ctx, usecases.CreateCapacityPlanCommand{
		WarehouseID: "WH-1", SiteID: "SIM1", Location: "SIM1", WindowStart: covAt(9, 0, 0), WindowEnd: covAt(15, 0, 0),
		ProcessPathID: "pick-rebin-pack", AssignedDemand: 12000, UnitsPerOrder: &upo, PackagesPerOrder: &ppo,
	})
	if err != nil {
		t.Fatalf("CreateCapacityPlan over live-shaped windows: %v", err)
	}
	if plan.PathCapacity() != 1000 || plan.BottleneckStep() != "REBIN" || plan.CapacityOverWindow() != 6000 || plan.Shortage() != 6000 {
		t.Errorf("plan = %v/h bound by %s, over window %v, shortage %v; want 1000/h, REBIN, 6000, 6000 (the requested 6h window)",
			plan.PathCapacity(), plan.BottleneckStep(), plan.CapacityOverWindow(), plan.Shortage())
	}
	stored, err := postgres.NewCapacityPlanRepo(s.pool).FindByID(ctx, plan.ID())
	if err != nil || stored == nil || !stored.Window().Start().Equal(covAt(9, 0, 0)) || !stored.Window().End().Equal(covAt(15, 0, 0)) {
		t.Errorf("stored plan window is not the REQUESTED window: %+v, %v", stored, err)
	}

	// Past REBIN's end (16:00): not covered, and the error names REBIN.
	_, err = s.getCap.Handle(ctx, usecases.GetProcessPathCapacityCommand{
		ProcessPathID: "pick-rebin-pack", Location: "SIM1", WindowStart: covAt(9, 0, 0), WindowEnd: covAt(17, 0, 0),
		UnitsPerOrder: &upo, PackagesPerOrder: &ppo,
	})
	if !errors.Is(err, processcapacity.ErrMissingStepCapacity) {
		t.Fatalf("window past REBIN's end: err = %v, want ErrMissingStepCapacity", err)
	}
	if msg := err.Error(); !strings.Contains(msg, "REBIN") {
		t.Errorf("missing-coverage error %q does not name REBIN", msg)
	}
}

// FindCovering inside a UnitOfWork shares ONE connection: the header cursor
// must be closed before the constraint queries run, or this fails with
// "conn busy".
func TestProcessCapacityRepo_Postgres_FindCovering_InsideUnitOfWork(t *testing.T) {
	s := newLiveStack(t)
	ctx := context.Background()
	s.registerLive(t, "SIM1")
	// A second covering aggregate for PICK so the header loop runs more than once.
	savePCAt(t, s.pcs, "PICK", "SIM1", covAt(9, 0, 0), covAt(20, 0, 0), processcapacity.ConstraintEquipment, 5000, processcapacity.UnitUnit)

	err := postgres.NewUnitOfWork(s.pool).Do(ctx, func(ctx context.Context) error {
		got, err := s.pcs.FindCovering(ctx, "PICK", "SIM1", covAt(10, 0, 0), covAt(12, 0, 0))
		if err != nil {
			return err
		}
		want := []win{{covAt(9, 0, 0), covAt(20, 0, 0)}, {covAt(8, 0, 0), covAt(8, 0, 0).Add(32 * time.Hour)}}
		if !sameWindows(windowsOf(got), want) {
			t.Errorf("windows inside UoW = %v, want %v", windowsOf(got), want)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("FindCovering inside UnitOfWork: %v", err)
	}
}
