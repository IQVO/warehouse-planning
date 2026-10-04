package usecases

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// Window coverage (docs/adr/0003). The fixtures below replicate the LIVE data
// shape found at location SIM1: the labor consumer registers one
// ProcessCapacity per fanned-out ShiftPlanCommitted line with window
// [event.time, event.time + planned_hours), and one commit's lines share the
// SAME event time but have DIFFERENT planned_hours per path -- so no single
// (start, end) pair exists for all steps and an exact-key lookup can never
// resolve a real path.

// liveStart is the shared ShiftPlanCommitted event time of the live rows.
var liveStart = time.Date(2026, 10, 1, 22, 26, 55, 0, time.UTC)

func atH(h float64) time.Time { return liveStart.Add(time.Duration(h * float64(time.Hour))) }

type coverageFixture struct {
	pcs    *memory.ProcessCapacityRepo
	paths  *memory.ProcessPathRepo
	reg    *RegisterProcessCapacityConstraint
	getCap *GetProcessPathCapacity
}

func newCoverageFixture(t *testing.T) *coverageFixture {
	t.Helper()
	pcs, paths := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo()
	return &coverageFixture{
		pcs: pcs, paths: paths,
		reg:    &RegisterProcessCapacityConstraint{Repo: pcs},
		getCap: &GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: memory.NewStationStandardRepo(), Tally: memory.NewStorageTallyRepo()},
	}
}

func (f *coverageFixture) register(t *testing.T, step processcapacity.ProcessType, ct processcapacity.ConstraintType, qty float64, unit processcapacity.CapacityUnit, start, end time.Time) {
	t.Helper()
	if _, err := f.reg.Handle(context.Background(), RegisterProcessCapacityConstraintCommand{
		ProcessType: step, Location: "SIM1", WindowStart: start, WindowEnd: end,
		ConstraintType: ct, Quantity: qty, Unit: unit, Period: time.Hour,
	}); err != nil {
		t.Fatalf("register %s %s: %v", step, ct, err)
	}
}

func (f *coverageFixture) path(t *testing.T, id string, steps ...processpath.ProcessType) {
	t.Helper()
	if _, err := (&RegisterProcessPath{Repo: f.paths}).Handle(context.Background(), RegisterProcessPathCommand{ID: id, Name: id, Steps: steps}); err != nil {
		t.Fatal(err)
	}
}

// seedLive registers the live shape: PICK +32h, REBIN +8h, PACK +24h from the
// SAME start, with the design doc's rates (PICK 4000 UNIT/h, REBIN 2500
// UNIT/h, PACK 1800 PACKAGE/h => 1600 / 1000 / 1800 ORDER/h).
func (f *coverageFixture) seedLive(t *testing.T) {
	t.Helper()
	f.register(t, "PICK", processcapacity.ConstraintLabor, 4000, processcapacity.UnitUnit, liveStart, atH(32))
	f.register(t, "REBIN", processcapacity.ConstraintLabor, 2500, processcapacity.UnitUnit, liveStart, atH(8))
	f.register(t, "PACK", processcapacity.ConstraintLabor, 1800, processcapacity.UnitPackage, liveStart, atH(24))
	f.path(t, "pick-rebin-pack", "PICK", "REBIN", "PACK")
}

// tenPackStations tallies 10 PACK stations at SIM1 and declares 180 PACKAGE/h
// per station (design doc section 19: 1800 PACKAGE/h).
func (f *coverageFixture) tenPackStations(t *testing.T) {
	t.Helper()
	tallies, standards := memory.NewStorageTallyRepo(), memory.NewStationStandardRepo()
	for i := 0; i < 10; i++ {
		if _, err := tallies.RegisterSlot(context.Background(), "SIM1-OPS-WC-"+strconv.Itoa(i), "SIM1-OPS-WC", tally.TypeStation, []string{"PACK"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := (&DeclareStationStandard{Repo: standards}).Handle(context.Background(), DeclareStationStandardCommand{
		Location: "SIM1", ProcessType: "PACK", Quantity: 180, Unit: processcapacity.UnitPackage, Period: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
	f.getCap.Tally, f.getCap.StationStandards = tallies, standards
}

func (f *coverageFixture) capacity(path string, start, end time.Time) (GetProcessPathCapacityResult, error) {
	return f.getCap.Handle(context.Background(), GetProcessPathCapacityCommand{
		ProcessPathID: path, Location: "SIM1", WindowStart: start, WindowEnd: end,
		UnitsPerOrder: f64ptr(2.5), PackagesPerOrder: f64ptr(1),
	})
}

// The live shape resolves for a window strictly inside the intersection of
// the three windows -- impossible with exact (start, end) matching.
func TestGetProcessPathCapacity_LiveShape_ResolvesInsideTheIntersection(t *testing.T) {
	f := newCoverageFixture(t)
	f.seedLive(t)

	res, err := f.capacity("pick-rebin-pack", atH(1), atH(7))
	if err != nil {
		t.Fatalf("a window inside the intersection of PICK/REBIN/PACK must resolve, got %v", err)
	}
	if res.NormalizedRate.Quantity() != 1000 || res.NormalizedRate.Unit() != processcapacity.UnitOrder || res.BottleneckStep != "REBIN" {
		t.Fatalf("got %v %s bottleneck %s, want 1000 ORDER bottleneck REBIN", res.NormalizedRate.Quantity(), res.NormalizedRate.Unit(), res.BottleneckStep)
	}
	if len(res.Steps) != 3 || res.Steps[0].Rate.Quantity() != 1600 || res.Steps[1].Rate.Quantity() != 1000 || res.Steps[2].Rate.Quantity() != 1800 {
		t.Fatalf("step breakdown = %+v, want 1600 / 1000 / 1800", res.Steps)
	}
	if res.BottleneckConstraint != processcapacity.ConstraintLabor || len(res.Warnings) != 0 {
		t.Fatalf("constraint %s warnings %v, want LABOR and none", res.BottleneckConstraint, res.Warnings)
	}
}

// The edges of the intersection: exactly [start, REBIN's end) resolves (a
// window equal to the narrowest one is covered by all of them) and starting
// before the shared start does not.
func TestGetProcessPathCapacity_LiveShape_IntersectionBoundaries(t *testing.T) {
	f := newCoverageFixture(t)
	f.seedLive(t)

	if _, err := f.capacity("pick-rebin-pack", liveStart, atH(8)); err != nil {
		t.Fatalf("exactly the intersection must resolve: %v", err)
	}
	if _, err := f.capacity("pick-rebin-pack", liveStart.Add(-time.Second), atH(8)); !errors.Is(err, processcapacity.ErrMissingStepCapacity) {
		t.Fatalf("one second before the shared start: got %v, want ErrMissingStepCapacity", err)
	}
	if _, err := f.capacity("pick-rebin-pack", liveStart, atH(8).Add(time.Second)); !errors.Is(err, processcapacity.ErrMissingStepCapacity) {
		t.Fatalf("one second past REBIN's end: got %v, want ErrMissingStepCapacity", err)
	}
}

// A window running past REBIN's end (+8h) but inside PICK's and PACK's fails
// with the missing-coverage error naming REBIN, not PICK or PACK.
func TestGetProcessPathCapacity_LiveShape_PastRebinEndNamesRebin(t *testing.T) {
	f := newCoverageFixture(t)
	f.seedLive(t)

	_, err := f.capacity("pick-rebin-pack", atH(1), atH(12))
	if !errors.Is(err, processcapacity.ErrMissingStepCapacity) {
		t.Fatalf("got %v, want ErrMissingStepCapacity", err)
	}
	msg := err.Error()
	if !strings.Contains(msg, "step: REBIN") || strings.Contains(msg, "PICK") || strings.Contains(msg, "PACK") {
		t.Fatalf("message %q must name REBIN only", msg)
	}
	for _, want := range []string{"SIM1", "covers", atH(1).UTC().Format(time.RFC3339), atH(12).UTC().Format(time.RFC3339)} {
		if !strings.Contains(msg, want) {
			t.Fatalf("message %q must say the window is not covered (missing %q)", msg, want)
		}
	}
}

// Newest wins: two LABOR registrations that both cover the window -- the one
// with the LATER start wins whatever the registration order.
func TestGetProcessPathCapacity_NewestCoveringWindowWins(t *testing.T) {
	for name, order := range map[string][2]bool{"older registered first": {true, false}, "newer registered first": {false, true}} {
		t.Run(name, func(t *testing.T) {
			f := newCoverageFixture(t)
			f.path(t, "pick-only", "PICK")
			regs := map[bool]func(){
				true: func() {
					f.register(t, "PICK", processcapacity.ConstraintLabor, 1000, processcapacity.UnitUnit, atH(0), atH(40))
				},
				false: func() {
					f.register(t, "PICK", processcapacity.ConstraintLabor, 4000, processcapacity.UnitUnit, atH(2), atH(40))
				},
			}
			regs[order[0]]()
			regs[!order[0]]()

			res, err := f.capacity("pick-only", atH(3), atH(5))
			if err != nil {
				t.Fatal(err)
			}
			// 4000/2.5 = 1600 (later start) rather than 1000/2.5 = 400.
			if res.NormalizedRate.Quantity() != 1600 {
				t.Fatalf("rate = %v, want 1600 (the later-start LABOR wins)", res.NormalizedRate.Quantity())
			}
		})
	}
}

// A newer aggregate that does NOT cover the window never shadows an older one
// that does.
func TestGetProcessPathCapacity_NonCoveringNewerWindowIsIgnored(t *testing.T) {
	f := newCoverageFixture(t)
	f.path(t, "pick-only", "PICK")
	f.register(t, "PICK", processcapacity.ConstraintLabor, 1000, processcapacity.UnitUnit, atH(0), atH(40))
	f.register(t, "PICK", processcapacity.ConstraintLabor, 4000, processcapacity.UnitUnit, atH(4), atH(6)) // newer, narrower: does not cover [3h, 5h)

	res, err := f.capacity("pick-only", atH(3), atH(5))
	if err != nil {
		t.Fatal(err)
	}
	if res.NormalizedRate.Quantity() != 400 {
		t.Fatalf("rate = %v, want 400 (only the older window covers)", res.NormalizedRate.Quantity())
	}
}

// Shadowing is per constraint type: a LABOR from an older aggregate and an
// EQUIPMENT from a newer one both apply and the minimum is taken.
func TestGetProcessPathCapacity_ShadowingIsPerTypeOnly(t *testing.T) {
	f := newCoverageFixture(t)
	f.path(t, "pick-only", "PICK")
	f.register(t, "PICK", processcapacity.ConstraintLabor, 2500, processcapacity.UnitUnit, atH(0), atH(40))     // 1000 ORDER/h, older
	f.register(t, "PICK", processcapacity.ConstraintEquipment, 1000, processcapacity.UnitUnit, atH(2), atH(40)) // 400 ORDER/h, newer

	res, err := f.capacity("pick-only", atH(3), atH(5))
	if err != nil {
		t.Fatal(err)
	}
	if res.NormalizedRate.Quantity() != 400 || res.BottleneckConstraint != processcapacity.ConstraintEquipment {
		t.Fatalf("got %v bound by %s, want 400 bound by EQUIPMENT (the older LABOR still applies; min taken)", res.NormalizedRate.Quantity(), res.BottleneckConstraint)
	}
}

// Mixed native units across covering aggregates normalize before the min.
func TestGetProcessPathCapacity_MixedNativeUnitsAcrossCoveringAggregates(t *testing.T) {
	f := newCoverageFixture(t)
	f.path(t, "pack-only", "PACK")
	f.register(t, "PACK", processcapacity.ConstraintLabor, 2500, processcapacity.UnitUnit, atH(0), atH(40))        // 1000 ORDER/h
	f.register(t, "PACK", processcapacity.ConstraintEquipment, 1800, processcapacity.UnitPackage, atH(2), atH(40)) // 1800 ORDER/h

	res, err := f.capacity("pack-only", atH(3), atH(5))
	if err != nil {
		t.Fatal(err)
	}
	if res.NormalizedRate.Quantity() != 1000 || res.BottleneckConstraint != processcapacity.ConstraintLabor {
		t.Fatalf("got %v bound by %s, want 1000 bound by LABOR", res.NormalizedRate.Quantity(), res.BottleneckConstraint)
	}
}

// A STATION constraint derived from tallied stations still participates next
// to covering registered constraints.
func TestGetProcessPathCapacity_DerivedStationStillParticipatesWithCovering(t *testing.T) {
	f := newCoverageFixture(t)
	f.path(t, "pack-only", "PACK")
	f.register(t, "PACK", processcapacity.ConstraintLabor, 2500, processcapacity.UnitPackage, atH(0), atH(40))
	f.tenPackStations(t)

	res, err := f.capacity("pack-only", atH(3), atH(5))
	if err != nil {
		t.Fatal(err)
	}
	if res.NormalizedRate.Quantity() != 1800 || res.BottleneckConstraint != processcapacity.ConstraintStation {
		t.Fatalf("got %v bound by %s, want 1800 (10 x 180) bound by STATION", res.NormalizedRate.Quantity(), res.BottleneckConstraint)
	}
}

// A step with NO covering aggregate but stations + standard still resolves
// (derived STATION constraint alone), as before.
func TestGetProcessPathCapacity_StationAloneResolvesWithoutCoveringAggregate(t *testing.T) {
	f := newCoverageFixture(t)
	f.path(t, "pack-only", "PACK")
	f.register(t, "PACK", processcapacity.ConstraintLabor, 2500, processcapacity.UnitPackage, atH(10), atH(20)) // does not cover [3h, 5h)
	f.tenPackStations(t)

	res, err := f.capacity("pack-only", atH(3), atH(5))
	if err != nil {
		t.Fatal(err)
	}
	if res.NormalizedRate.Quantity() != 1800 || res.BottleneckConstraint != processcapacity.ConstraintStation {
		t.Fatalf("got %v bound by %s, want 1800 bound by STATION", res.NormalizedRate.Quantity(), res.BottleneckConstraint)
	}
}

func TestGetProcessPathCapacity_InvertedWindowIsInvalid(t *testing.T) {
	f := newCoverageFixture(t)
	f.seedLive(t)
	for name, w := range map[string][2]time.Time{"end == start": {atH(2), atH(2)}, "end before start": {atH(3), atH(2)}} {
		if _, err := f.capacity("pick-rebin-pack", w[0], w[1]); !errors.Is(err, processcapacity.ErrInvalidWindow) {
			t.Fatalf("%s: got %v, want ErrInvalidWindow", name, err)
		}
	}
}

type coveringFailRepo struct {
	ports.ProcessCapacityRepository
	err error
}

func (r coveringFailRepo) FindCovering(context.Context, processcapacity.ProcessType, string, time.Time, time.Time) ([]*processcapacity.ProcessCapacity, error) {
	return nil, r.err
}

func TestGetProcessPathCapacity_PropagatesFindCoveringErrors(t *testing.T) {
	f := newCoverageFixture(t)
	f.seedLive(t)
	boom := errors.New("boom")
	f.getCap.ProcessCapacities = coveringFailRepo{ProcessCapacityRepository: f.pcs, err: boom}
	if _, err := f.capacity("pick-rebin-pack", atH(1), atH(7)); !errors.Is(err, boom) {
		t.Fatalf("got %v, want the repository error", err)
	}
}

// CreateCapacityPlan shares the composition: the live shape resolves, the plan
// stores its OWN requested window (not a registered one) and the numbers add
// up; a window past REBIN's end fails and stores/queues nothing.
func TestCreateCapacityPlan_LiveShape(t *testing.T) {
	f := newCoverageFixture(t)
	f.seedLive(t)
	plans, ob := memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	create := &CreateCapacityPlan{
		PathCapacity: f.getCap, Plans: plans, Outbox: ob, Encoder: fakeEncoder{},
		UnitOfWork: memory.NewUnitOfWork(f.pcs, plans, ob),
		NewID:      func() string { return "plan-live" },
		Now:        func() time.Time { return planCreatedAt },
	}
	cmd := CreateCapacityPlanCommand{
		WarehouseID: "WH-1", Location: "SIM1", WindowStart: atH(1), WindowEnd: atH(7),
		ProcessPathID: "pick-rebin-pack", AssignedDemand: 7000,
		UnitsPerOrder: f64ptr(2.5), PackagesPerOrder: f64ptr(1),
	}

	plan, err := create.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatalf("live-shaped plan must resolve: %v", err)
	}
	if plan.PathCapacity() != 1000 || plan.BottleneckStep() != "REBIN" || plan.CapacityOverWindow() != 6000 || plan.Shortage() != 1000 {
		t.Fatalf("computed = %v %s %v %v, want 1000 REBIN 6000 1000", plan.PathCapacity(), plan.BottleneckStep(), plan.CapacityOverWindow(), plan.Shortage())
	}
	if !plan.Window().Start().Equal(atH(1)) || !plan.Window().End().Equal(atH(7)) {
		t.Fatalf("plan window = [%v, %v), want the requested [%v, %v)", plan.Window().Start(), plan.Window().End(), atH(1), atH(7))
	}
	if len(ob.Messages()) != 1 {
		t.Fatalf("outbox has %d messages, want 1 (CapacityPlanCreated)", len(ob.Messages()))
	}

	bad := cmd
	bad.WindowEnd = atH(12)
	bad.WindowStart = atH(1)
	if _, err := create.Handle(context.Background(), bad); !errors.Is(err, processcapacity.ErrMissingStepCapacity) || !strings.Contains(err.Error(), "step: REBIN") {
		t.Fatalf("past REBIN's end: got %v, want ErrMissingStepCapacity naming REBIN", err)
	}
	if len(ob.Messages()) != 1 {
		t.Fatalf("a failed plan queued events: %d messages", len(ob.Messages()))
	}
}

// Registration and the exact lookup keep EXACT-key semantics: a covering
// window is not the registered one.
func TestFindByProcessLocationWindow_StaysExactWhileFindCoveringMatchesByCoverage(t *testing.T) {
	f := newCoverageFixture(t)
	f.seedLive(t)
	ctx := context.Background()

	exact, err := f.pcs.FindByProcessLocationWindow(ctx, "PICK", "SIM1", atH(1), atH(7))
	if err != nil || exact != nil {
		t.Fatalf("exact lookup of a covered-but-unregistered window = %v, %v, want nil, nil", exact, err)
	}
	covering, err := f.pcs.FindCovering(ctx, "PICK", "SIM1", atH(1), atH(7))
	if err != nil || len(covering) != 1 {
		t.Fatalf("FindCovering = %v, %v, want one aggregate", covering, err)
	}
}
