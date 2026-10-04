package usecases

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

var errBoom = errors.New("boom")

// compositionFixture is the site-SIM1 world of the station-capacity
// composition tests, wired over in-memory repos exactly as the composition
// roots wire the real ones.
type compositionFixture struct {
	pcs       *memory.ProcessCapacityRepo
	paths     *memory.ProcessPathRepo
	standards *memory.StationStandardRepo
	tally     *memory.StorageTallyRepo
	uc        *GetProcessPathCapacity
	// start/end is the window the fixture registers LABOR under and reads.
	start, end time.Time
}

func newCompositionFixture(t *testing.T) *compositionFixture {
	t.Helper()
	start, end := pickZoneAWindowTimes()
	f := &compositionFixture{
		start: start, end: end,
		pcs: memory.NewProcessCapacityRepo(), paths: memory.NewProcessPathRepo(),
		standards: memory.NewStationStandardRepo(), tally: memory.NewStorageTallyRepo(),
	}
	f.uc = &GetProcessPathCapacity{ProcessPaths: f.paths, ProcessCapacities: f.pcs, StationStandards: f.standards, Tally: f.tally}
	return f
}

func (f *compositionFixture) labor(t *testing.T, process processcapacity.ProcessType, qty float64, unit processcapacity.CapacityUnit) {
	t.Helper()
	_, err := (&RegisterProcessCapacityConstraint{Repo: f.pcs}).Handle(context.Background(), RegisterProcessCapacityConstraintCommand{
		ProcessType: process, Location: "SIM1", WindowStart: f.start, WindowEnd: f.end,
		ConstraintType: processcapacity.ConstraintLabor, Quantity: qty, Unit: unit, Period: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (f *compositionFixture) path(t *testing.T, id string, steps ...processpath.ProcessType) {
	t.Helper()
	if _, err := (&RegisterProcessPath{Repo: f.paths}).Handle(context.Background(), RegisterProcessPathCommand{ID: id, Name: id, Steps: steps}); err != nil {
		t.Fatal(err)
	}
}

func (f *compositionFixture) stations(t *testing.T, zone, activity string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		code := zone + "-" + activity + "-" + strconv.Itoa(i)
		if _, err := f.tally.RegisterSlot(context.Background(), code, zone, tally.TypeStation, []string{activity}); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *compositionFixture) declare(t *testing.T, location string, process processcapacity.ProcessType, qty float64, unit processcapacity.CapacityUnit) {
	t.Helper()
	if _, _, err := (&DeclareStationStandard{Repo: f.standards}).Handle(context.Background(), DeclareStationStandardCommand{
		Location: location, ProcessType: process, Quantity: qty, Unit: unit, Period: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}
}

func (f *compositionFixture) get(pathID string) (GetProcessPathCapacityResult, error) {
	return f.uc.Handle(context.Background(), GetProcessPathCapacityCommand{
		ProcessPathID: pathID, Location: "SIM1", WindowStart: f.start, WindowEnd: f.end,
		UnitsPerOrder: f64ptr(2.5), PackagesPerOrder: f64ptr(1),
	})
}

func stepRates(res GetProcessPathCapacityResult) []float64 {
	out := make([]float64, 0, len(res.Steps))
	for _, s := range res.Steps {
		out = append(out, s.Rate.Quantity())
	}
	return out
}

func assertRates(t *testing.T, res GetProcessPathCapacityResult, want ...float64) {
	t.Helper()
	got := stepRates(res)
	if len(got) != len(want) {
		t.Fatalf("step rates = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("step rates = %v, want %v", got, want)
		}
	}
}

// FIXTURE B through the real use case: Pick 8000 UNIT/h, Rebin 2500 UNIT/h,
// Pack LABOR 2500 PACKAGE/h with 10 PACK stations tallied in SIM1-OPS-WC and
// a declared standard of 180 PACKAGE/h per station.
func TestGetProcessPathCapacity_FixtureB_StationComposition(t *testing.T) {
	f := newCompositionFixture(t)
	f.labor(t, "PICK", 8000, processcapacity.UnitUnit)
	f.labor(t, "REBIN", 2500, processcapacity.UnitUnit)
	f.labor(t, "PACK", 2500, processcapacity.UnitPackage)
	f.path(t, "pick-rebin-pack", "PICK", "REBIN", "PACK")
	f.stations(t, "SIM1-OPS-WC", "PACK", 10)
	f.declare(t, "SIM1", "PACK", 180, processcapacity.UnitPackage)

	res, err := f.get("pick-rebin-pack")
	if err != nil {
		t.Fatal(err)
	}
	assertRates(t, res, 3200, 1000, 1800)
	if res.NormalizedRate.Quantity() != 1000 || res.BottleneckStep != "REBIN" || res.BottleneckConstraint != processcapacity.ConstraintLabor {
		t.Fatalf("got %v bottleneck %s/%s, want 1000 REBIN/LABOR", res.NormalizedRate.Quantity(), res.BottleneckStep, res.BottleneckConstraint)
	}
	if len(res.Warnings) != 0 {
		t.Fatalf("warnings = %v, want none", res.Warnings)
	}

	f.labor(t, "REBIN", 6000, processcapacity.UnitUnit)
	res, err = f.get("pick-rebin-pack")
	if err != nil {
		t.Fatal(err)
	}
	assertRates(t, res, 3200, 2400, 1800)
	if res.NormalizedRate.Quantity() != 1800 || res.NormalizedRate.Unit() != processcapacity.UnitOrder ||
		res.BottleneckStep != "PACK" || res.BottleneckConstraint != processcapacity.ConstraintStation {
		t.Fatalf("got %v %s bottleneck %s/%s, want 1800 ORDER PACK/STATION", res.NormalizedRate.Quantity(), res.NormalizedRate.Unit(), res.BottleneckStep, res.BottleneckConstraint)
	}
}

// The composition is derived at read time, so it converges: a late standard
// declaration, a decommissioned station and a new station are all picked up
// by the very next read, with no stale stored row anywhere.
func TestGetProcessPathCapacity_ConvergesWhenFactsChange(t *testing.T) {
	f := newCompositionFixture(t)
	f.labor(t, "PACK", 2500, processcapacity.UnitPackage)
	f.path(t, "pack", "PACK")
	f.stations(t, "SIM1-OPS-WC", "PACK", 10)

	res, err := f.get("pack") // FIXTURE C: no standard yet
	if err != nil {
		t.Fatal(err)
	}
	if res.NormalizedRate.Quantity() != 2500 || res.BottleneckConstraint != processcapacity.ConstraintLabor || len(res.Warnings) != 1 {
		t.Fatalf("before declaration: %v bound by %s warnings %v, want 2500 LABOR + one warning", res.NormalizedRate.Quantity(), res.BottleneckConstraint, res.Warnings)
	}
	want := "no station standard declared for PACK at SIM1: 10 stations are tallied but their throughput is unknown, so no STATION constraint was applied"
	if res.Warnings[0] != want {
		t.Fatalf("warning = %q, want %q", res.Warnings[0], want)
	}

	f.declare(t, "SIM1", "PACK", 180, processcapacity.UnitPackage) // late declaration
	if res, err = f.get("pack"); err != nil || res.NormalizedRate.Quantity() != 1800 || len(res.Warnings) != 0 {
		t.Fatalf("after declaration: %v %v warnings %v, want 1800 and none", res.NormalizedRate.Quantity(), err, res.Warnings)
	}

	f.stations(t, "SIM1-OPS-WC2", "PACK", 1) // a new station in another zone of the site
	if res, err = f.get("pack"); err != nil || res.NormalizedRate.Quantity() != 1980 {
		t.Fatalf("after +1 station: %v %v, want 1980", res.NormalizedRate.Quantity(), err)
	}

	if _, _, err := f.tally.DecommissionSlot(context.Background(), "SIM1-OPS-WC-PACK-0"); err != nil {
		t.Fatal(err)
	}
	if res, err = f.get("pack"); err != nil || res.NormalizedRate.Quantity() != 1800 {
		t.Fatalf("after decommission: %v %v, want 1800", res.NormalizedRate.Quantity(), err)
	}
}

// A zone belongs to a site only by its `<site>-` prefix. SIM10's and SIM2's
// zones and a zone with no matching site must not contribute to SIM1.
func TestGetProcessPathCapacity_OnlyZonesOfTheSiteContribute(t *testing.T) {
	f := newCompositionFixture(t)
	f.labor(t, "PACK", 2500, processcapacity.UnitPackage)
	f.path(t, "pack", "PACK")
	f.declare(t, "SIM1", "PACK", 180, processcapacity.UnitPackage)
	f.stations(t, "SIM1-OPS-WC", "PACK", 3)
	f.stations(t, "SIM10-OPS-WC", "PACK", 5) // shares the characters "SIM1" but is another site
	f.stations(t, "SIM2-OPS-WC", "PACK", 7)
	f.stations(t, "SIM1", "PACK", 11) // zone id equal to the site, not `SIM1-...`
	f.stations(t, "ORPHAN-ZONE", "PACK", 13)

	res, err := f.get("pack")
	if err != nil {
		t.Fatal(err)
	}
	if res.NormalizedRate.Quantity() != 540 { // 3 x 180
		t.Fatalf("got %v, want 540 (only SIM1's 3 stations)", res.NormalizedRate.Quantity())
	}
}

// A step with no registered constraint at all is still computable from the
// derived STATION constraint; with neither, the missing-capacity error stays.
func TestGetProcessPathCapacity_DerivedOnlyAndMissingStep(t *testing.T) {
	f := newCompositionFixture(t)
	f.path(t, "pack", "PACK")
	f.stations(t, "SIM1-OPS-WC", "PACK", 4)
	f.declare(t, "SIM1", "PACK", 180, processcapacity.UnitPackage)

	res, err := f.get("pack")
	if err != nil {
		t.Fatal(err)
	}
	if res.NormalizedRate.Quantity() != 720 || res.BottleneckConstraint != processcapacity.ConstraintStation || res.BottleneckStep != "PACK" {
		t.Fatalf("derived-only: %v %s/%s, want 720 PACK/STATION", res.NormalizedRate.Quantity(), res.BottleneckStep, res.BottleneckConstraint)
	}

	f.path(t, "sort", "SORT") // stations tallied but neither standard nor labor
	f.stations(t, "SIM1-OPS-WC", "SORT", 2)
	if _, err := f.get("sort"); !errors.Is(err, processcapacity.ErrMissingStepCapacity) {
		t.Fatalf("got %v, want ErrMissingStepCapacity", err)
	}
	f.path(t, "qc", "QC") // nothing at all
	if _, err := f.get("qc"); !errors.Is(err, processcapacity.ErrMissingStepCapacity) {
		t.Fatalf("got %v, want ErrMissingStepCapacity", err)
	}
}

// Tally keys are upper-case activities: a lower-case step name still finds
// its stations (and the standard declared under that same step name).
func TestGetProcessPathCapacity_ActivityLookupIsUpperCased(t *testing.T) {
	f := newCompositionFixture(t)
	f.path(t, "pack", "pack")
	f.stations(t, "SIM1-OPS-WC", "PACK", 2)
	f.declare(t, "SIM1", "pack", 180, processcapacity.UnitPackage)
	res, err := f.get("pack")
	if err != nil || res.NormalizedRate.Quantity() != 360 {
		t.Fatalf("got %v %v, want 360", res.NormalizedRate.Quantity(), err)
	}
}

type failingStandards struct {
	*memory.StationStandardRepo
	findCalls int
	err       error
}

func (f *failingStandards) Find(context.Context, string, processcapacity.ProcessType) (*processcapacity.StationStandard, error) {
	f.findCalls++
	return nil, f.err
}

type failingTally struct {
	*memory.StorageTallyRepo
	err error
}

func (f failingTally) StationCount(context.Context, string, string) (int, error) { return 0, f.err }

func (f failingTally) SiteBuckets(context.Context, string) ([]tally.Bucket, error) { return nil, f.err }

func TestGetProcessPathCapacity_InfrastructureErrorsAreReturned(t *testing.T) {
	f := newCompositionFixture(t)
	f.labor(t, "PACK", 2500, processcapacity.UnitPackage)
	f.path(t, "pack", "PACK")

	f.uc.Tally = failingTally{StorageTallyRepo: f.tally, err: errBoom}
	if _, err := f.get("pack"); !errors.Is(err, errBoom) {
		t.Fatalf("tally error: got %v, want boom", err)
	}

	f.uc.Tally = f.tally
	standards := &failingStandards{StationStandardRepo: f.standards, err: errBoom}
	f.uc.StationStandards = standards
	if _, err := f.get("pack"); err != nil || standards.findCalls != 0 {
		t.Fatalf("with no stations the standard must not even be looked up: err %v, %d Find calls", err, standards.findCalls)
	}
	f.stations(t, "SIM1-OPS-WC", "PACK", 1)
	if _, err := f.get("pack"); !errors.Is(err, errBoom) || standards.findCalls != 1 {
		t.Fatalf("standard error: got %v after %d Find calls, want boom after 1", err, standards.findCalls)
	}
}

// CapacityPlan creation uses the same composition: shortage and bottleneck can
// now be bound by STATION, and the warnings of a plan computed without a
// standard are stored with it.
func TestCreateCapacityPlan_UsesCompositionAndStoresItsOutcome(t *testing.T) {
	f := newCompositionFixture(t)
	f.start, f.end = planWindowStart, planWindowEnd
	f.labor(t, "PICK", 8000, processcapacity.UnitUnit)
	f.labor(t, "REBIN", 6000, processcapacity.UnitUnit)
	f.labor(t, "PACK", 2500, processcapacity.UnitPackage)
	f.path(t, "pick-rebin-pack", "PICK", "REBIN", "PACK")
	f.stations(t, "SIM1-OPS-WC", "PACK", 10)

	plans, ob := memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	create := &CreateCapacityPlan{
		PathCapacity: f.uc, Plans: plans, Outbox: ob, Encoder: fakeEncoder{}, UnitOfWork: memory.NewUnitOfWork(plans, ob),
		NewID: func() string { return "plan-c" }, Now: func() time.Time { return planCreatedAt },
	}
	cmd := CreateCapacityPlanCommand{
		WarehouseID: "WH-1", Location: "SIM1", WindowStart: planWindowStart, WindowEnd: planWindowEnd,
		ProcessPathID: "pick-rebin-pack", AssignedDemand: 20000, UnitsPerOrder: f64ptr(2.5), PackagesPerOrder: f64ptr(1),
	}

	// FIXTURE C: stations but no standard -> labor only (REBIN 2400, PACK 2500), warning stored.
	withoutStandard, err := create.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if withoutStandard.PathCapacity() != 2400 || withoutStandard.BottleneckStep() != "REBIN" || len(withoutStandard.Warnings()) != 1 {
		t.Fatalf("no standard: %v %s warnings %v", withoutStandard.PathCapacity(), withoutStandard.BottleneckStep(), withoutStandard.Warnings())
	}

	f.declare(t, "SIM1", "PACK", 180, processcapacity.UnitPackage)
	cmd2 := cmd
	create.NewID = func() string { return "plan-b" }
	plan, err := create.Handle(context.Background(), cmd2)
	if err != nil {
		t.Fatal(err)
	}
	// 1800 ORDER/h over 8h = 14400; shortage 20000 - 14400 = 5600, bound by PACK/STATION.
	if plan.PathCapacity() != 1800 || plan.CapacityOverWindow() != 14400 || plan.Shortage() != 5600 ||
		plan.BottleneckStep() != "PACK" || plan.BottleneckConstraint() != processcapacity.ConstraintStation || len(plan.Warnings()) != 0 {
		t.Fatalf("plan = %v over %v short %v bottleneck %s/%s warnings %v", plan.PathCapacity(), plan.CapacityOverWindow(), plan.Shortage(),
			plan.BottleneckStep(), plan.BottleneckConstraint(), plan.Warnings())
	}
	stored, err := plans.FindByID(context.Background(), "plan-b")
	if err != nil || stored == nil || stored.BottleneckConstraint() != processcapacity.ConstraintStation {
		t.Fatalf("stored plan = %v err %v, want STATION bottleneck constraint kept", stored, err)
	}
	storedC, _ := plans.FindByID(context.Background(), "plan-c")
	if storedC == nil || len(storedC.Warnings()) != 1 {
		t.Fatalf("stored fixture C plan lost its warning: %v", storedC)
	}
}

// --- DeclareStationStandard ---------------------------------------------------

func TestDeclareStationStandard_CreatesThenReplaces(t *testing.T) {
	repo := memory.NewStationStandardRepo()
	uc := &DeclareStationStandard{Repo: repo}
	ctx := context.Background()
	cmd := DeclareStationStandardCommand{Location: "SIM1", ProcessType: "PACK", Quantity: 180, Unit: processcapacity.UnitPackage, Period: time.Hour}

	std, created, err := uc.Handle(ctx, cmd)
	if err != nil || !created || std.PerStation().Quantity() != 180 {
		t.Fatalf("first declare: %v created=%v err=%v, want 180 created", std.PerStation().Quantity(), created, err)
	}
	cmd.Quantity = 150.5
	std, created, err = uc.Handle(ctx, cmd)
	if err != nil || created || std.PerStation().Quantity() != 150.5 {
		t.Fatalf("redeclare: %v created=%v err=%v, want 150.5 replaced", std.PerStation().Quantity(), created, err)
	}
	stored, _ := repo.Find(ctx, "SIM1", "PACK")
	if stored == nil || stored.PerStation().Quantity() != 150.5 {
		t.Fatalf("stored = %v, want the replaced 150.5", stored)
	}
}

func TestDeclareStationStandard_Rejections(t *testing.T) {
	good := DeclareStationStandardCommand{Location: "SIM1", ProcessType: "PACK", Quantity: 180, Unit: processcapacity.UnitPackage, Period: time.Hour}
	with := func(mut func(*DeclareStationStandardCommand)) DeclareStationStandardCommand {
		c := good
		mut(&c)
		return c
	}
	tests := []struct {
		name string
		cmd  DeclareStationStandardCommand
		want error
	}{
		{"negative quantity", with(func(c *DeclareStationStandardCommand) { c.Quantity = -1 }), processcapacity.ErrNegativeQuantity},
		{"zero quantity", with(func(c *DeclareStationStandardCommand) { c.Quantity = 0 }), processcapacity.ErrNonPositiveStationStandard},
		{"zero period", with(func(c *DeclareStationStandardCommand) { c.Period = 0 }), processcapacity.ErrNonPositivePeriod},
		{"LINE unit", with(func(c *DeclareStationStandardCommand) { c.Unit = processcapacity.UnitLine }), processcapacity.ErrUnsupportedNormalizationUnit},
		{"blank location", with(func(c *DeclareStationStandardCommand) { c.Location = "" }), processcapacity.ErrStationStandardRequiredField},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := memory.NewStationStandardRepo()
			if _, _, err := (&DeclareStationStandard{Repo: repo}).Handle(context.Background(), tc.cmd); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if all, _ := repo.List(context.Background(), ""); len(all) != 0 {
				t.Fatalf("a rejected declaration was stored: %v", all)
			}
		})
	}
}

type brokenStandards struct {
	*memory.StationStandardRepo
	findErr, saveErr error
}

func (b brokenStandards) Find(ctx context.Context, l string, p processcapacity.ProcessType) (*processcapacity.StationStandard, error) {
	if b.findErr != nil {
		return nil, b.findErr
	}
	return b.StationStandardRepo.Find(ctx, l, p)
}

func (b brokenStandards) Save(ctx context.Context, s processcapacity.StationStandard) error {
	if b.saveErr != nil {
		return b.saveErr
	}
	return b.StationStandardRepo.Save(ctx, s)
}

func TestDeclareStationStandard_RepositoryErrorsAreReturned(t *testing.T) {
	cmd := DeclareStationStandardCommand{Location: "SIM1", ProcessType: "PACK", Quantity: 180, Unit: processcapacity.UnitPackage, Period: time.Hour}
	for name, repo := range map[string]brokenStandards{
		"find": {StationStandardRepo: memory.NewStationStandardRepo(), findErr: errBoom},
		"save": {StationStandardRepo: memory.NewStationStandardRepo(), saveErr: errBoom},
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := (&DeclareStationStandard{Repo: repo}).Handle(context.Background(), cmd); !errors.Is(err, errBoom) {
				t.Fatalf("got %v, want boom", err)
			}
		})
	}
}

// --- GetStorageCapacity -------------------------------------------------------

func TestGetStorageCapacity_SplitsPositionsAndStationsForTheSite(t *testing.T) {
	f := newCompositionFixture(t)
	ctx := context.Background()
	f.stations(t, "SIM1-OPS-WC", "PACK", 11)
	f.stations(t, "SIM1-OPS-WC", "SORT", 2)
	f.stations(t, "SIM2-OPS-WC", "PACK", 5) // another site
	for i := 0; i < 24; i++ {
		if _, err := f.tally.RegisterSlot(ctx, "SIM1-STOR-AMB-"+strconv.Itoa(i), "SIM1-STOR-AMB", tally.TypeLocation, []string{"SimShelf"}); err != nil {
			t.Fatal(err)
		}
	}

	got, err := (&GetStorageCapacity{Tally: f.tally}).Handle(ctx, "SIM1")
	if err != nil {
		t.Fatal(err)
	}
	wantPositions := []StoragePositions{{ZoneID: "SIM1-STOR-AMB", LocationType: "SimShelf", Positions: 24}}
	wantStations := []ZoneStations{{ZoneID: "SIM1-OPS-WC", Activity: "PACK", Stations: 11}, {ZoneID: "SIM1-OPS-WC", Activity: "SORT", Stations: 2}}
	if got.Location != "SIM1" || len(got.StoragePositions) != 1 || got.StoragePositions[0] != wantPositions[0] ||
		len(got.Stations) != 2 || got.Stations[0] != wantStations[0] || got.Stations[1] != wantStations[1] {
		t.Fatalf("storage capacity = %+v, want positions %v stations %v", got, wantPositions, wantStations)
	}
}

func TestGetStorageCapacity_EmptySiteAndErrors(t *testing.T) {
	f := newCompositionFixture(t)
	got, err := (&GetStorageCapacity{Tally: f.tally}).Handle(context.Background(), "NOWHERE")
	if err != nil || got.StoragePositions == nil || got.Stations == nil || len(got.StoragePositions) != 0 || len(got.Stations) != 0 {
		t.Fatalf("empty site = %+v err %v, want empty non-nil lists", got, err)
	}
	if _, err := (&GetStorageCapacity{Tally: failingTally{StorageTallyRepo: f.tally, err: errBoom}}).Handle(context.Background(), "SIM1"); !errors.Is(err, errBoom) {
		t.Fatalf("got %v, want boom", err)
	}
}
