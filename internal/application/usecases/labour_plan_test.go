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
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

var (
	shiftStart    = time.Date(2026, 11, 19, 6, 0, 0, 0, time.UTC)
	planCommitAt  = time.Date(2026, 11, 19, 5, 0, 0, 0, time.UTC)
	errLabourBoom = errors.New("boom")
)

// conflictingPlans answers ErrVersionConflict to the first conflicts Saves.
type conflictingPlans struct {
	*memory.LabourPlanRepo
	conflicts int
	saves     int
}

func (c *conflictingPlans) Save(ctx context.Context, p *labourplan.Plan) error {
	c.saves++
	if c.saves <= c.conflicts {
		return ports.ErrVersionConflict
	}
	return c.LabourPlanRepo.Save(ctx, p)
}

// failingPlanStore fails every Get and List.
type failingPlanStore struct{ *memory.LabourPlanRepo }

func (failingPlanStore) Get(context.Context, string, string, string) (*labourplan.Plan, bool, error) {
	return nil, false, errLabourBoom
}

func (failingPlanStore) List(context.Context, string, string, string, int) ([]*labourplan.Plan, bool, error) {
	return nil, false, errLabourBoom
}

type labourFixture struct {
	plans     *memory.LabourPlanRepo
	forecasts *memory.WorkloadForecastRepo
	tally     *memory.StorageTallyRepo
	outbox    *memory.OutboxRepo
	commit    *CommitLabourPlan
}

func newLabourFixture() *labourFixture {
	plans, forecasts, ob, tallies := memory.NewLabourPlanRepo(), memory.NewWorkloadForecastRepo(), memory.NewOutboxRepo(), memory.NewStorageTallyRepo()
	f := &labourFixture{plans: plans, forecasts: forecasts, tally: tallies, outbox: ob}
	f.commit = &CommitLabourPlan{
		Plans: plans, Forecasts: forecasts, Outbox: ob, Encoder: fakeEncoder{},
		UnitOfWork: memory.NewUnitOfWork(plans, forecasts, ob), Stations: tallies,
		Now: func() time.Time { return planCommitAt },
	}
	return f
}

// seedForecast stores the SIM1/PICK forecast: 300 units due at 06:00 and 500
// at 13:59 (inside an 8 h shift from 06:00), 999 at 14:00 (the shift end,
// excluded).
func (f *labourFixture) seedForecast(t *testing.T) {
	t.Helper()
	fc, _, err := workloadforecast.New("SIM1", "PICK", []workloadforecast.Bucket{
		{CPT: shiftStart, Quantity: 300},
		{CPT: shiftStart.Add(7*time.Hour + 59*time.Minute), Quantity: 500},
		{CPT: shiftStart.Add(8 * time.Hour), Quantity: 999},
	}, planCommitAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.forecasts.Save(context.Background(), fc); err != nil {
		t.Fatal(err)
	}
}

// seedStations tallies n PICK stations at SIM1.
func (f *labourFixture) seedStations(t *testing.T, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := f.tally.RegisterSlot(context.Background(), "SIM1-PK-"+strconv.Itoa(i), "SIM1-PK", tally.TypeStation, []string{"PICK"}); err != nil {
			t.Fatal(err)
		}
	}
}

func heads(n int) *int { return &n }

func planCmd(plannedHeads *int) CommitLabourPlanCommand {
	start := shiftStart
	return CommitLabourPlanCommand{
		SiteID: "SIM1", ShiftID: "S1", PathID: "PICK", PlannedHeads: plannedHeads,
		RateUnitsPerHour: 10, Hours: 8, InstalledStations: 12, ShiftStart: &start,
	}
}

func TestCommitLabourPlan_SentHeadsAreStoredAsRequestHeads(t *testing.T) {
	f := newLabourFixture()
	f.seedStations(t, 12)
	res, err := f.commit.Handle(context.Background(), planCmd(heads(7)))
	if err != nil {
		t.Fatal(err)
	}
	spec := res.Plan.Spec()
	if !res.Created || !res.Changed || res.Plan.Version() != 1 || spec.PlannedHeads != 7 || spec.HeadsSource != labourplan.HeadsFromRequest || len(res.Warnings) != 0 {
		t.Fatalf("result = %+v spec %+v", res, spec)
	}
	msgs := f.outbox.Messages()
	if len(msgs) != 1 || msgs[0].EventType != labourplan.EventLabourPlanCommitted || msgs[0].Subject != "SIM1/S1/PICK" {
		t.Fatalf("outbox = %+v", msgs)
	}
	if _, found, _ := f.plans.Get(context.Background(), "SIM1", "S1", "PICK"); !found {
		t.Fatal("the plan was not stored")
	}
}

func TestCommitLabourPlan_OmittedHeadsAreDerivedFromTheForecastInTheShift(t *testing.T) {
	f := newLabourFixture()
	f.seedForecast(t)
	res, err := f.commit.Handle(context.Background(), planCmd(nil))
	if err != nil {
		t.Fatal(err)
	}
	// ceil(800 units / (10 units/h x 8 h)) = 10; the 999 at the shift end is out.
	if spec := res.Plan.Spec(); spec.PlannedHeads != 10 || spec.HeadsSource != labourplan.HeadsFromForecast {
		t.Fatalf("spec = %+v, want 10 heads from the forecast", spec)
	}
	cmd := planCmd(nil)
	cmd.RateUnitsPerHour = 7
	res, err = f.commit.Handle(context.Background(), cmd)
	// ceil(800 / 56) = 15 > 12 installed stations.
	if !errors.Is(err, labourplan.ErrHeadsExceedStations) {
		t.Fatalf("err = %v (%+v), want derived heads above the stations refused", err, res)
	}
}

func TestCommitLabourPlan_HeadsThatCannotBeDerivedAreRefused(t *testing.T) {
	cases := map[string]func(*labourFixture, *CommitLabourPlanCommand){
		"no shift start":   func(_ *labourFixture, c *CommitLabourPlanCommand) { c.ShiftStart = nil },
		"no forecast":      func(*labourFixture, *CommitLabourPlanCommand) {},
		"no forecast port": func(f *labourFixture, _ *CommitLabourPlanCommand) { f.commit.Forecasts = nil },
		"no bucket inshift": func(_ *labourFixture, c *CommitLabourPlanCommand) {
			later := shiftStart.Add(24 * time.Hour)
			c.ShiftStart = &later
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newLabourFixture()
			if name != "no forecast" {
				f.seedForecast(t)
			}
			cmd := planCmd(nil)
			mutate(f, &cmd)
			if _, err := f.commit.Handle(context.Background(), cmd); !errors.Is(err, ErrMissingPlannedHeads) {
				t.Fatalf("err = %v, want ErrMissingPlannedHeads", err)
			}
			if len(f.outbox.Messages()) != 0 {
				t.Fatal("a refused plan queued an event")
			}
		})
	}
}

func TestCommitLabourPlan_InvalidHoursOrRateIsReportedBeforeTheForecast(t *testing.T) {
	f := newLabourFixture()
	cmd := planCmd(nil)
	cmd.Hours = 0
	if _, err := f.commit.Handle(context.Background(), cmd); !errors.Is(err, labourplan.ErrInvalidHours) {
		t.Fatalf("err = %v, want ErrInvalidHours", err)
	}
	cmd = planCmd(nil)
	cmd.RateUnitsPerHour = -1
	if _, err := f.commit.Handle(context.Background(), cmd); !errors.Is(err, labourplan.ErrInvalidRate) {
		t.Fatalf("err = %v, want ErrInvalidRate", err)
	}
}

func TestCommitLabourPlan_SentHeadsAboveTheStationsAreRefused(t *testing.T) {
	f := newLabourFixture()
	if _, err := f.commit.Handle(context.Background(), planCmd(heads(13))); !errors.Is(err, labourplan.ErrHeadsExceedStations) {
		t.Fatalf("err = %v, want ErrHeadsExceedStations", err)
	}
	if _, found, _ := f.plans.Get(context.Background(), "SIM1", "S1", "PICK"); found {
		t.Fatal("a refused plan was stored")
	}
}

func TestCommitLabourPlan_AnIdenticalCommitKeepsTheVersionAndQueuesNothing(t *testing.T) {
	f := newLabourFixture()
	if _, err := f.commit.Handle(context.Background(), planCmd(heads(7))); err != nil {
		t.Fatal(err)
	}
	res, err := f.commit.Handle(context.Background(), planCmd(heads(7)))
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || res.Changed || res.Plan.Version() != 1 || len(f.outbox.Messages()) != 1 {
		t.Fatalf("result = %+v, outbox %d; want an unchanged v1", res, len(f.outbox.Messages()))
	}
}

func TestCommitLabourPlan_ARevisionBumpsTheVersion(t *testing.T) {
	f := newLabourFixture()
	if _, err := f.commit.Handle(context.Background(), planCmd(heads(7))); err != nil {
		t.Fatal(err)
	}
	res, err := f.commit.Handle(context.Background(), planCmd(heads(9)))
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || !res.Changed || res.Plan.Version() != 2 || res.Plan.Spec().PlannedHeads != 9 || len(f.outbox.Messages()) != 2 {
		t.Fatalf("result = %+v, outbox %d; want a revised v2", res, len(f.outbox.Messages()))
	}
}

func TestCommitLabourPlan_VersionConflicts(t *testing.T) {
	f := newLabourFixture()
	racing := &conflictingPlans{LabourPlanRepo: f.plans, conflicts: 1}
	f.commit.Plans = racing
	if res, err := f.commit.Handle(context.Background(), planCmd(heads(7))); err != nil || racing.saves != 2 || res.Plan.Version() != 1 {
		t.Fatalf("one conflict: err %v saves %d", err, racing.saves)
	}
	f = newLabourFixture()
	racing = &conflictingPlans{LabourPlanRepo: f.plans, conflicts: 2}
	f.commit.Plans = racing
	if _, err := f.commit.Handle(context.Background(), planCmd(heads(7))); !errors.Is(err, ports.ErrVersionConflict) || racing.saves != 2 {
		t.Fatalf("two conflicts: err %v saves %d, want ErrVersionConflict after 2 attempts", err, racing.saves)
	}
	if len(f.outbox.Messages()) != 0 {
		t.Fatal("a conflicting commit queued an event")
	}
}

func TestCommitLabourPlan_StationWarning(t *testing.T) {
	f := newLabourFixture()
	f.seedStations(t, 3)
	cmd := planCmd(heads(2))
	cmd.InstalledStations = 5
	res, err := f.commit.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Warnings) != 1 || !strings.Contains(res.Warnings[0], "installed_stations is 5") || !strings.Contains(res.Warnings[0], "knows 3 PICK station(s) at SIM1") {
		t.Fatalf("warnings = %q", res.Warnings)
	}
	if res.Plan.Spec().InstalledStations != 5 {
		t.Fatal("installed_stations was replaced")
	}
	cmd.InstalledStations = 3
	if res, err = f.commit.Handle(context.Background(), cmd); err != nil || len(res.Warnings) != 0 {
		t.Fatalf("matching tally: %v %q, want no warning", err, res.Warnings)
	}
	f.commit.Stations = nil
	cmd.InstalledStations = 50
	if res, err = f.commit.Handle(context.Background(), cmd); err != nil || len(res.Warnings) != 0 {
		t.Fatalf("no tally wired: %v %q, want no warning", err, res.Warnings)
	}
}

func TestCommitLabourPlan_FailuresStoreNothing(t *testing.T) {
	cases := map[string]func(*labourFixture){
		"tally": func(f *labourFixture) {
			f.commit.Stations = failingTally{StorageTallyRepo: f.tally, err: errLabourBoom}
		},
		"encoder":  func(f *labourFixture) { f.commit.Encoder = fakeEncoder{err: errLabourBoom} },
		"outbox":   func(f *labourFixture) { f.commit.Outbox = failingOutbox{err: errLabourBoom} },
		"plan get": func(f *labourFixture) { f.commit.Plans = failingPlanStore{f.plans} },
		"forecast": func(f *labourFixture) { f.commit.Forecasts = failingForecastGet{err: errLabourBoom} },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newLabourFixture()
			mutate(f)
			cmd := planCmd(heads(7))
			if name == "forecast" {
				cmd.PlannedHeads = nil
			}
			if _, err := f.commit.Handle(context.Background(), cmd); !errors.Is(err, errLabourBoom) {
				t.Fatalf("err = %v, want boom", err)
			}
			if _, found, _ := f.plans.Get(context.Background(), "SIM1", "S1", "PICK"); found {
				t.Fatal("the plan was stored")
			}
		})
	}
}

func TestCommitLabourPlan_DefaultsTheClockToUTCNow(t *testing.T) {
	f := newLabourFixture()
	f.commit.Now = nil
	if _, err := f.commit.Handle(context.Background(), planCmd(heads(1))); err != nil {
		t.Fatal(err)
	}
}

func TestGetLabourPlan(t *testing.T) {
	f := newLabourFixture()
	get := &GetLabourPlan{Plans: f.plans, Stations: f.tally}
	if _, err := get.Handle(context.Background(), "SIM1", "S1", "PICK"); !errors.Is(err, ErrLabourPlanNotFound) {
		t.Fatalf("err = %v, want ErrLabourPlanNotFound", err)
	}
	if _, err := f.commit.Handle(context.Background(), planCmd(heads(7))); err != nil {
		t.Fatal(err)
	}
	view, err := get.Handle(context.Background(), "SIM1", "S1", "PICK")
	if err != nil || view.Plan.Spec().PlannedHeads != 7 || len(view.Warnings) != 1 {
		t.Fatalf("view = %+v, %v; want the plan with the empty-tally warning", view, err)
	}
	get.Stations = failingTally{StorageTallyRepo: f.tally, err: errLabourBoom}
	if _, err := get.Handle(context.Background(), "SIM1", "S1", "PICK"); !errors.Is(err, errLabourBoom) {
		t.Fatalf("tally failure: %v", err)
	}
	get.Plans = failingPlanStore{f.plans}
	if _, err := get.Handle(context.Background(), "SIM1", "S1", "PICK"); !errors.Is(err, errLabourBoom) {
		t.Fatalf("store failure: %v", err)
	}
}

func TestListLabourPlans(t *testing.T) {
	f := newLabourFixture()
	f.seedStations(t, 12)
	for _, c := range []struct{ shift, path string }{{"S1", "PICK"}, {"S1", "PACK"}, {"S1", "SORT"}, {"S2", "PICK"}} {
		cmd := planCmd(heads(1))
		cmd.ShiftID, cmd.PathID = c.shift, c.path
		if _, err := f.commit.Handle(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}
	}
	list := &ListLabourPlans{Plans: f.plans, Stations: f.tally}
	page, err := list.Handle(context.Background(), "SIM1", "S1", "", 2)
	if err != nil || len(page) != 2 || page[0].Plan.Spec().PathID != "PACK" || page[1].Plan.Spec().PathID != "PICK" {
		t.Fatalf("first page = %+v, %v", page, err)
	}
	// Only PICK is tallied: PACK carries the warning, PICK does not.
	if len(page[0].Warnings) != 1 || len(page[1].Warnings) != 0 {
		t.Fatalf("warnings = %q / %q", page[0].Warnings, page[1].Warnings)
	}
	rest, err := list.Handle(context.Background(), "SIM1", "S1", "PICK", 2)
	if err != nil || len(rest) != 1 || rest[0].Plan.Spec().PathID != "SORT" {
		t.Fatalf("second page = %+v, %v", rest, err)
	}
	if empty, err := list.Handle(context.Background(), "SIM2", "S1", "", 2); err != nil || empty == nil || len(empty) != 0 {
		t.Fatalf("unknown site = %+v, %v; want an empty, non-nil page", empty, err)
	}
}

func TestListLabourPlans_FailuresAreReturned(t *testing.T) {
	f := newLabourFixture()
	if _, err := f.commit.Handle(context.Background(), planCmd(heads(1))); err != nil {
		t.Fatal(err)
	}
	list := &ListLabourPlans{Plans: f.plans, Stations: failingTally{StorageTallyRepo: f.tally, err: errLabourBoom}}
	if _, err := list.Handle(context.Background(), "SIM1", "S1", "", 2); !errors.Is(err, errLabourBoom) {
		t.Fatalf("tally failure: %v", err)
	}
	list.Plans = failingPlanStore{f.plans}
	if _, err := list.Handle(context.Background(), "SIM1", "S1", "", 2); !errors.Is(err, errLabourBoom) {
		t.Fatalf("store failure: %v", err)
	}
}
