package usecases

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

// recordStandard stores one labour standard in the local copy.
func recordStandard(t *testing.T, repo *memory.LaborStandardRepo, id, taskType, site string, seconds int64, effectiveFrom time.Time) {
	t.Helper()
	s, err := laborstandard.New(laborstandard.Params{
		StandardID: id, TaskType: taskType, SiteCode: site, ExpectedSeconds: seconds,
		EffectiveFrom: effectiveFrom, AsOf: effectiveFrom,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Record(context.Background(), s); err != nil {
		t.Fatal(err)
	}
}

// newLabourPlanFixture is the pick-rebin-pack fixture with the labour
// standards copy wired and seeded: PICK 12 s scoped to SIM1 (the plan site),
// PACK 90 s for every site, and three rows that must NOT apply (a PICK of
// another site, a PACK that comes into force after the window start, a
// handling-unit PACK).
func newLabourPlanFixture(t *testing.T) (*planFixture, *memory.LaborStandardRepo) {
	t.Helper()
	f := newPlanFixture(t)
	repo := memory.NewLaborStandardRepo()
	before := planWindowStart.Add(-24 * time.Hour)
	recordStandard(t, repo, "std-pick-sim1", "PICK", "SIM1", 12, before)
	recordStandard(t, repo, "std-pick-sim2", "PICK", "SIM2", 1, before.Add(time.Hour))
	recordStandard(t, repo, "std-pack", "PACK", "", 90, before)
	recordStandard(t, repo, "std-pack-later", "PACK", "", 1, planWindowStart.Add(time.Second))
	caseStd, err := laborstandard.New(laborstandard.Params{
		StandardID: "std-pack-case", TaskType: "PACK", HandlingUnit: laborstandard.HandlingUnitCase,
		ExpectedSeconds: 1, EffectiveFrom: before.Add(time.Hour), AsOf: before.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Record(context.Background(), caseStd); err != nil {
		t.Fatal(err)
	}
	f.create.LaborStandards = repo
	return f, repo
}

func TestCreateCapacityPlan_WithoutAStandardsCopyEveryStepIsUnknown(t *testing.T) {
	f := newPlanFixture(t)
	cmd := planCommand(12000)
	cmd.TasksPerOrder = map[string]float64{"PACK": 1}
	plan, err := f.create.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	want := []capacityplan.LabourRequirement{
		{ProcessType: "PICK", Reason: capacityplan.ReasonNoLaborStandard},
		{ProcessType: "REBIN", Reason: capacityplan.ReasonNoLaborStandard},
		{ProcessType: "PACK", Reason: capacityplan.ReasonNoLaborStandard},
	}
	if got := plan.LabourRequirement(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LabourRequirement = %+v\nwant %+v", got, want)
	}
}

func TestCreateCapacityPlan_PricesEachStepWithTheStandardInForce(t *testing.T) {
	f, _ := newLabourPlanFixture(t)
	cmd := planCommand(12000)
	cmd.TasksPerOrder = map[string]float64{"PICK": 3, "PACK": 1, "REBIN": 1}
	plan, err := f.create.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	// PICK: 12000 x 3 x 12 s = 120 h over 8 h = 15 heads (SIM1's own row).
	// REBIN: labor-performance has no REBIN standard, factor or not.
	// PACK: 12000 x 1 x 90 s = 300 h = 37.5 heads (the fleet-wide row; the
	// later and the handling-unit rows do not apply).
	want := []capacityplan.LabourRequirement{
		{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Tasks: 36000, RequiredLabourHours: 120, RequiredAverageHeads: 15, Known: true},
		{ProcessType: "REBIN", Reason: capacityplan.ReasonNoLaborStandard},
		{ProcessType: "PACK", StandardSeconds: 90, Tasks: 12000, RequiredLabourHours: 300, RequiredAverageHeads: 37.5, Known: true},
	}
	if got := plan.LabourRequirement(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LabourRequirement = %+v\nwant %+v", got, want)
	}
	stored, err := f.plans.FindByID(context.Background(), plan.ID())
	if err != nil || stored == nil {
		t.Fatalf("FindByID: %v %v", stored, err)
	}
	if got := stored.LabourRequirement(); !reflect.DeepEqual(got, want) {
		t.Fatalf("the stored plan lost its requirement: %+v", got)
	}
}

func TestCreateCapacityPlan_AStepWithoutAFactorIsUnknownNotGuessed(t *testing.T) {
	f, _ := newLabourPlanFixture(t)
	plan, err := f.create.Handle(context.Background(), planCommand(12000))
	if err != nil {
		t.Fatal(err)
	}
	want := []capacityplan.LabourRequirement{
		{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Reason: capacityplan.ReasonNoTaskFactor},
		{ProcessType: "REBIN", Reason: capacityplan.ReasonNoLaborStandard},
		{ProcessType: "PACK", StandardSeconds: 90, Reason: capacityplan.ReasonNoTaskFactor},
	}
	if got := plan.LabourRequirement(); !reflect.DeepEqual(got, want) {
		t.Fatalf("LabourRequirement = %+v\nwant %+v", got, want)
	}
}

func TestCreateCapacityPlan_PickTasksAreTheReleasedLinesWhenTheDemandCameFromOrders(t *testing.T) {
	f, _ := newLabourPlanFixture(t)
	demandRepo := memory.NewOrderDemandRepo()
	f.create.Demand = &GetExpectedDemand{Demand: demandRepo}
	for i, lines := range []int{3, 5, 2} {
		o := demandOrder(t, "ord-"+string(rune('a'+i)), "PATH-ZONE-A", planWindowStart.Add(time.Hour), lines, demandAsOf)
		if _, err := demandRepo.Upsert(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}
	// Outside the window: neither an order nor lines of this plan.
	if _, err := demandRepo.Upsert(context.Background(), demandOrder(t, "late", "PATH-ZONE-A", planWindowEnd, 40, demandAsOf)); err != nil {
		t.Fatal(err)
	}
	cmd := omittedDemandCommand()
	// A PICK factor is ignored when the released lines are known; PACK uses
	// its factor over the 3 orders.
	cmd.TasksPerOrder = map[string]float64{"PICK": 100, "PACK": 2}
	plan, err := f.create.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if plan.DemandSource() != capacityplan.DemandSourceOrders || plan.AssignedDemand() != 3 {
		t.Fatalf("demand %v from %s, want 3 orders", plan.AssignedDemand(), plan.DemandSource())
	}
	got := plan.LabourRequirement()
	if len(got) != 3 || got[0].Tasks != 10 || got[0].RequiredLabourHours != 10*12.0/3600 || !got[0].Known {
		t.Fatalf("PICK = %+v, want the 3+5+2 released lines x 12 s", got)
	}
	if got[2].Tasks != 6 || got[2].RequiredLabourHours != 6*90.0/3600 || !got[2].Known {
		t.Fatalf("PACK = %+v, want 3 orders x 2 tasks x 90 s", got[2])
	}
}

func TestCreateCapacityPlan_TheRequirementChangesNoShortageBottleneckOrEvent(t *testing.T) {
	legacy := newPlanFixture(t)
	legacyPlan, err := legacy.create.Handle(context.Background(), planCommand(12000))
	if err != nil {
		t.Fatal(err)
	}
	priced, _ := newLabourPlanFixture(t)
	cmd := planCommand(12000)
	cmd.TasksPerOrder = map[string]float64{"PICK": 3, "PACK": 1}
	pricedPlan, err := priced.create.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if pricedPlan.Shortage() != legacyPlan.Shortage() || pricedPlan.BottleneckStep() != legacyPlan.BottleneckStep() ||
		!reflect.DeepEqual(pricedPlan.Warnings(), legacyPlan.Warnings()) {
		t.Fatal("the labour requirement changed the shortage, the bottleneck or the warnings")
	}
	if !reflect.DeepEqual(priced.outbox.Messages(), legacy.outbox.Messages()) {
		t.Fatalf("the labour requirement changed the outboxed events:\n%+v\n%+v", priced.outbox.Messages(), legacy.outbox.Messages())
	}
}

func TestCreateCapacityPlan_ALaterRevisionNeverRewritesAStoredPlan(t *testing.T) {
	f, repo := newLabourPlanFixture(t)
	cmd := planCommand(12000)
	cmd.TasksPerOrder = map[string]float64{"PACK": 1}
	plan, err := f.create.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	// A revision that would now be in force for the same window.
	recordStandard(t, repo, "std-pack-v2", "PACK", "", 30, planWindowStart.Add(-time.Hour))
	stored, err := f.plans.FindByID(context.Background(), plan.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got := stored.LabourRequirement()[2]; got.StandardSeconds != 90 || got.RequiredLabourHours != 300 {
		t.Fatalf("the stored PACK requirement was rewritten: %+v", got)
	}
}

func TestCreateCapacityPlan_RejectsANonPositiveTaskFactorAndStoresNothing(t *testing.T) {
	for name, factors := range map[string]map[string]float64{
		"zero":     {"PACK": 0},
		"negative": {"PICK": 1, "PACK": -2},
	} {
		t.Run(name, func(t *testing.T) {
			f, _ := newLabourPlanFixture(t)
			cmd := planCommand(12000)
			cmd.TasksPerOrder = factors
			if _, err := f.create.Handle(context.Background(), cmd); !errors.Is(err, capacityplan.ErrNonPositiveTasksPerOrder) {
				t.Fatalf("err = %v, want ErrNonPositiveTasksPerOrder", err)
			}
			if len(f.outbox.Messages()) != 0 {
				t.Fatal("a rejected request outboxed events")
			}
			if stored, _ := f.plans.FindByID(context.Background(), "plan-1"); stored != nil {
				t.Fatal("a rejected request stored a plan")
			}
		})
	}
}

// failingLaborStandards (labor_standard_test.go) fails every read of the copy.

func TestCreateCapacityPlan_AFailingStandardsReadFailsTheRequest(t *testing.T) {
	f := newPlanFixture(t)
	boom := errors.New("postgres down")
	f.create.LaborStandards = failingLaborStandards{err: boom}
	if _, err := f.create.Handle(context.Background(), planCommand(12000)); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the read failure", err)
	}
	if len(f.outbox.Messages()) != 0 {
		t.Fatal("a failed request outboxed events")
	}
}
