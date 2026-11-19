package capacityplan

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// labourWindow is the plan window of the labour tests: 08:00-16:00, 8 hours.
func labourWindow(t *testing.T) processcapacity.CapacityWindow {
	t.Helper()
	return mustWindow(t, windowStart, windowEnd)
}

func TestRequireLabour(t *testing.T) {
	pack90 := StepStandard{Found: true, Seconds: 90}
	pick12Site := StepStandard{Found: true, Seconds: 12, SiteScoped: true}
	fromRequest := LabourDemand{AssignedDemand: 1000, Source: DemandSourceRequest, TasksPerOrder: map[processcapacity.ProcessType]float64{"PACK": 1, "PICK": 3}}
	fromOrders := LabourDemand{AssignedDemand: 1000, Source: DemandSourceOrders, ReleasedLines: 2400, TasksPerOrder: map[processcapacity.ProcessType]float64{"PICK": 3}}
	cases := []struct {
		name     string
		step     processcapacity.ProcessType
		standard StepStandard
		demand   LabourDemand
		want     LabourRequirement
	}{
		{
			name: "no standard is unknown, every figure zero", step: "REBIN", standard: StepStandard{}, demand: fromRequest,
			want: LabourRequirement{ProcessType: "REBIN", Reason: ReasonNoLaborStandard},
		},
		{
			name: "no standard wins over a missing factor", step: "SORT", standard: StepStandard{}, demand: LabourDemand{AssignedDemand: 5},
			want: LabourRequirement{ProcessType: "SORT", Reason: ReasonNoLaborStandard},
		},
		{
			// 1000 orders x 1 task x 90 s = 25 h over 8 h = 3.125 heads.
			name: "a factor prices the step", step: "PACK", standard: pack90, demand: fromRequest,
			want: LabourRequirement{ProcessType: "PACK", StandardSeconds: 90, Tasks: 1000, RequiredLabourHours: 25, RequiredAverageHeads: 3.125, Known: true},
		},
		{
			// PICK from the request uses the factor: 1000 x 3 x 12 s = 10 h.
			name: "PICK with demand from the request uses its factor", step: "PICK", standard: pick12Site, demand: fromRequest,
			want: LabourRequirement{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Tasks: 3000, RequiredLabourHours: 10, RequiredAverageHeads: 1.25, Known: true},
		},
		{
			// PICK from orders uses the released lines, NOT 1000 x 3:
			// 2400 x 12 s = 8 h over 8 h = 1 head.
			name: "PICK with demand from orders uses the released lines", step: "PICK", standard: pick12Site, demand: fromOrders,
			want: LabourRequirement{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Tasks: 2400, RequiredLabourHours: 8, RequiredAverageHeads: 1, Known: true},
		},
		{
			name: "PICK from orders needs no factor", step: "PICK", standard: pick12Site, demand: LabourDemand{AssignedDemand: 3, Source: DemandSourceOrders, ReleasedLines: 0},
			want: LabourRequirement{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Known: true},
		},
		{
			name: "a non-PICK step with demand from orders still needs its factor", step: "PACK", standard: pack90, demand: fromOrders,
			want: LabourRequirement{ProcessType: "PACK", StandardSeconds: 90, Reason: ReasonNoTaskFactor},
		},
		{
			name: "PICK from the request without a factor is unknown", step: "PICK", standard: pick12Site, demand: LabourDemand{AssignedDemand: 10, Source: DemandSourceRequest},
			want: LabourRequirement{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Reason: ReasonNoTaskFactor},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := RequireLabour(c.step, c.standard, c.demand, labourWindow(t))
			if got != c.want {
				t.Fatalf("RequireLabour = %+v\nwant %+v", got, c.want)
			}
		})
	}
}

func TestRequireLabour_ZeroWindowGivesZeroHeads(t *testing.T) {
	got := RequireLabour("PACK", StepStandard{Found: true, Seconds: 3600}, LabourDemand{AssignedDemand: 2, TasksPerOrder: map[processcapacity.ProcessType]float64{"PACK": 1}}, processcapacity.CapacityWindow{})
	if !got.Known || got.RequiredLabourHours != 2 || got.RequiredAverageHeads != 0 {
		t.Fatalf("RequireLabour over a zero window = %+v, want 2 h and 0 heads (never Inf/NaN)", got)
	}
}

func TestRequireLabour_HeadsScaleWithTheWindow(t *testing.T) {
	// 36 tasks x 100 s = 1 h of labour over a 2 h window = 0.5 heads.
	w := mustWindow(t, windowStart, windowStart.Add(2*time.Hour))
	got := RequireLabour("SLAM", StepStandard{Found: true, Seconds: 100}, LabourDemand{AssignedDemand: 36, TasksPerOrder: map[processcapacity.ProcessType]float64{"SLAM": 1}}, w)
	if got.RequiredLabourHours != 1 || got.RequiredAverageHeads != 0.5 {
		t.Fatalf("RequireLabour = %+v, want 1 h and 0.5 heads", got)
	}
}

func TestValidateTasksPerOrder(t *testing.T) {
	for name, factors := range map[string]map[processcapacity.ProcessType]float64{
		"nil":            nil,
		"empty":          {},
		"positive":       {"PACK": 1, "PICK": 0.25},
		"smallest float": {"PACK": math.SmallestNonzeroFloat64},
	} {
		if err := ValidateTasksPerOrder(factors); err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
	for name, factors := range map[string]map[processcapacity.ProcessType]float64{
		"zero":         {"PACK": 0},
		"negative":     {"PACK": -1},
		"one bad":      {"PACK": 1, "PICK": -0.5},
		"positive inf": {"PACK": math.Inf(1)},
		"negative inf": {"PACK": math.Inf(-1)},
		"NaN":          {"PACK": math.NaN()},
	} {
		if err := ValidateTasksPerOrder(factors); !errors.Is(err, ErrNonPositiveTasksPerOrder) {
			t.Errorf("%s: err = %v, want ErrNonPositiveTasksPerOrder", name, err)
		}
	}
}

func labourItems() []LabourRequirement {
	return []LabourRequirement{
		{ProcessType: "PICK", StandardSeconds: 12, Tasks: 2400, RequiredLabourHours: 8, RequiredAverageHeads: 1, Known: true},
		{ProcessType: "REBIN", Reason: ReasonNoLaborStandard},
		{ProcessType: "PACK", StandardSeconds: 90, StandardSiteScoped: true, Reason: ReasonNoTaskFactor},
	}
}

func TestCreate_KeepsTheLabourRequirementAndLeavesEverythingElseAlone(t *testing.T) {
	rate, bottleneck := phase2PathCapacity(t)
	params := CreateParams{
		ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location,
		Window: labourWindow(t), ProcessPathID: pathID, AssignedDemand: 12000,
		PathRate: rate, BottleneckStep: bottleneck,
	}
	legacy, err := Create(params, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if got := legacy.LabourRequirement(); len(got) != 0 {
		t.Fatalf("a plan created without a requirement has %+v", got)
	}

	items := labourItems()
	params.LabourRequirement = items
	plan, err := Create(params, createdAt)
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.LabourRequirement(); !reflect.DeepEqual(got, labourItems()) {
		t.Fatalf("LabourRequirement = %+v", got)
	}
	// The caller's slice is copied in, the accessor copies out.
	items[0].Tasks = -1
	plan.LabourRequirement()[1].Known = true
	if got := plan.LabourRequirement(); !reflect.DeepEqual(got, labourItems()) {
		t.Fatalf("the requirement was aliased: %+v", got)
	}
	// The order shortage, the bottleneck and every recorded event are the
	// same with and without a requirement (docs/adr/0022 decision 5).
	if plan.Shortage() != legacy.Shortage() || plan.BottleneckStep() != legacy.BottleneckStep() || plan.CapacityOverWindow() != legacy.CapacityOverWindow() {
		t.Fatalf("the requirement changed the shortage or the bottleneck")
	}
	if !reflect.DeepEqual(plan.PullEvents(), legacy.PullEvents()) {
		t.Fatal("the requirement changed a recorded event")
	}
	if err := plan.Publish(publishedAt); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Publish(publishedAt); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(plan.PullEvents(), legacy.PullEvents()) {
		t.Fatal("the requirement changed a published event")
	}
	if got := plan.LabourRequirement(); !reflect.DeepEqual(got, labourItems()) {
		t.Fatalf("LabourRequirement after publish = %+v", got)
	}
}

func TestRehydrate_LabourRequirement(t *testing.T) {
	base := RehydrateParams{
		ID: planID, WarehouseID: warehouseID, Location: location,
		Window:    labourWindow(t),
		CreatedAt: createdAt, Status: StatusDraft,
	}
	if got := Rehydrate(base).LabourRequirement(); len(got) != 0 {
		t.Fatalf("a plan stored before migration 0013 reads back as %+v", got)
	}
	items := labourItems()
	base.LabourRequirement = items
	plan := Rehydrate(base)
	items[2].Reason = ""
	if got := plan.LabourRequirement(); !reflect.DeepEqual(got, labourItems()) {
		t.Fatalf("LabourRequirement = %+v", got)
	}
}
