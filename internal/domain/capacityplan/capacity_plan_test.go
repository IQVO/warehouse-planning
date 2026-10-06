package capacityplan

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

const (
	planID      = "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10"
	warehouseID = "WH-1"
	siteID      = "SIM1"
	location    = "PATH-ZONE-A"
	pathID      = "pick-rebin-pack"
)

var (
	windowStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	windowEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	createdAt   = time.Date(2026, 10, 4, 17, 15, 30, 0, time.UTC)
	publishedAt = time.Date(2026, 10, 4, 18, 45, 10, 0, time.UTC)
)

func mustWindow(t *testing.T, start, end time.Time) processcapacity.CapacityWindow {
	t.Helper()
	w, err := processcapacity.NewCapacityWindow(start, end)
	if err != nil {
		t.Fatalf("window: %v", err)
	}
	return w
}

func mustRate(t *testing.T, q float64, unit processcapacity.CapacityUnit, period time.Duration) processcapacity.CapacityRate {
	t.Helper()
	r, err := processcapacity.NewCapacityRate(q, unit, period)
	if err != nil {
		t.Fatalf("rate: %v", err)
	}
	return r
}

// phase2PathCapacity builds the Phase 2 worked-example fixture through the
// REAL domain service (not hard-coded numbers): PICK 4000 UNIT/h, REBIN
// 2500 UNIT/h, PACK 1800 PACKAGE/h with units_per_order 2.5 and
// packages_per_order 1 -> 1000 ORDER/h, bottleneck REBIN.
func phase2PathCapacity(t *testing.T) (processcapacity.CapacityRate, processcapacity.ProcessType) {
	t.Helper()
	w := mustWindow(t, windowStart, windowEnd)
	mk := func(pt processcapacity.ProcessType, q float64, u processcapacity.CapacityUnit) *processcapacity.ProcessCapacity {
		pc := processcapacity.NewProcessCapacity(pt, location, w)
		if err := pc.AddConstraint(processcapacity.ConstraintLabor, mustRate(t, q, u, time.Hour)); err != nil {
			t.Fatalf("constraint: %v", err)
		}
		return pc
	}
	caps := map[processcapacity.ProcessType]*processcapacity.ProcessCapacity{
		"PICK":  mk("PICK", 4000, processcapacity.UnitUnit),
		"REBIN": mk("REBIN", 2500, processcapacity.UnitUnit),
		"PACK":  mk("PACK", 1800, processcapacity.UnitPackage),
	}
	path, err := processpath.NewProcessPath(pathID, "Pick-Rebin-Pack", []processpath.ProcessType{"PICK", "REBIN", "PACK"})
	if err != nil {
		t.Fatalf("path: %v", err)
	}
	upo, ppo := 2.5, 1.0
	profile, err := processcapacity.NewWorkloadProfile(&upo, &ppo)
	if err != nil {
		t.Fatalf("profile: %v", err)
	}
	rate, bottleneck, err := processcapacity.ComputeProcessPathCapacity(path, caps, profile)
	if err != nil {
		t.Fatalf("compute: %v", err)
	}
	return rate, bottleneck
}

func createWorkedExample(t *testing.T, demand float64) *CapacityPlan {
	t.Helper()
	rate, bottleneck := phase2PathCapacity(t)
	plan, err := Create(CreateParams{
		ID:             planID,
		WarehouseID:    warehouseID,
		SiteID:         siteID,
		Location:       location,
		Window:         mustWindow(t, windowStart, windowEnd),
		ProcessPathID:  pathID,
		AssignedDemand: demand,
		PathRate:       rate,
		BottleneckStep: bottleneck,
	}, createdAt)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	return plan
}

// TestWorkedExample_ShortageDetected is the design doc's section-43
// regression fixture, byte for byte: demand 12,000 orders over 08:00-16:00
// (8h) on Pick->Rebin->Pack (1000 ORDER/h, bottleneck REBIN) ->
// capacityOverWindow 8000, shortage 4000, events Created, Published,
// ShortageDetected(4000, REBIN), BottleneckDetected(REBIN). Do not change
// these numbers without updating the design doc.
func TestWorkedExample_ShortageDetected(t *testing.T) {
	plan := createWorkedExample(t, 12000)

	if plan.PathCapacity() != 1000 {
		t.Errorf("PathCapacity = %v, want 1000", plan.PathCapacity())
	}
	if plan.BottleneckStep() != "REBIN" {
		t.Errorf("BottleneckStep = %q, want REBIN", plan.BottleneckStep())
	}
	if plan.CapacityOverWindow() != 8000 {
		t.Errorf("CapacityOverWindow = %v, want 8000", plan.CapacityOverWindow())
	}
	if plan.Shortage() != 4000 {
		t.Errorf("Shortage = %v, want 4000", plan.Shortage())
	}
	if plan.Status() != StatusDraft {
		t.Errorf("Status = %q, want DRAFT", plan.Status())
	}

	created := plan.PullEvents()
	wantCreated := []Event{CapacityPlanCreated{
		Header:             Header{PlanID: planID, At: createdAt},
		WarehouseID:        warehouseID,
		Location:           location,
		PathID:             pathID,
		WindowStart:        windowStart,
		WindowEnd:          windowEnd,
		AssignedDemand:     12000,
		PathCapacity:       1000,
		CapacityOverWindow: 8000,
		Shortage:           4000,
		BottleneckStep:     "REBIN",
	}}
	if !reflect.DeepEqual(created, wantCreated) {
		t.Fatalf("Create events =\n%#v\nwant\n%#v", created, wantCreated)
	}

	if err := plan.Publish(publishedAt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if plan.Status() != StatusPublished {
		t.Errorf("Status after publish = %q, want PUBLISHED", plan.Status())
	}
	if !plan.PublishedAt().Equal(publishedAt) {
		t.Errorf("PublishedAt = %v, want %v", plan.PublishedAt(), publishedAt)
	}

	header := Header{PlanID: planID, At: publishedAt}
	want := []Event{
		CapacityPlanPublished{
			Header: header, WarehouseID: warehouseID, SiteID: siteID, Location: location, PathID: pathID,
			WindowStart: windowStart, WindowEnd: windowEnd,
			AssignedDemand: 12000, PathCapacity: 1000, CapacityOverWindow: 8000, Shortage: 4000,
			BottleneckStep: "REBIN",
		},
		CapacityShortageDetected{
			Header: header, WarehouseID: warehouseID, Location: location, PathID: pathID,
			WindowStart: windowStart, WindowEnd: windowEnd,
			AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000,
			BottleneckStep: "REBIN",
		},
		BottleneckDetected{
			Header: header, WarehouseID: warehouseID, Location: location, PathID: pathID,
			WindowStart: windowStart, WindowEnd: windowEnd,
			BottleneckStep: "REBIN", PathCapacity: 1000,
		},
	}
	got := plan.PullEvents()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Publish events =\n%#v\nwant\n%#v", got, want)
	}
	assertEventNames(t, got, EventCapacityPlanPublished, EventCapacityShortageDetected, EventBottleneckDetected)
}

func TestWithinCapacity_NoShortageNoBottleneckEvents(t *testing.T) {
	plan := createWorkedExample(t, 6000)
	if plan.Shortage() != 0 {
		t.Fatalf("Shortage = %v, want 0", plan.Shortage())
	}
	if plan.CapacityOverWindow() != 8000 {
		t.Fatalf("CapacityOverWindow = %v, want 8000", plan.CapacityOverWindow())
	}
	assertEventNames(t, plan.PullEvents(), EventCapacityPlanCreated)

	if err := plan.Publish(publishedAt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	assertEventNames(t, plan.PullEvents(), EventCapacityPlanPublished)
}

// TestShortageBoundary pins the exact boundary: demand == capacity is NOT
// a shortage; one order more is.
func TestShortageBoundary(t *testing.T) {
	cases := []struct {
		demand       float64
		wantShortage float64
		wantEvents   []string
	}{
		{7999, 0, []string{EventCapacityPlanPublished}},
		{8000, 0, []string{EventCapacityPlanPublished}},
		{8001, 1, []string{EventCapacityPlanPublished, EventCapacityShortageDetected, EventBottleneckDetected}},
	}
	for _, tc := range cases {
		plan := createWorkedExample(t, tc.demand)
		if plan.Shortage() != tc.wantShortage {
			t.Errorf("demand %v: Shortage = %v, want %v", tc.demand, plan.Shortage(), tc.wantShortage)
		}
		plan.PullEvents()
		if err := plan.Publish(publishedAt); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		assertEventNames(t, plan.PullEvents(), tc.wantEvents...)
	}
}

// TestNonHourlyPeriodAndWindow uses a rate over a non-1h period and a
// non-integer-hour window so that dividing vs multiplying by the period or
// the window hours cannot give the same answer.
func TestNonHourlyPeriodAndWindow(t *testing.T) {
	// 600 ORDER / 30min = 1200 ORDER/h; window 07:00-12:30 = 5.5h -> 6600.
	plan, err := Create(CreateParams{
		ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location,
		Window:         mustWindow(t, time.Date(2026, 10, 5, 7, 0, 0, 0, time.UTC), time.Date(2026, 10, 5, 12, 30, 0, 0, time.UTC)),
		ProcessPathID:  pathID,
		AssignedDemand: 7000,
		PathRate:       mustRate(t, 600, processcapacity.UnitOrder, 30*time.Minute),
		BottleneckStep: "PACK",
	}, createdAt)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if plan.PathCapacity() != 1200 {
		t.Errorf("PathCapacity = %v, want 1200", plan.PathCapacity())
	}
	if plan.CapacityOverWindow() != 6600 {
		t.Errorf("CapacityOverWindow = %v, want 6600", plan.CapacityOverWindow())
	}
	if plan.Shortage() != 400 {
		t.Errorf("Shortage = %v, want 400", plan.Shortage())
	}
}

func TestCreateValidation(t *testing.T) {
	rate := mustRate(t, 1000, processcapacity.UnitOrder, time.Hour)
	base := func() CreateParams {
		return CreateParams{
			ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location,
			Window: mustWindow(t, windowStart, windowEnd), ProcessPathID: pathID,
			AssignedDemand: 100, PathRate: rate, BottleneckStep: "REBIN",
		}
	}

	t.Run("negative demand", func(t *testing.T) {
		p := base()
		p.AssignedDemand = -0.5
		if _, err := Create(p, createdAt); !errors.Is(err, ErrNegativeDemand) {
			t.Fatalf("err = %v, want ErrNegativeDemand", err)
		}
	})

	t.Run("zero demand is valid and never a shortage", func(t *testing.T) {
		p := base()
		p.AssignedDemand = 0
		plan, err := Create(p, createdAt)
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		if plan.Shortage() != 0 {
			t.Fatalf("Shortage = %v, want 0", plan.Shortage())
		}
	})

	t.Run("blank required fields", func(t *testing.T) {
		for name, mutate := range map[string]func(*CreateParams){
			"id":        func(p *CreateParams) { p.ID = "" },
			"warehouse": func(p *CreateParams) { p.WarehouseID = "" },
			"location":  func(p *CreateParams) { p.Location = "" },
			"path":      func(p *CreateParams) { p.ProcessPathID = "" },
		} {
			p := base()
			mutate(&p)
			if _, err := Create(p, createdAt); !errors.Is(err, ErrRequiredField) {
				t.Errorf("%s blank: err = %v, want ErrRequiredField", name, err)
			}
		}
	})

	t.Run("non-ORDER path rate", func(t *testing.T) {
		p := base()
		p.PathRate = mustRate(t, 1000, processcapacity.UnitUnit, time.Hour)
		if _, err := Create(p, createdAt); !errors.Is(err, ErrPathRateNotOrder) {
			t.Fatalf("err = %v, want ErrPathRateNotOrder", err)
		}
	})
}

func TestPublishTwiceIsRejected(t *testing.T) {
	plan := createWorkedExample(t, 12000)
	plan.PullEvents()
	if err := plan.Publish(publishedAt); err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	first := plan.PullEvents()
	if len(first) != 3 {
		t.Fatalf("first publish recorded %d events, want 3", len(first))
	}

	later := publishedAt.Add(time.Hour)
	if err := plan.Publish(later); !errors.Is(err, ErrAlreadyPublished) {
		t.Fatalf("second Publish err = %v, want ErrAlreadyPublished", err)
	}
	if got := plan.PullEvents(); len(got) != 0 {
		t.Errorf("failed second publish recorded %d events, want 0", len(got))
	}
	if !plan.PublishedAt().Equal(publishedAt) {
		t.Errorf("PublishedAt moved to %v on a rejected publish, want %v", plan.PublishedAt(), publishedAt)
	}
	if plan.Status() != StatusPublished {
		t.Errorf("Status = %q, want PUBLISHED", plan.Status())
	}
}

func TestPullEventsHandsOverEachEventOnce(t *testing.T) {
	plan := createWorkedExample(t, 12000)
	if got := plan.PullEvents(); len(got) != 1 {
		t.Fatalf("first pull = %d events, want 1", len(got))
	}
	if got := plan.PullEvents(); len(got) != 0 {
		t.Fatalf("second pull = %d events, want 0", len(got))
	}
}

func TestRehydrateRecordsNoEventsAndKeepsState(t *testing.T) {
	w := mustWindow(t, windowStart, windowEnd)
	plan := Rehydrate(RehydrateParams{
		ID: planID, WarehouseID: warehouseID, Location: location, Window: w, ProcessPathID: pathID,
		AssignedDemand: 12000, PathCapacity: 1000, BottleneckStep: "REBIN",
		CapacityOverWindow: 8000, Shortage: 4000, Status: StatusPublished,
		CreatedAt: createdAt, PublishedAt: publishedAt,
	})
	if got := plan.PullEvents(); len(got) != 0 {
		t.Fatalf("Rehydrate recorded %d events, want 0", len(got))
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"ID", plan.ID(), planID},
		{"WarehouseID", plan.WarehouseID(), warehouseID},
		{"Location", plan.Location(), location},
		{"Window", plan.Window(), w},
		{"ProcessPathID", plan.ProcessPathID(), pathID},
		{"AssignedDemand", plan.AssignedDemand(), 12000.0},
		{"PathCapacity", plan.PathCapacity(), 1000.0},
		{"BottleneckStep", plan.BottleneckStep(), processcapacity.ProcessType("REBIN")},
		{"CapacityOverWindow", plan.CapacityOverWindow(), 8000.0},
		{"Shortage", plan.Shortage(), 4000.0},
		{"Status", plan.Status(), StatusPublished},
		{"CreatedAt", plan.CreatedAt(), createdAt},
		{"PublishedAt", plan.PublishedAt(), publishedAt},
	}
	for _, c := range checks {
		if !reflect.DeepEqual(c.got, c.want) {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if err := plan.Publish(publishedAt.Add(time.Minute)); !errors.Is(err, ErrAlreadyPublished) {
		t.Fatalf("Publish on a rehydrated PUBLISHED plan err = %v, want ErrAlreadyPublished", err)
	}
}

func TestEventAccessors(t *testing.T) {
	h := Header{PlanID: planID, At: createdAt}
	events := []struct {
		e    Event
		name string
	}{
		{CapacityPlanCreated{Header: h}, "CapacityPlanCreated"},
		{CapacityPlanPublished{Header: h}, "CapacityPlanPublished"},
		{CapacityShortageDetected{Header: h}, "CapacityShortageDetected"},
		{BottleneckDetected{Header: h}, "BottleneckDetected"},
	}
	for _, c := range events {
		if c.e.EventName() != c.name {
			t.Errorf("EventName = %q, want %q", c.e.EventName(), c.name)
		}
		if c.e.AggregateID() != planID {
			t.Errorf("%s AggregateID = %q, want %q", c.name, c.e.AggregateID(), planID)
		}
		if !c.e.OccurredAt().Equal(createdAt) {
			t.Errorf("%s OccurredAt = %v, want %v", c.name, c.e.OccurredAt(), createdAt)
		}
	}
}

func assertEventNames(t *testing.T, events []Event, want ...string) {
	t.Helper()
	got := make([]string, 0, len(events))
	for _, e := range events {
		got = append(got, e.EventName())
	}
	if !reflect.DeepEqual(got, append([]string{}, want...)) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

// The composition outcome (binding constraint + warnings) is recorded on the
// plan, survives Rehydrate, and is defensively copied both ways -- and none of
// it leaks into the published events.
func TestCreate_RecordsBottleneckConstraintAndWarnings(t *testing.T) {
	rate := mustRate(t, 1800, processcapacity.UnitOrder, time.Hour)
	warnings := []string{"no station standard declared for PACK at SIM1"}
	plan, err := Create(CreateParams{
		ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: "SIM1", Window: mustWindow(t, windowStart, windowEnd),
		ProcessPathID: pathID, AssignedDemand: 20000, PathRate: rate, BottleneckStep: "PACK",
		BottleneckConstraint: processcapacity.ConstraintStation, Warnings: warnings,
	}, createdAt)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if plan.BottleneckConstraint() != processcapacity.ConstraintStation {
		t.Fatalf("BottleneckConstraint = %s, want STATION", plan.BottleneckConstraint())
	}
	if got := plan.Warnings(); !reflect.DeepEqual(got, warnings) {
		t.Fatalf("Warnings = %q, want %q", got, warnings)
	}
	warnings[0] = "mutated by the caller"
	plan.Warnings()[0] = "mutated through the accessor"
	if got := plan.Warnings(); got[0] != "no station standard declared for PACK at SIM1" {
		t.Fatalf("Warnings is not defensively copied: %q", got)
	}

	// 1800/h over 8h = 14400 orders; shortage 5600; the event stays the old shape.
	if plan.Shortage() != 5600 {
		t.Fatalf("shortage = %v, want 5600", plan.Shortage())
	}
	events := plan.PullEvents()
	created, ok := events[0].(CapacityPlanCreated)
	if len(events) != 1 || !ok || created.BottleneckStep != "PACK" {
		t.Fatalf("events = %#v, want exactly CapacityPlanCreated for PACK", events)
	}

	again := Rehydrate(RehydrateParams{
		ID: planID, WarehouseID: warehouseID, Location: "SIM1", Window: mustWindow(t, windowStart, windowEnd),
		ProcessPathID: pathID, BottleneckStep: "PACK", Status: StatusDraft, CreatedAt: createdAt,
		BottleneckConstraint: processcapacity.ConstraintStation, Warnings: []string{"w1", "w2"},
	})
	if again.BottleneckConstraint() != processcapacity.ConstraintStation || !reflect.DeepEqual(again.Warnings(), []string{"w1", "w2"}) {
		t.Fatalf("Rehydrate lost the composition outcome: %s %q", again.BottleneckConstraint(), again.Warnings())
	}
}

// TestPublish_CarriesTheBottleneckConstraintOnThePublishedEventOnly func is above.

// The canonical site id is an explicit planning fact (it names the
// facility-layout Site by site_code), required at Create, carried on the
// plan and on CapacityPlanPublished's payload source, and never inferred
// from warehouse_id or location. A plan Rehydrated without one (a row
// stored before migration 0008) publishes an empty site id and stays
// decodable: the v1 payload is additive.
func TestCreate_RequiresAndRecordsSiteID(t *testing.T) {
	rate := mustRate(t, 1000, processcapacity.UnitOrder, time.Hour)
	base := func() CreateParams {
		return CreateParams{
			ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location,
			Window: mustWindow(t, windowStart, windowEnd), ProcessPathID: pathID,
			AssignedDemand: 100, PathRate: rate, BottleneckStep: "REBIN",
		}
	}

	t.Run("blank site id is rejected", func(t *testing.T) {
		p := base()
		p.SiteID = ""
		if _, err := Create(p, createdAt); !errors.Is(err, ErrRequiredField) {
			t.Fatalf("err = %v, want ErrRequiredField", err)
		}
	})

	t.Run("recorded on the plan and on the published event", func(t *testing.T) {
		plan, err := Create(base(), createdAt)
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if plan.SiteID() != siteID {
			t.Fatalf("SiteID = %q, want %q", plan.SiteID(), siteID)
		}
		plan.PullEvents()
		if err := plan.Publish(publishedAt); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		published, ok := plan.PullEvents()[0].(CapacityPlanPublished)
		if !ok || published.SiteID != siteID {
			t.Fatalf("published event = %#v, want SiteID %q on CapacityPlanPublished", published, siteID)
		}
	})

	t.Run("a rehydrated plan without a site id publishes an empty one", func(t *testing.T) {
		legacy := Rehydrate(RehydrateParams{
			ID: planID, WarehouseID: warehouseID, Location: location, Window: mustWindow(t, windowStart, windowEnd),
			ProcessPathID: pathID, AssignedDemand: 100, PathCapacity: 1000, BottleneckStep: "REBIN",
			CapacityOverWindow: 800, Shortage: 0, Status: StatusDraft, CreatedAt: createdAt,
		})
		if legacy.SiteID() != "" {
			t.Fatalf("legacy SiteID = %q, want empty", legacy.SiteID())
		}
		if err := legacy.Publish(publishedAt); err != nil {
			t.Fatalf("Publish legacy: %v", err)
		}
		if published := legacy.PullEvents()[0].(CapacityPlanPublished); published.SiteID != "" {
			t.Fatalf("legacy published SiteID = %q, want empty", published.SiteID)
		}
	})

	t.Run("rehydrate round-trips the site id", func(t *testing.T) {
		plan := Rehydrate(RehydrateParams{
			ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location, Window: mustWindow(t, windowStart, windowEnd),
			ProcessPathID: pathID, Status: StatusDraft, CreatedAt: createdAt,
		})
		if plan.SiteID() != siteID {
			t.Fatalf("Rehydrate SiteID = %q, want %q", plan.SiteID(), siteID)
		}
	})
}

// The bottleneck's binding constraint travels on CapacityPlanPublished (and
// only there: it feeds the analytics bottleneck-frequency report, ADR 0005).
// A plan rebuilt from storage publishes the constraint it was stored with, a
// plan created before the constraint was recorded publishes an empty one, and
// the shortage/bottleneck events never carry it.
func TestPublish_CarriesTheBottleneckConstraintOnThePublishedEventOnly(t *testing.T) {
	plan := Rehydrate(RehydrateParams{
		ID: planID, WarehouseID: warehouseID, Location: "SIM1", Window: mustWindow(t, windowStart, windowEnd),
		ProcessPathID: pathID, AssignedDemand: 20000, PathCapacity: 1800, BottleneckStep: "PACK",
		CapacityOverWindow: 14400, Shortage: 5600, Status: StatusDraft, CreatedAt: createdAt,
		BottleneckConstraint: processcapacity.ConstraintStation,
	})
	if err := plan.Publish(publishedAt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	events := plan.PullEvents()
	published, ok := events[0].(CapacityPlanPublished)
	if !ok || published.BottleneckConstraint != processcapacity.ConstraintStation {
		t.Fatalf("published event = %#v, want BottleneckConstraint STATION", events[0])
	}
	if len(events) != 3 || events[1].(CapacityShortageDetected).BottleneckStep != "PACK" {
		t.Fatalf("events = %#v", events)
	}

	legacy := Rehydrate(RehydrateParams{
		ID: planID, WarehouseID: warehouseID, Location: "SIM1", Window: mustWindow(t, windowStart, windowEnd),
		ProcessPathID: pathID, BottleneckStep: "PACK", Status: StatusDraft, CreatedAt: createdAt,
	})
	if err := legacy.Publish(publishedAt); err != nil {
		t.Fatalf("Publish legacy: %v", err)
	}
	if got := legacy.PullEvents()[0].(CapacityPlanPublished).BottleneckConstraint; got != "" {
		t.Fatalf("legacy plan published constraint %q, want empty", got)
	}
}
