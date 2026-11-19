package usecases

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// docs/adr/0024 decision 3: publishing a plan supersedes the plan published
// before it under the same (site, location, path, window), in the same unit.

// supersedeFixture is a planFixture whose creates mint plan-1, plan-2, ...
// and whose publishes happen one minute apart.
func supersedeFixture(t *testing.T) *planFixture {
	t.Helper()
	f := newPlanFixture(t)
	n := 0
	f.create.NewID = func() string { n++; return fmt.Sprintf("plan-%d", n) }
	at := planPublishedAt
	f.publish.Now = func() time.Time { at = at.Add(time.Minute); return at }
	return f
}

func (f *planFixture) mustPublish(t *testing.T, id string) {
	t.Helper()
	if _, err := f.publish.Handle(context.Background(), id); err != nil {
		t.Fatalf("publish %s: %v", id, err)
	}
}

func (f *planFixture) stored(t *testing.T, id string) *capacityplan.CapacityPlan {
	t.Helper()
	plan, err := f.plans.FindByID(context.Background(), id)
	if err != nil || plan == nil {
		t.Fatalf("find %s = %v, %v", id, plan, err)
	}
	return plan
}

// subjects lists the outbox subjects from index from on.
func (f *planFixture) subjects(from int) []string {
	var out []string
	for _, m := range f.outbox.Messages()[from:] {
		out = append(out, m.Subject)
	}
	return out
}

func TestPublishCapacityPlan_SupersedesThePlanPublishedBeforeIt(t *testing.T) {
	f := supersedeFixture(t)
	createPlan(t, f, 12000)
	f.mustPublish(t, "plan-1")
	createPlan(t, f, 12000)
	before := len(f.outbox.Messages())

	plan, err := f.publish.Handle(context.Background(), "plan-2")
	if err != nil || plan.Status() != capacityplan.StatusPublished || plan.SupersededBy() != "" {
		t.Fatalf("publish plan-2 = %v, %v; want it PUBLISHED", plan, err)
	}
	assertTypes(t, f.eventTypes()[before:], "CapacityPlanSuperseded", "CapacityPlanPublished", "CapacityShortageDetected", "BottleneckDetected")
	if got := fmt.Sprint(f.subjects(before)); got != "[plan-1 plan-2 plan-2 plan-2]" {
		t.Errorf("subjects = %s: Superseded is the OLDER plan's, the rest the newer one's", got)
	}
	older := f.stored(t, "plan-1")
	if older.Status() != capacityplan.StatusSuperseded || older.SupersededBy() != "plan-2" {
		t.Errorf("plan-1 = %s by %q, want SUPERSEDED by plan-2", older.Status(), older.SupersededBy())
	}
	if older.PublishedAt().IsZero() || older.Shortage() != 4000 {
		t.Errorf("plan-1 lost its stored facts: published_at %v, shortage %v", older.PublishedAt(), older.Shortage())
	}
	if newer := f.stored(t, "plan-2"); newer.Status() != capacityplan.StatusPublished {
		t.Errorf("plan-2 stored as %s, want PUBLISHED", newer.Status())
	}
}

func TestPublishCapacityPlan_EachPublishSupersedesOnlyTheCurrentPlan(t *testing.T) {
	f := supersedeFixture(t)
	for i := 1; i <= 3; i++ {
		createPlan(t, f, 6000)
		f.mustPublish(t, fmt.Sprintf("plan-%d", i))
	}
	assertTypes(t, f.eventTypes(),
		"CapacityPlanCreated", "CapacityPlanPublished",
		"CapacityPlanCreated", "CapacityPlanSuperseded", "CapacityPlanPublished",
		"CapacityPlanCreated", "CapacityPlanSuperseded", "CapacityPlanPublished")
	if got := fmt.Sprint(f.subjects(0)); got != "[plan-1 plan-1 plan-2 plan-1 plan-2 plan-3 plan-2 plan-3]" {
		t.Errorf("subjects = %s: plan-1 must never be superseded twice", got)
	}
	for id, by := range map[string]string{"plan-1": "plan-2", "plan-2": "plan-3"} {
		if p := f.stored(t, id); p.Status() != capacityplan.StatusSuperseded || p.SupersededBy() != by {
			t.Errorf("%s = %s by %q, want SUPERSEDED by %s", id, p.Status(), p.SupersededBy(), by)
		}
	}
}

func TestPublishCapacityPlan_ASupersededPlanIsNeverPublishedAgain(t *testing.T) {
	f := supersedeFixture(t)
	createPlan(t, f, 6000)
	f.mustPublish(t, "plan-1")
	createPlan(t, f, 6000)
	f.mustPublish(t, "plan-2")
	before := len(f.outbox.Messages())

	plan, err := f.publish.Handle(context.Background(), "plan-1")
	if !errors.Is(err, capacityplan.ErrSuperseded) || plan != nil {
		t.Fatalf("republish plan-1 = %v, %v; want nil, ErrSuperseded", plan, err)
	}
	if after := len(f.outbox.Messages()); after != before {
		t.Errorf("outbox grew from %d to %d on a refused publish", before, after)
	}
	if p := f.stored(t, "plan-2"); p.Status() != capacityplan.StatusPublished {
		t.Errorf("plan-2 = %s, want still PUBLISHED", p.Status())
	}
}

// A PUBLISHED plan differing from the one published in ANY part of the key
// is another plan: it is not superseded.
func TestPublishCapacityPlan_ADifferentKeySupersedesNothing(t *testing.T) {
	cases := map[string]func(*capacityplan.RehydrateParams){
		"site":     func(p *capacityplan.RehydrateParams) { p.SiteID = "SIM2" },
		"location": func(p *capacityplan.RehydrateParams) { p.Location = "PATH-ZONE-B" },
		"path":     func(p *capacityplan.RehydrateParams) { p.ProcessPathID = "pick-only" },
		"window start": func(p *capacityplan.RehydrateParams) {
			p.Window = keyWindow(t, planWindowStart.Add(time.Hour), planWindowEnd)
		},
		"window end": func(p *capacityplan.RehydrateParams) {
			p.Window = keyWindow(t, planWindowStart, planWindowEnd.Add(time.Hour))
		},
	}
	for name, vary := range cases {
		t.Run(name, func(t *testing.T) {
			f := supersedeFixture(t)
			other := publishedParams("other")
			vary(&other)
			if err := f.plans.Save(context.Background(), capacityplan.Rehydrate(other)); err != nil {
				t.Fatal(err)
			}
			createPlan(t, f, 6000)
			f.mustPublish(t, "plan-1")
			assertTypes(t, f.eventTypes(), "CapacityPlanCreated", "CapacityPlanPublished")
			if p := f.stored(t, "other"); p.Status() != capacityplan.StatusPublished || p.SupersededBy() != "" {
				t.Errorf("other = %s by %q, want untouched", p.Status(), p.SupersededBy())
			}
		})
	}
}

// The same key with the window given in another zone is the same key.
func TestPublishCapacityPlan_TheWindowComparesAsInstants(t *testing.T) {
	f := supersedeFixture(t)
	other := publishedParams("other")
	zone := time.FixedZone("BRT", -3*3600)
	other.Window = keyWindow(t, planWindowStart.In(zone), planWindowEnd.In(zone))
	if err := f.plans.Save(context.Background(), capacityplan.Rehydrate(other)); err != nil {
		t.Fatal(err)
	}
	createPlan(t, f, 6000)
	f.mustPublish(t, "plan-1")
	if p := f.stored(t, "other"); p.Status() != capacityplan.StatusSuperseded || p.SupersededBy() != "plan-1" {
		t.Errorf("other = %s by %q, want SUPERSEDED by plan-1", p.Status(), p.SupersededBy())
	}
}

// conflictingSaves answers ports.ErrVersionConflict to the first conflicts
// saves of a PUBLISHED plan: a concurrent publish of the same key won.
type conflictingSaves struct {
	ports.CapacityPlanRepository
	conflicts int
	saves     *int
}

func (c conflictingSaves) Save(ctx context.Context, p *capacityplan.CapacityPlan) error {
	if p.Status() == capacityplan.StatusPublished {
		*c.saves++
		if *c.saves <= c.conflicts {
			return ports.ErrVersionConflict
		}
	}
	return c.CapacityPlanRepository.Save(ctx, p)
}

func TestPublishCapacityPlan_ALostRaceRerunsTheUnitOnce(t *testing.T) {
	f := supersedeFixture(t)
	createPlan(t, f, 6000)
	saves := 0
	f.publish.Plans = conflictingSaves{CapacityPlanRepository: f.plans, conflicts: 1, saves: &saves}
	if _, err := f.publish.Handle(context.Background(), "plan-1"); err != nil || saves != 2 {
		t.Fatalf("one conflict: err %v after %d saves, want success on the rerun", err, saves)
	}
	assertTypes(t, f.eventTypes(), "CapacityPlanCreated", "CapacityPlanPublished")
}

func TestPublishCapacityPlan_TwoLostRacesAreAVersionConflict(t *testing.T) {
	f := supersedeFixture(t)
	createPlan(t, f, 6000)
	saves := 0
	f.publish.Plans = conflictingSaves{CapacityPlanRepository: f.plans, conflicts: 2, saves: &saves}
	if _, err := f.publish.Handle(context.Background(), "plan-1"); !errors.Is(err, ports.ErrVersionConflict) || saves != 2 {
		t.Fatalf("two conflicts: err %v after %d saves, want ErrVersionConflict after 2", err, saves)
	}
	if p := f.stored(t, "plan-1"); p.Status() != capacityplan.StatusDraft {
		t.Errorf("plan-1 = %s, want DRAFT (rolled back)", p.Status())
	}
	assertTypes(t, f.eventTypes(), "CapacityPlanCreated")
}

// failsOnSupersededSave fails the save of the plan being superseded.
type failsOnSupersededSave struct {
	ports.CapacityPlanRepository
	err error
}

func (p failsOnSupersededSave) Save(ctx context.Context, plan *capacityplan.CapacityPlan) error {
	if plan.Status() == capacityplan.StatusSuperseded {
		return p.err
	}
	return p.CapacityPlanRepository.Save(ctx, plan)
}

// findPublishedFails fails the lookup of the plan to supersede.
type findPublishedFails struct {
	ports.CapacityPlanRepository
	err error
}

func (p findPublishedFails) FindPublished(context.Context, capacityplan.SupersedeKey, string) (*capacityplan.CapacityPlan, error) {
	return nil, p.err
}

func TestPublishCapacityPlan_ASupersedeFailureRollsTheWholeUnitBack(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]func(*planFixture){
		"older plan save fails": func(f *planFixture) {
			f.publish.Plans = failsOnSupersededSave{CapacityPlanRepository: f.plans, err: boom}
		},
		"older plan lookup fails": func(f *planFixture) {
			f.publish.Plans = findPublishedFails{CapacityPlanRepository: f.plans, err: boom}
		},
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			f := supersedeFixture(t)
			createPlan(t, f, 6000)
			f.mustPublish(t, "plan-1")
			createPlan(t, f, 6000)
			before := len(f.outbox.Messages())
			breakIt(f)
			if _, err := f.publish.Handle(context.Background(), "plan-2"); !errors.Is(err, boom) {
				t.Fatalf("err = %v, want boom", err)
			}
			if p := f.stored(t, "plan-1"); p.Status() != capacityplan.StatusPublished {
				t.Errorf("plan-1 = %s, want PUBLISHED (rolled back)", p.Status())
			}
			if p := f.stored(t, "plan-2"); p.Status() != capacityplan.StatusDraft {
				t.Errorf("plan-2 = %s, want DRAFT (rolled back)", p.Status())
			}
			if after := len(f.outbox.Messages()); after != before {
				t.Errorf("outbox grew from %d to %d", before, after)
			}
		})
	}
}

// publishedParams is a PUBLISHED plan of planCommand's key, published before
// any plan the fixture publishes.
func publishedParams(id string) capacityplan.RehydrateParams {
	w, _ := processcapacity.NewCapacityWindow(planWindowStart, planWindowEnd)
	return capacityplan.RehydrateParams{
		ID: id, WarehouseID: "WH-1", SiteID: "SIM1", Location: "PATH-ZONE-A", ProcessPathID: "pick-rebin-pack",
		Window: w, AssignedDemand: 10, Status: capacityplan.StatusPublished,
		CreatedAt: planCreatedAt, PublishedAt: planCreatedAt,
	}
}

func keyWindow(t *testing.T, start, end time.Time) processcapacity.CapacityWindow {
	t.Helper()
	w, err := processcapacity.NewCapacityWindow(start, end)
	if err != nil {
		t.Fatal(err)
	}
	return w
}
