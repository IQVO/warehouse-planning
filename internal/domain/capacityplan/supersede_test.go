package capacityplan

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

// docs/adr/0024 decision 3: a PUBLISHED plan is superseded when a newer plan
// of its SupersedeKey is published; SUPERSEDED is terminal.

const newerPlanID = "9d1f0c2a-7b3e-4f5a-8c6d-2e4b1a0f9c87"

var supersededAt = time.Date(2026, 10, 4, 19, 5, 0, 0, time.UTC)

func publishedWorkedExample(t *testing.T) *CapacityPlan {
	t.Helper()
	plan := createWorkedExample(t, 12000)
	if err := plan.Publish(publishedAt); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	plan.PullEvents()
	return plan
}

func TestSupersede_MovesAPublishedPlanToSupersededAndRecordsTheEvent(t *testing.T) {
	plan := publishedWorkedExample(t)
	if err := plan.Supersede(newerPlanID, supersededAt); err != nil {
		t.Fatalf("Supersede: %v", err)
	}
	if plan.Status() != StatusSuperseded || plan.SupersededBy() != newerPlanID {
		t.Fatalf("status %s by %q, want SUPERSEDED by %s", plan.Status(), plan.SupersededBy(), newerPlanID)
	}
	if !plan.PublishedAt().Equal(publishedAt) || plan.Shortage() != 4000 || plan.AssignedDemand() != 12000 {
		t.Errorf("supersede changed stored facts: published_at %v, shortage %v, demand %v",
			plan.PublishedAt(), plan.Shortage(), plan.AssignedDemand())
	}
	want := []Event{CapacityPlanSuperseded{
		Header:       Header{PlanID: planID, At: supersededAt},
		SupersededBy: newerPlanID,
		WarehouseID:  warehouseID,
		SiteID:       siteID,
		Location:     location,
		PathID:       pathID,
		WindowStart:  windowStart,
		WindowEnd:    windowEnd,
	}}
	if got := plan.PullEvents(); !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %#v\nwant %#v", got, want)
	}
	if got := want[0].EventName(); got != "CapacityPlanSuperseded" {
		t.Errorf("EventName = %q", got)
	}
}

func TestSupersede_IsRefusedOutsidePublished(t *testing.T) {
	draft := createWorkedExample(t, 12000)
	draft.PullEvents()
	superseded := publishedWorkedExample(t)
	if err := superseded.Supersede(newerPlanID, supersededAt); err != nil {
		t.Fatal(err)
	}
	superseded.PullEvents()

	cases := []struct {
		name string
		plan *CapacityPlan
		by   string
		want error
		was  Status
	}{
		{"a DRAFT plan", draft, newerPlanID, ErrNotSupersedable, StatusDraft},
		{"twice", superseded, "another-plan", ErrSuperseded, StatusSuperseded},
		{"by a blank id", publishedWorkedExample(t), "", ErrNotSupersedable, StatusPublished},
		{"by itself", publishedWorkedExample(t), planID, ErrNotSupersedable, StatusPublished},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := tc.plan.SupersededBy()
			if err := tc.plan.Supersede(tc.by, supersededAt); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.plan.Status() != tc.was || tc.plan.SupersededBy() != before {
				t.Errorf("status %s by %q changed on a refused supersede", tc.plan.Status(), tc.plan.SupersededBy())
			}
			if got := tc.plan.PullEvents(); len(got) != 0 {
				t.Errorf("a refused supersede recorded %v", got)
			}
		})
	}
}

func TestPublish_ASupersededPlanIsNeverPublishedAgain(t *testing.T) {
	plan := Rehydrate(RehydrateParams{
		ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location, ProcessPathID: pathID,
		Window: mustWindow(t, windowStart, windowEnd), Status: StatusSuperseded,
		PublishedAt: publishedAt, SupersededBy: newerPlanID,
	})
	if err := plan.Publish(supersededAt); !errors.Is(err, ErrSuperseded) {
		t.Fatalf("err = %v, want ErrSuperseded", err)
	}
	if plan.Status() != StatusSuperseded || !plan.PublishedAt().Equal(publishedAt) || plan.SupersededBy() != newerPlanID {
		t.Errorf("refused publish changed the plan: %s at %v by %q", plan.Status(), plan.PublishedAt(), plan.SupersededBy())
	}
	if got := plan.PullEvents(); len(got) != 0 {
		t.Errorf("a refused publish recorded %v", got)
	}
}

func TestSupersedeKey_IsSiteLocationPathAndWindow(t *testing.T) {
	plan := createWorkedExample(t, 12000)
	key := plan.SupersedeKey()
	want := SupersedeKey{SiteID: siteID, Location: location, PathID: pathID, WindowStart: windowStart, WindowEnd: windowEnd}
	if key != want {
		t.Fatalf("key = %+v, want %+v", key, want)
	}
	if !key.Matches(plan) {
		t.Fatal("a plan does not match its own key")
	}
	brt := time.FixedZone("BRT", -3*3600)
	sameInstants := key
	sameInstants.WindowStart, sameInstants.WindowEnd = windowStart.In(brt), windowEnd.In(brt)
	if !sameInstants.Matches(plan) {
		t.Error("the window must compare as instants, whatever the zone")
	}
	differs := map[string]func(*SupersedeKey){
		"site":         func(k *SupersedeKey) { k.SiteID = "SIM2" },
		"location":     func(k *SupersedeKey) { k.Location = "PATH-ZONE-B" },
		"path":         func(k *SupersedeKey) { k.PathID = "pick-only" },
		"window start": func(k *SupersedeKey) { k.WindowStart = windowStart.Add(time.Minute) },
		"window end":   func(k *SupersedeKey) { k.WindowEnd = windowEnd.Add(time.Minute) },
	}
	for name, vary := range differs {
		other := key
		vary(&other)
		if other.Matches(plan) {
			t.Errorf("a key with another %s matches the plan", name)
		}
	}
}

func TestRehydrate_KeepsSupersededBy(t *testing.T) {
	plan := Rehydrate(RehydrateParams{ID: planID, Status: StatusSuperseded, SupersededBy: newerPlanID,
		Window: mustWindow(t, windowStart, windowEnd)})
	if plan.SupersededBy() != newerPlanID || plan.Status() != StatusSuperseded {
		t.Fatalf("rehydrated %s by %q", plan.Status(), plan.SupersededBy())
	}
}
