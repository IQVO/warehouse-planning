package usecases

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var (
	planWindowStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	planWindowEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	planCreatedAt   = time.Date(2026, 10, 4, 17, 15, 30, 0, time.UTC)
	planPublishedAt = time.Date(2026, 10, 4, 18, 45, 10, 0, time.UTC)
)

// fakeEncoder maps each event to one message named after it; it is a
// stand-in for the Kafka adapter's CloudEvents encoder.
type fakeEncoder struct{ err error }

func (e fakeEncoder) Encode(events ...capacityplan.Event) ([]outbox.Message, error) {
	if e.err != nil {
		return nil, e.err
	}
	out := make([]outbox.Message, 0, len(events))
	for i, ev := range events {
		out = append(out, outbox.Message{
			EventID:   fmt.Sprintf("evt-%s-%d", ev.EventName(), i),
			EventType: ev.EventName(),
			Subject:   ev.AggregateID(),
		})
	}
	return out, nil
}

// failingOutbox fails every Insert.
type failingOutbox struct{ err error }

func (f failingOutbox) Insert(context.Context, ...outbox.Message) error { return f.err }

// failingPlans saves for real and then reports failure (a commit-time
// style failure), so tests prove the Save itself is rolled back.
type failingPlans struct {
	ports.CapacityPlanRepository
	saveErr error
}

func (f failingPlans) Save(ctx context.Context, p *capacityplan.CapacityPlan) error {
	if err := f.CapacityPlanRepository.Save(ctx, p); err != nil {
		return err
	}
	return f.saveErr
}

type planFixture struct {
	pcs     *memory.ProcessCapacityRepo
	paths   *memory.ProcessPathRepo
	plans   *memory.CapacityPlanRepo
	outbox  *memory.OutboxRepo
	uow     *memory.UnitOfWork
	create  *CreateCapacityPlan
	publish *PublishCapacityPlan
}

func newPlanFixture(t *testing.T) *planFixture {
	t.Helper()
	pcs, paths, plans, ob := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	uow := memory.NewUnitOfWork(pcs, plans, ob)
	seedPickRebinPackPathCapacity(t, context.Background(), pcs, paths, planWindowStart, planWindowEnd)
	f := &planFixture{pcs: pcs, paths: paths, plans: plans, outbox: ob, uow: uow}
	f.create = &CreateCapacityPlan{
		PathCapacity: &GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs},
		Plans:        plans, Outbox: ob, Encoder: fakeEncoder{}, UnitOfWork: uow,
		NewID: func() string { return "plan-1" },
		Now:   func() time.Time { return planCreatedAt },
	}
	f.publish = &PublishCapacityPlan{
		Plans: plans, Outbox: ob, Encoder: fakeEncoder{}, UnitOfWork: uow,
		Now: func() time.Time { return planPublishedAt },
	}
	return f
}

func planCommand(demand float64) CreateCapacityPlanCommand {
	return CreateCapacityPlanCommand{
		WarehouseID:      "WH-1",
		Location:         "PATH-ZONE-A",
		WindowStart:      planWindowStart,
		WindowEnd:        planWindowEnd,
		ProcessPathID:    "pick-rebin-pack",
		AssignedDemand:   demand,
		UnitsPerOrder:    f64ptr(2.5),
		PackagesPerOrder: f64ptr(1),
	}
}

func (f *planFixture) eventTypes() []string {
	var out []string
	for _, m := range f.outbox.Messages() {
		out = append(out, m.EventType)
	}
	return out
}

func assertTypes(t *testing.T, got []string, want ...string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(append([]string(nil), want...)) {
		t.Fatalf("event types = %v, want %v", got, want)
	}
}

func TestCreateCapacityPlan_WorkedExample(t *testing.T) {
	f := newPlanFixture(t)
	plan, err := f.create.Handle(context.Background(), planCommand(12000))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if plan.ID() != "plan-1" || plan.Status() != capacityplan.StatusDraft {
		t.Errorf("id/status = %s/%s", plan.ID(), plan.Status())
	}
	if plan.PathCapacity() != 1000 || plan.BottleneckStep() != "REBIN" || plan.CapacityOverWindow() != 8000 || plan.Shortage() != 4000 {
		t.Errorf("computed = %v %s %v %v", plan.PathCapacity(), plan.BottleneckStep(), plan.CapacityOverWindow(), plan.Shortage())
	}

	stored, err := f.plans.FindByID(context.Background(), "plan-1")
	if err != nil || stored == nil || stored.Shortage() != 4000 || stored.Status() != capacityplan.StatusDraft {
		t.Fatalf("stored plan = %+v, err %v", stored, err)
	}
	// Only CapacityPlanCreated is queued at creation; the others wait for Publish.
	assertTypes(t, f.eventTypes(), "CapacityPlanCreated")
	if got := plan.PullEvents(); len(got) != 0 {
		t.Errorf("events were not pulled: %d left on the aggregate", len(got))
	}
}

func TestCreateCapacityPlan_Rejections(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*CreateCapacityPlanCommand)
		want   error
	}{
		{"inverted window", func(c *CreateCapacityPlanCommand) { c.WindowEnd = c.WindowStart }, processcapacity.ErrInvalidWindow},
		{"unknown path", func(c *CreateCapacityPlanCommand) { c.ProcessPathID = "nope" }, ErrProcessPathNotFound},
		{"window without step capacity", func(c *CreateCapacityPlanCommand) { c.WindowEnd = c.WindowEnd.Add(time.Hour) }, processcapacity.ErrMissingStepCapacity},
		{"non-positive factor", func(c *CreateCapacityPlanCommand) { c.UnitsPerOrder = f64ptr(0) }, processcapacity.ErrNonPositiveConversionFactor},
		{"missing factor", func(c *CreateCapacityPlanCommand) { c.UnitsPerOrder = nil }, processcapacity.ErrMissingConversionFactor},
		{"negative demand", func(c *CreateCapacityPlanCommand) { c.AssignedDemand = -1 }, capacityplan.ErrNegativeDemand},
		{"blank warehouse", func(c *CreateCapacityPlanCommand) { c.WarehouseID = "" }, capacityplan.ErrRequiredField},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPlanFixture(t)
			cmd := planCommand(12000)
			tc.mutate(&cmd)
			plan, err := f.create.Handle(context.Background(), cmd)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if plan != nil {
				t.Errorf("plan = %+v, want nil on error", plan)
			}
			if got, _ := f.plans.FindByID(context.Background(), "plan-1"); got != nil {
				t.Error("a plan was saved despite the error")
			}
			if len(f.outbox.Messages()) != 0 {
				t.Error("outbox rows were written despite the error")
			}
		})
	}
}

func TestCreateCapacityPlan_IsAtomic(t *testing.T) {
	boom := errors.New("boom")
	t.Run("outbox insert fails -> plan rolled back", func(t *testing.T) {
		f := newPlanFixture(t)
		f.create.Outbox = failingOutbox{err: boom}
		if _, err := f.create.Handle(context.Background(), planCommand(12000)); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if got, _ := f.plans.FindByID(context.Background(), "plan-1"); got != nil {
			t.Error("plan survived a failed outbox insert")
		}
	})
	t.Run("encoder fails -> plan rolled back", func(t *testing.T) {
		f := newPlanFixture(t)
		f.create.Encoder = fakeEncoder{err: boom}
		if _, err := f.create.Handle(context.Background(), planCommand(12000)); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if got, _ := f.plans.FindByID(context.Background(), "plan-1"); got != nil {
			t.Error("plan survived an encode failure")
		}
	})
	t.Run("plan save fails -> no outbox row", func(t *testing.T) {
		f := newPlanFixture(t)
		f.create.Plans = failingPlans{CapacityPlanRepository: f.plans, saveErr: boom}
		if _, err := f.create.Handle(context.Background(), planCommand(12000)); !errors.Is(err, boom) {
			t.Fatalf("err = %v, want boom", err)
		}
		if got, _ := f.plans.FindByID(context.Background(), "plan-1"); got != nil {
			t.Error("plan survived a failed save")
		}
		if len(f.outbox.Messages()) != 0 {
			t.Error("outbox rows exist for a plan whose save failed")
		}
	})
}

func TestCreateCapacityPlan_DefaultsIDAndClock(t *testing.T) {
	f := newPlanFixture(t)
	f.create.NewID, f.create.Now = nil, nil
	before := time.Now().UTC().Add(-time.Second)
	plan, err := f.create.Handle(context.Background(), planCommand(6000))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(plan.ID()) != 36 {
		t.Errorf("default id %q is not a UUID", plan.ID())
	}
	if plan.CreatedAt().Before(before) || plan.CreatedAt().Location() != time.UTC || plan.CreatedAt().Nanosecond()%1000 != 0 {
		t.Errorf("CreatedAt = %v, want a recent UTC time", plan.CreatedAt())
	}
}

func createPlan(t *testing.T, f *planFixture, demand float64) {
	t.Helper()
	if _, err := f.create.Handle(context.Background(), planCommand(demand)); err != nil {
		t.Fatalf("create: %v", err)
	}
}

func TestPublishCapacityPlan_ShortageQueuesEveryEvent(t *testing.T) {
	f := newPlanFixture(t)
	createPlan(t, f, 12000)

	plan, err := f.publish.Handle(context.Background(), "plan-1")
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if plan.Status() != capacityplan.StatusPublished || !plan.PublishedAt().Equal(planPublishedAt) {
		t.Errorf("status/publishedAt = %s/%v", plan.Status(), plan.PublishedAt())
	}
	stored, _ := f.plans.FindByID(context.Background(), "plan-1")
	if stored.Status() != capacityplan.StatusPublished {
		t.Errorf("stored status = %s, want PUBLISHED", stored.Status())
	}
	assertTypes(t, f.eventTypes(), "CapacityPlanCreated", "CapacityPlanPublished", "CapacityShortageDetected", "BottleneckDetected")
}

func TestPublishCapacityPlan_WithinCapacityQueuesOnlyPublished(t *testing.T) {
	f := newPlanFixture(t)
	createPlan(t, f, 6000)
	if _, err := f.publish.Handle(context.Background(), "plan-1"); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	assertTypes(t, f.eventTypes(), "CapacityPlanCreated", "CapacityPlanPublished")
}

func TestPublishCapacityPlan_NotFound(t *testing.T) {
	f := newPlanFixture(t)
	if _, err := f.publish.Handle(context.Background(), "ghost"); !errors.Is(err, ErrCapacityPlanNotFound) {
		t.Fatalf("err = %v, want ErrCapacityPlanNotFound", err)
	}
}

func TestPublishCapacityPlan_SecondPublishIsRejectedAndQueuesNothing(t *testing.T) {
	f := newPlanFixture(t)
	createPlan(t, f, 12000)
	if _, err := f.publish.Handle(context.Background(), "plan-1"); err != nil {
		t.Fatalf("first: %v", err)
	}
	before := len(f.outbox.Messages())
	plan, err := f.publish.Handle(context.Background(), "plan-1")
	if !errors.Is(err, capacityplan.ErrAlreadyPublished) || plan != nil {
		t.Fatalf("second publish = %v, %v; want nil, ErrAlreadyPublished", plan, err)
	}
	if after := len(f.outbox.Messages()); after != before {
		t.Errorf("outbox grew from %d to %d on a rejected publish", before, after)
	}
}

func TestPublishCapacityPlan_IsAtomic(t *testing.T) {
	boom := errors.New("boom")
	cases := map[string]func(*planFixture){
		"outbox insert fails": func(f *planFixture) { f.publish.Outbox = failingOutbox{err: boom} },
		"encoder fails":       func(f *planFixture) { f.publish.Encoder = fakeEncoder{err: boom} },
		"plan save fails":     func(f *planFixture) { f.publish.Plans = failingPlans{CapacityPlanRepository: f.plans, saveErr: boom} },
	}
	for name, breakIt := range cases {
		t.Run(name, func(t *testing.T) {
			f := newPlanFixture(t)
			createPlan(t, f, 12000)
			breakIt(f)
			if _, err := f.publish.Handle(context.Background(), "plan-1"); !errors.Is(err, boom) {
				t.Fatalf("err = %v, want boom", err)
			}
			stored, _ := f.plans.FindByID(context.Background(), "plan-1")
			if stored.Status() != capacityplan.StatusDraft {
				t.Errorf("status = %s, want DRAFT (rolled back)", stored.Status())
			}
			assertTypes(t, f.eventTypes(), "CapacityPlanCreated")
		})
	}
}

func TestPublishCapacityPlan_FindFailureIsReturned(t *testing.T) {
	boom := errors.New("boom")
	f := newPlanFixture(t)
	f.publish.Plans = findFailsPlans{boom}
	if _, err := f.publish.Handle(context.Background(), "plan-1"); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

type findFailsPlans struct{ err error }

func (f findFailsPlans) Save(context.Context, *capacityplan.CapacityPlan) error { return nil }
func (f findFailsPlans) FindByID(context.Context, string) (*capacityplan.CapacityPlan, error) {
	return nil, f.err
}

func TestPublishCapacityPlan_DefaultsClock(t *testing.T) {
	f := newPlanFixture(t)
	createPlan(t, f, 6000)
	f.publish.Now = nil
	before := time.Now().UTC().Add(-time.Second)
	plan, err := f.publish.Handle(context.Background(), "plan-1")
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if plan.PublishedAt().Before(before) || plan.PublishedAt().Location() != time.UTC || plan.PublishedAt().Nanosecond()%1000 != 0 {
		t.Errorf("PublishedAt = %v, want a recent UTC time", plan.PublishedAt())
	}
}
