package usecases

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var demandAsOf = time.Date(2026, 10, 4, 9, 15, 30, 0, time.UTC)

func demandOrder(t *testing.T, id, location string, promise time.Time, lines int, asOf time.Time) demand.Order {
	t.Helper()
	o, err := demand.NewOrder(demand.OrderParams{OrderID: id, Location: location, PromiseAt: promise, ReleasedLines: lines, AsOf: asOf})
	if err != nil {
		t.Fatalf("NewOrder(%s): %v", id, err)
	}
	return o
}

// --- RecordOrderDemand -------------------------------------------------------

type recordFixture struct {
	repo      *memory.OrderDemandRepo
	processed *memory.ProcessedEventRepo
	record    *RecordOrderDemand
}

func newRecordFixture(demandRepo interface {
	Upsert(context.Context, demand.Order) (bool, error)
	Expected(context.Context, string, time.Time, time.Time) (demand.Summary, error)
}) *recordFixture {
	repo, processed := memory.NewOrderDemandRepo(), memory.NewProcessedEventRepo()
	if demandRepo == nil {
		demandRepo = repo
	}
	return &recordFixture{
		repo: repo, processed: processed,
		record: &RecordOrderDemand{
			UoW:             memory.NewUnitOfWork(repo, processed),
			ProcessedEvents: processed,
			Demand:          demandRepo,
		},
	}
}

func TestRecordOrderDemand_AppliesThenIgnoresAReplayOfTheSameEventID(t *testing.T) {
	f := newRecordFixture(nil)
	ctx := context.Background()
	first := demandOrder(t, "ord-1", "SIM1", planWindowStart.Add(time.Hour), 2, demandAsOf)

	got, err := f.record.Handle(ctx, "c", "evt-1", first)
	if err != nil || got != OutcomeApplied {
		t.Fatalf("first = %v, %v; want applied", got, err)
	}

	// The SAME event id replayed with a DIFFERENT payload: nothing may change.
	different := demandOrder(t, "ord-1", "SIM1", planWindowEnd.Add(time.Hour), 9, demandAsOf.Add(time.Hour))
	got, err = f.record.Handle(ctx, "c", "evt-1", different)
	if err != nil || got != OutcomeDuplicate {
		t.Fatalf("replay = %v, %v; want duplicate", got, err)
	}
	stored, _ := f.repo.Get("ord-1")
	if !stored.PromiseAt().Equal(first.PromiseAt()) || stored.ReleasedLines() != 2 {
		t.Fatalf("a replayed event id changed the model: %+v", stored)
	}
}

func TestRecordOrderDemand_ClaimIsPerConsumer(t *testing.T) {
	f := newRecordFixture(nil)
	o := demandOrder(t, "ord-1", "SIM1", planWindowStart, 1, demandAsOf)
	if got, err := f.record.Handle(context.Background(), "consumer-a", "evt-1", o); err != nil || got != OutcomeApplied {
		t.Fatalf("a = %v, %v", got, err)
	}
	other := demandOrder(t, "ord-2", "SIM1", planWindowStart, 1, demandAsOf)
	if got, err := f.record.Handle(context.Background(), "consumer-b", "evt-1", other); err != nil || got != OutcomeApplied {
		t.Fatalf("the same id under another consumer name must be processed: %v, %v", got, err)
	}
}

func TestRecordOrderDemand_OlderEventForTheSameOrderIsStale(t *testing.T) {
	f := newRecordFixture(nil)
	ctx := context.Background()
	newer := demandOrder(t, "ord-1", "SIM1", planWindowEnd.Add(time.Hour), 4, demandAsOf.Add(time.Hour))
	older := demandOrder(t, "ord-1", "SIM1", planWindowStart, 1, demandAsOf)

	if got, _ := f.record.Handle(ctx, "c", "evt-new", newer); got != OutcomeApplied {
		t.Fatalf("newer = %v", got)
	}
	got, err := f.record.Handle(ctx, "c", "evt-old", older)
	if err != nil || got != OutcomeStale {
		t.Fatalf("older = %v, %v; want stale", got, err)
	}
	stored, _ := f.repo.Get("ord-1")
	if stored.ReleasedLines() != 4 || !stored.AsOf().Equal(newer.AsOf()) {
		t.Fatalf("an older event overwrote the newer one: %+v", stored)
	}
	if !f.processed.Has("c", "evt-old") {
		t.Fatal("a stale event is still claimed (it was handled, it changed nothing)")
	}
}

type failingDemandRepo struct{ err error }

func (f failingDemandRepo) Upsert(context.Context, demand.Order) (bool, error) { return false, f.err }
func (f failingDemandRepo) Expected(context.Context, string, time.Time, time.Time) (demand.Summary, error) {
	return demand.Summary{}, f.err
}

// A repository failure after the claim must leave the claim UN-recorded, so
// the redelivery is processed instead of being skipped as already handled.
func TestRecordOrderDemand_TransientFailureRollsBackTheClaim(t *testing.T) {
	boom := errors.New("transient database failure")
	f := newRecordFixture(failingDemandRepo{err: boom})
	o := demandOrder(t, "ord-1", "SIM1", planWindowStart, 1, demandAsOf)

	got, err := f.record.Handle(context.Background(), "c", "evt-1", o)
	if !errors.Is(err, boom) || got != "" {
		t.Fatalf("Handle = %q, %v; want the injected error", got, err)
	}
	if f.processed.Has("c", "evt-1") {
		t.Fatal("the claim survived a failed handling: every redelivery would be skipped")
	}
	if f.repo.Len() != 0 {
		t.Fatal("something was written")
	}
}

type failingClaim struct{ err error }

func (f failingClaim) Claim(context.Context, string, string) (bool, error) { return false, f.err }

func TestRecordOrderDemand_ClaimFailureIsReturned(t *testing.T) {
	boom := errors.New("claim failed")
	repo := memory.NewOrderDemandRepo()
	rec := &RecordOrderDemand{UoW: memory.NewUnitOfWork(repo), ProcessedEvents: failingClaim{err: boom}, Demand: repo}
	o := demandOrder(t, "ord-1", "SIM1", planWindowStart, 1, demandAsOf)
	if _, err := rec.Handle(context.Background(), "c", "evt-1", o); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the claim error", err)
	}
	if repo.Len() != 0 {
		t.Fatal("an order was written without a claim")
	}
}

// --- GetExpectedDemand ---------------------------------------------------------

func TestGetExpectedDemand_WindowBoundaries(t *testing.T) {
	repo := memory.NewOrderDemandRepo()
	ctx := context.Background()
	for i, o := range []demand.Order{
		demandOrder(t, "before", "SIM1", planWindowStart.Add(-time.Second), 1, demandAsOf),
		demandOrder(t, "at-start", "SIM1", planWindowStart, 2, demandAsOf.Add(time.Minute)),
		demandOrder(t, "last-second", "SIM1", planWindowEnd.Add(-time.Second), 3, demandAsOf.Add(2*time.Minute)),
		demandOrder(t, "at-end", "SIM1", planWindowEnd, 5, demandAsOf.Add(3*time.Minute)),
		demandOrder(t, "elsewhere", "SIM2", planWindowStart.Add(time.Hour), 7, demandAsOf.Add(time.Hour)),
	} {
		if _, err := repo.Upsert(ctx, o); err != nil {
			t.Fatalf("seed %d: %v", i, err)
		}
	}
	uc := &GetExpectedDemand{Demand: repo}

	got, err := uc.Handle(ctx, "SIM1", planWindowStart, planWindowEnd)
	if err != nil {
		t.Fatal(err)
	}
	if got.Orders != 2 || got.ReleasedLines != 2+3 {
		t.Fatalf("summary = %+v, want exactly the orders at start and one second before end", got)
	}
	if !got.AsOf.Equal(demandAsOf.Add(3 * time.Minute)) {
		t.Fatalf("AsOf = %v: the other site's newer event must not leak in", got.AsOf)
	}

	other, _ := uc.Handle(ctx, "SIM2", planWindowStart, planWindowEnd)
	if other.Orders != 1 || other.ReleasedLines != 7 {
		t.Fatalf("SIM2 summary = %+v", other)
	}
}

func TestGetExpectedDemand_RejectsAnEmptyOrInvertedWindow(t *testing.T) {
	uc := &GetExpectedDemand{Demand: memory.NewOrderDemandRepo()}
	for name, end := range map[string]time.Time{"end == start": planWindowStart, "end < start": planWindowStart.Add(-time.Hour)} {
		if _, err := uc.Handle(context.Background(), "SIM1", planWindowStart, end); !errors.Is(err, processcapacity.ErrInvalidWindow) {
			t.Errorf("%s: err = %v, want ErrInvalidWindow", name, err)
		}
	}
}

// --- CreateCapacityPlan: defaulting the demand from orders ---------------------

func seedOrders(t *testing.T, repo *memory.OrderDemandRepo, location string, n int, promise time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		o := demandOrder(t, fmt.Sprintf("%s-%d", location, i), location, promise, 1, demandAsOf)
		if _, err := repo.Upsert(context.Background(), o); err != nil {
			t.Fatal(err)
		}
	}
}

func newDemandPlanFixture(t *testing.T) (*planFixture, *memory.OrderDemandRepo) {
	t.Helper()
	f := newPlanFixture(t)
	repo := memory.NewOrderDemandRepo()
	f.create.Demand = &GetExpectedDemand{Demand: repo}
	return f, repo
}

func omittedDemandCommand() CreateCapacityPlanCommand {
	cmd := planCommand(0)
	cmd.DemandFromOrders = true
	return cmd
}

func TestCreateCapacityPlan_ExplicitDemandAlwaysWins(t *testing.T) {
	f, repo := newDemandPlanFixture(t)
	seedOrders(t, repo, "PATH-ZONE-A", 300, planWindowStart.Add(time.Hour)) // 300 orders exist...
	plan, err := f.create.Handle(context.Background(), planCommand(12000))  // ...but 12000 is stated
	if err != nil {
		t.Fatal(err)
	}
	if plan.AssignedDemand() != 12000 || plan.DemandSource() != capacityplan.DemandSourceRequest || plan.Shortage() != 4000 {
		t.Fatalf("plan = demand %v source %s shortage %v", plan.AssignedDemand(), plan.DemandSource(), plan.Shortage())
	}
}

func TestCreateCapacityPlan_ExplicitZeroIsAFigureNotAnOmission(t *testing.T) {
	f, repo := newDemandPlanFixture(t)
	seedOrders(t, repo, "PATH-ZONE-A", 300, planWindowStart.Add(time.Hour))
	plan, err := f.create.Handle(context.Background(), planCommand(0))
	if err != nil {
		t.Fatal(err)
	}
	if plan.AssignedDemand() != 0 || plan.DemandSource() != capacityplan.DemandSourceRequest {
		t.Fatalf("an explicit 0 was replaced by order demand: %v / %s", plan.AssignedDemand(), plan.DemandSource())
	}
}

func TestCreateCapacityPlan_OmittedDemandUsesTheExpectedOrders(t *testing.T) {
	f, repo := newDemandPlanFixture(t)
	// 8500 orders inside the 8h window (capacity 8000): short by 500.
	seedOrders(t, repo, "PATH-ZONE-A", 8500, planWindowStart.Add(2*time.Hour))
	// Noise that must not count: another site, and orders on both sides of the window.
	seedOrders(t, repo, "OTHER", 40, planWindowStart.Add(2*time.Hour))
	if _, err := repo.Upsert(context.Background(), demandOrder(t, "at-end", "PATH-ZONE-A", planWindowEnd, 1, demandAsOf)); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Upsert(context.Background(), demandOrder(t, "before", "PATH-ZONE-A", planWindowStart.Add(-time.Second), 1, demandAsOf)); err != nil {
		t.Fatal(err)
	}

	plan, err := f.create.Handle(context.Background(), omittedDemandCommand())
	if err != nil {
		t.Fatal(err)
	}
	if plan.AssignedDemand() != 8500 {
		t.Fatalf("AssignedDemand = %v, want exactly the 8500 orders in the window", plan.AssignedDemand())
	}
	if plan.DemandSource() != capacityplan.DemandSourceOrders || plan.Shortage() != 500 || plan.CapacityOverWindow() != 8000 {
		t.Fatalf("source %s shortage %v capacity %v", plan.DemandSource(), plan.Shortage(), plan.CapacityOverWindow())
	}

	stored, _ := f.plans.FindByID(context.Background(), plan.ID())
	if stored == nil || stored.DemandSource() != capacityplan.DemandSourceOrders {
		t.Fatalf("the persisted plan lost its demand source: %+v", stored)
	}
}

func TestCreateCapacityPlan_OmittedDemandWithoutOrderDataIsMissingAssignedDemand(t *testing.T) {
	cases := map[string]func(*planFixture, *memory.OrderDemandRepo){
		"no orders at all": func(*planFixture, *memory.OrderDemandRepo) {},
		"orders only at another site": func(_ *planFixture, r *memory.OrderDemandRepo) {
			seedOrders(t, r, "OTHER", 5, planWindowStart.Add(time.Hour))
		},
		"orders only outside the window, one exactly at its end": func(_ *planFixture, r *memory.OrderDemandRepo) {
			if _, err := r.Upsert(context.Background(), demandOrder(t, "at-end", "PATH-ZONE-A", planWindowEnd, 1, demandAsOf)); err != nil {
				t.Fatal(err)
			}
		},
		"no demand model wired": func(f *planFixture, _ *memory.OrderDemandRepo) { f.create.Demand = nil },
	}
	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			f, repo := newDemandPlanFixture(t)
			arrange(f, repo)
			plan, err := f.create.Handle(context.Background(), omittedDemandCommand())
			if !errors.Is(err, ErrMissingAssignedDemand) || plan != nil {
				t.Fatalf("plan %v, err %v; want ErrMissingAssignedDemand and no plan", plan, err)
			}
			if got := f.eventTypes(); len(got) != 0 {
				t.Fatalf("a rejected request queued events: %v", got)
			}
			if stored, _ := f.plans.FindByID(context.Background(), "plan-1"); stored != nil {
				t.Fatal("a rejected request stored a plan")
			}
		})
	}
}

func TestCreateCapacityPlan_OmittedDemandIsRejectedBeforeAnythingElseIsChecked(t *testing.T) {
	f, _ := newDemandPlanFixture(t)
	cmd := omittedDemandCommand()
	cmd.ProcessPathID = "no-such-path" // would be a 404 if demand were not resolved first
	if _, err := f.create.Handle(context.Background(), cmd); !errors.Is(err, ErrMissingAssignedDemand) {
		t.Fatalf("err = %v, want ErrMissingAssignedDemand (today's precedence)", err)
	}

	cmd = omittedDemandCommand()
	cmd.WindowEnd = cmd.WindowStart // an unusable window cannot hold demand
	if _, err := f.create.Handle(context.Background(), cmd); !errors.Is(err, ErrMissingAssignedDemand) {
		t.Fatalf("inverted window err = %v, want ErrMissingAssignedDemand", err)
	}
}

func TestCreateCapacityPlan_DemandReadFailureIsNotMaskedAsMissingDemand(t *testing.T) {
	f, _ := newDemandPlanFixture(t)
	boom := errors.New("database down")
	f.create.Demand = &GetExpectedDemand{Demand: failingDemandRepo{err: boom}}
	if _, err := f.create.Handle(context.Background(), omittedDemandCommand()); !errors.Is(err, boom) || errors.Is(err, ErrMissingAssignedDemand) {
		t.Fatalf("err = %v, want the infrastructure error passed through", err)
	}
}
