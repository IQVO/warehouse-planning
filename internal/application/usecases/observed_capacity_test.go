package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

var observedAt = time.Date(2026, 10, 5, 10, 7, 30, 0, time.UTC)

func completionAt(t *testing.T, step string, at time.Time, dur *int64) observed.Completion {
	t.Helper()
	c, err := observed.NewCompletion(observed.CompletionParams{Location: "PATH-ZONE-A", ProcessType: step, At: at, DurationSeconds: dur})
	if err != nil {
		t.Fatalf("NewCompletion: %v", err)
	}
	return c
}

func int64ptr(v int64) *int64 { return &v }

// --- RecordObservedCompletion ------------------------------------------------

type observedRecordFixture struct {
	repo      *memory.ObservedCapacityRepo
	processed *memory.ProcessedEventRepo
	record    *RecordObservedCompletion
}

type failingObserved struct{ err error }

func (f failingObserved) Record(context.Context, observed.Completion) error { return f.err }
func (f failingObserved) Throughput(context.Context, string, string, time.Time, time.Time) (observed.Throughput, error) {
	return observed.Throughput{}, f.err
}

func newObservedRecordFixture(failing *failingObserved) *observedRecordFixture {
	repo, processed := memory.NewObservedCapacityRepo(), memory.NewProcessedEventRepo()
	f := &observedRecordFixture{repo: repo, processed: processed}
	f.record = &RecordObservedCompletion{UoW: memory.NewUnitOfWork(repo, processed), ProcessedEvents: processed, Observed: repo}
	if failing != nil {
		f.record.Observed = failing
	}
	return f
}

func TestRecordObservedCompletion_CountsOnceAndIgnoresAReplayOfTheSameEventID(t *testing.T) {
	f := newObservedRecordFixture(nil)
	ctx := context.Background()
	c := completionAt(t, "PICK", observedAt, int64ptr(40))
	if got, err := f.record.Handle(ctx, "c", "evt-1", c); err != nil || got != OutcomeApplied {
		t.Fatalf("first = %v, %v", got, err)
	}
	if got, err := f.record.Handle(ctx, "c", "evt-1", c); err != nil || got != OutcomeDuplicate {
		t.Fatalf("replay = %v, %v; want duplicate", got, err)
	}
	if f.repo.Tasks() != 1 {
		t.Fatalf("tasks = %d, want 1 (exactly-once effect on at-least-once delivery)", f.repo.Tasks())
	}
}

func TestRecordObservedCompletion_DistinctEventsAccumulateInTheMinuteBucket(t *testing.T) {
	f := newObservedRecordFixture(nil)
	ctx := context.Background()
	for i, id := range []string{"e1", "e2", "e3"} {
		if _, err := f.record.Handle(ctx, "c", id, completionAt(t, "PICK", observedAt.Add(time.Duration(i)*time.Second), int64ptr(30))); err != nil {
			t.Fatal(err)
		}
	}
	th, _ := f.repo.Throughput(ctx, "PATH-ZONE-A", "PICK", planWindowStart, planWindowEnd)
	if th.Tasks != 3 || th.DurationSeconds != 90 || th.TasksWithDuration != 3 {
		t.Fatalf("throughput = %+v", th)
	}
}

func TestRecordObservedCompletion_TransientFailureRollsBackTheClaim(t *testing.T) {
	boom := errors.New("transient database failure")
	f := newObservedRecordFixture(&failingObserved{err: boom})
	_, err := f.record.Handle(context.Background(), "c", "evt-1", completionAt(t, "PICK", observedAt, nil))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if f.processed.Has("c", "evt-1") {
		t.Fatal("the claim survived the failed write: the redelivery would be skipped")
	}
}

// --- GetCapacityPlanVariance -------------------------------------------------

func newVarianceFixture(t *testing.T) (*planFixture, *memory.ObservedCapacityRepo, *GetCapacityPlanVariance) {
	t.Helper()
	f := newPlanFixture(t)
	obs := memory.NewObservedCapacityRepo()
	return f, obs, &GetCapacityPlanVariance{Plans: f.plans, Observed: obs}
}

func recordN(t *testing.T, repo *memory.ObservedCapacityRepo, step string, n int, from time.Time) {
	t.Helper()
	for i := 0; i < n; i++ {
		if err := repo.Record(context.Background(), completionAt(t, step, from.Add(time.Duration(i)*time.Minute), int64ptr(20))); err != nil {
			t.Fatal(err)
		}
	}
}

func TestGetCapacityPlanVariance_ComparesTheBottleneckStepOverThePlanWindow(t *testing.T) {
	f, obs, get := newVarianceFixture(t)
	// REBIN is the bottleneck of the fixture: 1000 ORDER/h x 8h = 8000 planned.
	plan, err := f.create.Handle(context.Background(), planCommand(8000))
	if err != nil {
		t.Fatal(err)
	}
	recordN(t, obs, "REBIN", 400, planWindowStart)
	recordN(t, obs, "PICK", 50, planWindowStart) // another step: must not count
	if err := obs.Record(context.Background(), completionAt(t, "REBIN", planWindowEnd, nil)); err != nil {
		t.Fatal(err) // exactly at the end: next window
	}

	v, err := get.Handle(context.Background(), plan.ID(), f64ptr(0.05))
	if err != nil {
		t.Fatal(err)
	}
	assertVariancePlanFacts(t, v, plan.ID())
	if v.Tasks != 400 || v.TasksPerHour != 50 || !v.HasData {
		t.Fatalf("observed = %+v", v.Variance)
	}
	if v.MeanTaskSeconds == nil || *v.MeanTaskSeconds != 20 {
		t.Fatalf("mean task seconds = %v", v.MeanTaskSeconds)
	}
	assertVarianceOnPlan(t, v)
}

// assertVariancePlanFacts checks the plan side of the fixture's variance.
func assertVariancePlanFacts(t *testing.T, v PlanVariance, planID string) {
	t.Helper()
	if v.PlanID != planID || v.Step != "REBIN" || v.Location != "PATH-ZONE-A" {
		t.Fatalf("identity = %+v", v)
	}
	if !v.WindowStart.Equal(planWindowStart) || !v.WindowEnd.Equal(planWindowEnd) {
		t.Fatalf("window = %v..%v", v.WindowStart, v.WindowEnd)
	}
	if v.PlannedPerHour != 1000 || v.PlannedOverWindow != 8000 {
		t.Fatalf("planned = %v/h %v over the window", v.PlannedPerHour, v.PlannedOverWindow)
	}
}

// assertVarianceOnPlan: 400 tasks / 0.05 tasks per order = 8000 orders, on plan.
func assertVarianceOnPlan(t *testing.T, v PlanVariance) {
	t.Helper()
	if !v.Comparable || *v.ObservedOrders != 8000 {
		t.Fatalf("variance = %+v", v.Variance)
	}
	if *v.VarianceOrders != 0 || *v.VariancePercent != 0 {
		t.Fatalf("variance = %v orders, %v%%", *v.VarianceOrders, *v.VariancePercent)
	}
}

func TestGetCapacityPlanVariance_WithoutTasksPerOrderReportsObservedButNoVariance(t *testing.T) {
	f, obs, get := newVarianceFixture(t)
	plan, _ := f.create.Handle(context.Background(), planCommand(8000))
	recordN(t, obs, "REBIN", 10, planWindowStart)
	v, err := get.Handle(context.Background(), plan.ID(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if v.Comparable || v.Reason != observed.ReasonNoTasksPerOrd || v.Tasks != 10 || v.VarianceOrders != nil {
		t.Fatalf("variance = %+v", v)
	}
}

func TestGetCapacityPlanVariance_NoCompletionsIsNoData(t *testing.T) {
	f, _, get := newVarianceFixture(t)
	plan, _ := f.create.Handle(context.Background(), planCommand(8000))
	v, err := get.Handle(context.Background(), plan.ID(), f64ptr(2))
	if err != nil {
		t.Fatal(err)
	}
	if v.HasData || v.Comparable || v.Reason != observed.ReasonNoData || v.PlannedOverWindow != 8000 {
		t.Fatalf("variance = %+v", v)
	}
}

func TestGetCapacityPlanVariance_UnknownPlanAndBadFactorAndReadFailure(t *testing.T) {
	f, _, get := newVarianceFixture(t)
	if _, err := get.Handle(context.Background(), "nope", nil); !errors.Is(err, ErrCapacityPlanNotFound) {
		t.Fatalf("unknown plan err = %v", err)
	}
	plan, _ := f.create.Handle(context.Background(), planCommand(8000))
	if _, err := get.Handle(context.Background(), plan.ID(), f64ptr(0)); !errors.Is(err, observed.ErrNonPositiveTasksPerOrder) {
		t.Fatalf("bad factor err = %v", err)
	}
	boom := errors.New("db down")
	broken := &GetCapacityPlanVariance{Plans: f.plans, Observed: failingObserved{err: boom}}
	if _, err := broken.Handle(context.Background(), plan.ID(), nil); !errors.Is(err, boom) {
		t.Fatalf("read failure err = %v (never masked as no data)", err)
	}
}
