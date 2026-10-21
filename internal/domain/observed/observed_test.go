package observed_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

var (
	from = time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)
	to   = time.Date(2026, 10, 9, 16, 0, 0, 0, time.UTC)
)

func i64(v int64) *int64     { return &v }
func f64(v float64) *float64 { return &v }
func completion(t *testing.T, step string, at time.Time, dur *int64) observed.Completion {
	t.Helper()
	c, err := observed.NewCompletion(observed.CompletionParams{Location: "SIM1", ProcessType: step, At: at, DurationSeconds: dur})
	if err != nil {
		t.Fatalf("NewCompletion: %v", err)
	}
	return c
}

func TestNewCompletionValidationAndNormalization(t *testing.T) {
	if _, err := observed.NewCompletion(observed.CompletionParams{ProcessType: "PICK", At: from}); !errors.Is(err, observed.ErrRequiredField) {
		t.Errorf("blank location: %v", err)
	}
	if _, err := observed.NewCompletion(observed.CompletionParams{Location: "SIM1", ProcessType: "  ", At: from}); !errors.Is(err, observed.ErrRequiredField) {
		t.Errorf("blank process type: %v", err)
	}
	if _, err := observed.NewCompletion(observed.CompletionParams{Location: "SIM1", ProcessType: "PICK"}); !errors.Is(err, observed.ErrAtRequired) {
		t.Errorf("no time: %v", err)
	}
	if _, err := observed.NewCompletion(observed.CompletionParams{Location: "SIM1", ProcessType: "PICK", At: from, DurationSeconds: i64(-1)}); !errors.Is(err, observed.ErrNegativeDuration) {
		t.Errorf("negative duration: %v", err)
	}

	zone := time.FixedZone("BRT", -3*3600)
	at := time.Date(2026, 10, 9, 5, 7, 42, 500, zone) // 08:07:42.0000005Z
	c := completion(t, " pick ", at, i64(0))
	if c.ProcessType() != "PICK" || c.Location() != "SIM1" {
		t.Fatalf("completion = %s @ %s", c.ProcessType(), c.Location())
	}
	if want := time.Date(2026, 10, 9, 8, 7, 0, 0, time.UTC); !c.Minute().Equal(want) || c.Minute().Location() != time.UTC {
		t.Fatalf("minute = %v, want %v UTC", c.Minute(), want)
	}
	if !c.At().Equal(at) || c.At().Location() != time.UTC {
		t.Fatalf("at = %v", c.At())
	}
	if d, ok := c.DurationSeconds(); !ok || d != 0 {
		t.Fatalf("a zero duration is a duration: %d %v", d, ok)
	}
	if _, ok := completion(t, "PICK", from, nil).DurationSeconds(); ok {
		t.Fatal("no duration given but one reported")
	}
}

func TestCountsInIsHalfOpenOnTheMinuteBucket(t *testing.T) {
	cases := map[string]struct {
		at   time.Time
		want bool
	}{
		"exactly the window start":         {from, true},
		"the last second before the start": {from.Add(-time.Second), false},
		"second 59 of the last minute":     {to.Add(-time.Second), true},
		"exactly the window end":           {to, false},
		"the minute before the end":        {to.Add(-time.Minute), true},
	}
	for name, c := range cases {
		if got := completion(t, "PICK", c.at, nil).CountsIn(from, to); got != c.want {
			t.Errorf("%s: CountsIn = %v, want %v", name, got, c.want)
		}
	}
}

func TestSumAggregatesWindowAndKeepsStepAsOf(t *testing.T) {
	cs := []observed.Completion{
		completion(t, "PICK", from, i64(30)),
		completion(t, "PICK", from.Add(time.Hour), i64(90)),
		completion(t, "PICK", from.Add(2*time.Hour), nil),
		completion(t, "PICK", to, i64(1000)), // outside, but newest
	}
	th := observed.Sum(cs, from, to)
	if th.Tasks != 3 || th.DurationSeconds != 120 || th.TasksWithDuration != 2 {
		t.Fatalf("throughput = %+v", th)
	}
	if !th.AsOf.Equal(to) {
		t.Fatalf("as_of = %v, want the newest completion even outside the window", th.AsOf)
	}
	if !th.HasData() {
		t.Fatal("HasData false with 3 tasks")
	}
	if mean, ok := th.MeanTaskSeconds(); !ok || mean != 60 {
		t.Fatalf("mean = %v %v, want 60 over the 2 timed tasks", mean, ok)
	}
	if got := th.TasksPerHour(8 * time.Hour); got != 3.0/8 {
		t.Fatalf("tasks/h = %v", got)
	}
}

func TestEmptyThroughput(t *testing.T) {
	th := observed.Sum(nil, from, to)
	if th.HasData() || th.Tasks != 0 || !th.AsOf.IsZero() {
		t.Fatalf("empty = %+v", th)
	}
	if _, ok := th.MeanTaskSeconds(); ok {
		t.Fatal("mean of nothing")
	}
	if got := (observed.Throughput{Tasks: 5}).TasksPerHour(0); got != 0 {
		t.Fatalf("zero window tasks/h = %v", got)
	}
	if got := (observed.Throughput{Tasks: 5}).TasksPerHour(-time.Hour); got != 0 {
		t.Fatalf("negative window tasks/h = %v", got)
	}
}

func TestCompareWithoutDataIsNotZeroThroughput(t *testing.T) {
	v, err := observed.Compare(100, 8*time.Hour, observed.Throughput{}, f64(2))
	if err != nil {
		t.Fatal(err)
	}
	if v.HasData || v.Comparable || v.Reason != observed.ReasonNoData || v.VarianceOrders != nil || v.ObservedOrders != nil {
		t.Fatalf("variance = %+v", v)
	}
}

func TestCompareNeedsTasksPerOrderToConvertUnits(t *testing.T) {
	th := observed.Throughput{Tasks: 400, DurationSeconds: 400 * 30, TasksWithDuration: 400, AsOf: to}
	v, err := observed.Compare(150, 8*time.Hour, th, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !v.HasData || v.Comparable || v.Reason != observed.ReasonNoTasksPerOrd {
		t.Fatalf("variance = %+v", v)
	}
	if v.VarianceOrders != nil || v.VariancePercent != nil || v.TasksPerOrder != nil {
		t.Fatalf("a variance was invented without a conversion factor: %+v", v)
	}
	if v.Tasks != 400 || v.TasksPerHour != 50 || v.MeanTaskSeconds == nil || *v.MeanTaskSeconds != 30 || !v.AsOf.Equal(to) {
		t.Fatalf("observed side = %+v", v)
	}
}

func TestCompareComputesVariance(t *testing.T) {
	th := observed.Throughput{Tasks: 400, AsOf: to}
	// 400 tasks / 4 tasks per order = 100 orders against 125 planned.
	v, err := observed.Compare(125, 8*time.Hour, th, f64(4))
	if err != nil {
		t.Fatal(err)
	}
	if !v.Comparable || v.Reason != "" {
		t.Fatalf("variance = %+v", v)
	}
	if *v.ObservedOrders != 100 || *v.VarianceOrders != -25 || *v.VariancePercent != -20 || *v.TasksPerOrder != 4 {
		t.Fatalf("variance = orders %v diff %v pct %v", *v.ObservedOrders, *v.VarianceOrders, *v.VariancePercent)
	}
	if v.MeanTaskSeconds != nil {
		t.Fatal("no task carried a duration, mean must be nil")
	}
}

func TestComparePlannedZeroHasNoPercent(t *testing.T) {
	v, err := observed.Compare(0, time.Hour, observed.Throughput{Tasks: 10}, f64(1))
	if err != nil {
		t.Fatal(err)
	}
	if !v.Comparable || *v.VarianceOrders != 10 || v.VariancePercent != nil {
		t.Fatalf("variance = %+v", v)
	}
}

func TestCompareRejectsABadTasksPerOrderEvenWithoutData(t *testing.T) {
	for _, bad := range []float64{0, -1, math.NaN(), math.Inf(1)} {
		if _, err := observed.Compare(10, time.Hour, observed.Throughput{}, f64(bad)); !errors.Is(err, observed.ErrNonPositiveTasksPerOrder) {
			t.Errorf("tasks_per_order %v: err = %v", bad, err)
		}
	}
}
