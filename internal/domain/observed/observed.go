// Package observed holds the observed-capacity rules of the planning context
// (docs/adr/0017, gap X4): how a fulfillment-execution TaskCompleted event
// becomes a per-minute throughput bucket, and how the throughput observed in a
// plan's window is compared with what the plan assumed.
//
// Observed capacity is ADVISORY. Nothing here changes a planned capacity or a
// stored plan (docs/adr/0017 decision 10). The package performs NO I/O and
// imports nothing from this module.
package observed

import (
	"errors"
	"math"
	"strings"
	"time"
)

var (
	// ErrRequiredField is returned when the site or the process type is
	// blank.
	ErrRequiredField = errors.New("observed: location and process type are required")

	// ErrAtRequired is returned when the completion time is missing: it
	// places the completion in a minute bucket.
	ErrAtRequired = errors.New("observed: the completion time is required")

	// ErrNegativeDuration is returned for a negative task duration.
	ErrNegativeDuration = errors.New("observed: task duration must not be negative")

	// ErrNonPositiveTasksPerOrder is returned for a tasks-per-order factor
	// that is zero, negative or not a finite number.
	ErrNonPositiveTasksPerOrder = errors.New("observed: tasks_per_order must be a positive number")
)

// Completion is one completed task as the observed-capacity read model counts
// it: the site (configured, the event carries none), the process STEP the
// task was of (the event's task_type, upper-cased: it carries no path id), the
// minute it completed in, and optionally how long it took.
type Completion struct {
	location        string
	processType     string
	minute          time.Time
	durationSeconds int64
	hasDuration     bool
	at              time.Time
}

// CompletionParams carries everything NewCompletion needs.
type CompletionParams struct {
	Location string
	// ProcessType is the TaskCompleted task_type (PICK, PACK, ...).
	ProcessType string
	// At is the CloudEvents `time` of the TaskCompleted event.
	At time.Time
	// DurationSeconds is the event's duration_seconds; nil when it carries
	// none (the claim time was unknown).
	DurationSeconds *int64
}

// NewCompletion validates p and builds a Completion. The process type is
// trimmed and upper-cased; the instant is normalized to UTC and bucketed to
// its minute.
func NewCompletion(p CompletionParams) (Completion, error) {
	processType := strings.ToUpper(strings.TrimSpace(p.ProcessType))
	if p.Location == "" || processType == "" {
		return Completion{}, ErrRequiredField
	}
	if p.At.IsZero() {
		return Completion{}, ErrAtRequired
	}
	c := Completion{
		location:    p.Location,
		processType: processType,
		minute:      p.At.UTC().Truncate(time.Minute),
		at:          p.At.UTC(),
	}
	if p.DurationSeconds != nil {
		if *p.DurationSeconds < 0 {
			return Completion{}, ErrNegativeDuration
		}
		c.durationSeconds, c.hasDuration = *p.DurationSeconds, true
	}
	return c, nil
}

// Location returns the site the completion is attributed to.
func (c Completion) Location() string { return c.location }

// ProcessType returns the upper-cased process step of the task.
func (c Completion) ProcessType() string { return c.processType }

// Minute returns the UTC minute bucket the completion falls in.
func (c Completion) Minute() time.Time { return c.minute }

// At returns the exact completion instant (UTC).
func (c Completion) At() time.Time { return c.at }

// DurationSeconds returns the task duration and whether the event carried one.
func (c Completion) DurationSeconds() (int64, bool) { return c.durationSeconds, c.hasDuration }

// CountsIn reports whether the completion's minute bucket lies in the
// half-open window [start, end): bucket_start >= start AND bucket_start < end
// (docs/adr/0017 decision 8: the resolution is one minute).
func (c Completion) CountsIn(start, end time.Time) bool {
	return c.minute.Compare(start) >= 0 && c.minute.Compare(end) < 0
}

// Throughput is what was observed for one site and process step over one
// window.
type Throughput struct {
	// Tasks is the number of completed tasks.
	Tasks int64
	// DurationSeconds sums the durations of the TasksWithDuration tasks that
	// carried one.
	DurationSeconds   int64
	TasksWithDuration int64
	// AsOf is the newest completion time the model reflects for the step.
	AsOf time.Time
}

// HasData reports whether at least one task was observed. A window without
// completions is "no data", never "zero throughput".
func (t Throughput) HasData() bool { return t.Tasks > 0 }

// TasksPerHour returns the observed tasks per hour over the window; 0 for a
// non-positive window.
func (t Throughput) TasksPerHour(window time.Duration) float64 {
	if window <= 0 {
		return 0
	}
	return float64(t.Tasks) / window.Hours()
}

// MeanTaskSeconds returns the mean duration of the tasks that carried one,
// and false when none did.
func (t Throughput) MeanTaskSeconds() (float64, bool) {
	if t.TasksWithDuration == 0 {
		return 0, false
	}
	return float64(t.DurationSeconds) / float64(t.TasksWithDuration), true
}

// Sum aggregates the completions of ONE site and process step that fall in
// [start, end). It is the reference implementation the in-memory repository
// uses; the Postgres adapter computes the same figures in SQL and is checked
// against the same boundary cases. AsOf is the newest completion of the whole
// input, in or out of the window.
func Sum(completions []Completion, start, end time.Time) Throughput {
	var t Throughput
	for _, c := range completions {
		if c.at.After(t.AsOf) {
			t.AsOf = c.at
		}
		if !c.CountsIn(start, end) {
			continue
		}
		t.Tasks++
		if c.hasDuration {
			t.DurationSeconds += c.durationSeconds
			t.TasksWithDuration++
		}
	}
	return t
}

// Variance is the advisory planned-versus-observed comparison of one plan
// window (docs/adr/0017 decision 9).
type Variance struct {
	// HasData is false when no task of the step was observed in the window.
	HasData bool
	// Tasks and TasksPerHour are the observed throughput, in TASKS.
	Tasks        int64
	TasksPerHour float64
	// MeanTaskSeconds is nil when no observed task carried a duration.
	MeanTaskSeconds *float64
	// AsOf is the newest completion time the model reflects for the step;
	// the zero time when nothing was ever observed.
	AsOf time.Time

	// Comparable is true only when the observed tasks could be converted to
	// ORDERS with a supplied tasks_per_order; Reason says why not otherwise.
	Comparable bool
	Reason     string

	// The fields below are nil unless Comparable.
	TasksPerOrder *float64
	// ObservedOrders = tasks / tasks_per_order.
	ObservedOrders *float64
	// VarianceOrders = ObservedOrders - planned orders over the window.
	VarianceOrders *float64
	// VariancePercent is VarianceOrders as a percentage of the planned
	// orders over the window; nil when the plan assumed none.
	VariancePercent *float64
}

// The reasons a variance is not computed.
const (
	ReasonNoData        = "no task completions observed for this step in the window"
	ReasonNoTasksPerOrd = "tasks_per_order was not provided: observed throughput is in TASKs and the plan is in ORDERs"
)

// Compare builds the Variance of observed t against a plan that assumed
// plannedOverWindow orders over window. tasksPerOrder is the caller's
// conversion factor from tasks to orders (nil when not supplied); a supplied
// one must be a positive finite number (ErrNonPositiveTasksPerOrder) even
// when there is no data, so a bad request is rejected consistently.
func Compare(plannedOverWindow float64, window time.Duration, t Throughput, tasksPerOrder *float64) (Variance, error) {
	if tasksPerOrder != nil && !(*tasksPerOrder > 0 && !math.IsInf(*tasksPerOrder, 0)) {
		return Variance{}, ErrNonPositiveTasksPerOrder
	}
	v := Variance{
		HasData:      t.HasData(),
		Tasks:        t.Tasks,
		TasksPerHour: t.TasksPerHour(window),
		AsOf:         t.AsOf,
	}
	if mean, ok := t.MeanTaskSeconds(); ok {
		v.MeanTaskSeconds = &mean
	}
	if !v.HasData {
		v.Reason = ReasonNoData
		return v, nil
	}
	if tasksPerOrder == nil {
		v.Reason = ReasonNoTasksPerOrd
		return v, nil
	}
	factor := *tasksPerOrder
	orders := float64(t.Tasks) / factor
	diff := orders - plannedOverWindow
	v.Comparable, v.TasksPerOrder, v.ObservedOrders, v.VarianceOrders = true, &factor, &orders, &diff
	if plannedOverWindow > 0 {
		pct := diff / plannedOverWindow * 100
		v.VariancePercent = &pct
	}
	return v, nil
}
