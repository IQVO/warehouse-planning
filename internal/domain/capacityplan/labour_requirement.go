package capacityplan

import (
	"errors"
	"math"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// secondsPerHour converts a standard's seconds into labour hours.
const secondsPerHour = 3600

// PickStep is the process step whose tasks are order lines: a PICK task is
// one order line (docs/adr/0017), so when the demand came from the order read
// model a PICK step's tasks are the released lines of the counted orders
// (docs/adr/0022 decision 5).
const PickStep processcapacity.ProcessType = "PICK"

// ErrNonPositiveTasksPerOrder is returned when a tasks-per-order factor is not
// a positive, finite number.
var ErrNonPositiveTasksPerOrder = errors.New("capacityplan: tasks_per_order must be a positive number for every process type")

// LabourReason says why a step's labour requirement is not known.
type LabourReason string

// The two reasons a requirement is unknown; never a guessed number.
const (
	// ReasonNoLaborStandard: no labor-performance standard of the step's task
	// type is in force for the plan's site and window (or the step is not a
	// task type labor-performance measures, e.g. REBIN).
	ReasonNoLaborStandard LabourReason = "no-labor-standard"
	// ReasonNoTaskFactor: a standard exists, but the step's tasks cannot be
	// counted: no tasks_per_order factor for it (and, for PICK, the demand was
	// not read from the order read model).
	ReasonNoTaskFactor LabourReason = "no-task-factor"
)

// LabourRequirement is the labour one step of a plan's path requires
// (docs/adr/0022 decision 5): tasks x the engineered standard seconds of the
// step's task type. Informational, like the inbound labour of docs/adr/0017:
// it never changes the ORDER shortage or the bottleneck, and no published
// event carries it. Unknown (Known false, Reason set) when no standard or no
// task count exists -- the figures are then zero, never guessed.
type LabourRequirement struct {
	ProcessType processcapacity.ProcessType
	// StandardSeconds is the expected seconds of the standard in force; 0 when
	// there is none.
	StandardSeconds float64
	// StandardSiteScoped is true when that standard is the plan site's own
	// rather than the fleet-wide one.
	StandardSiteScoped bool
	// Tasks is the number of tasks the step's demand is.
	Tasks float64
	// RequiredLabourHours = Tasks x StandardSeconds / 3600.
	RequiredLabourHours float64
	// RequiredAverageHeads = RequiredLabourHours / window hours: the average
	// number of people the step needs over the plan window.
	RequiredAverageHeads float64
	Known                bool
	// Reason is empty when Known.
	Reason LabourReason
}

// StepStandard is the labour standard in force for one step, as the
// application read it from the local copy of labor-performance's standards.
type StepStandard struct {
	// Found is false when no standard applies.
	Found      bool
	Seconds    float64
	SiteScoped bool
}

// LabourDemand is the demand a plan's labour requirement is derived from.
type LabourDemand struct {
	// AssignedDemand is the plan's demand, in orders.
	AssignedDemand float64
	// Source is where AssignedDemand came from.
	Source DemandSource
	// ReleasedLines is the sum of the released lines of the orders counted
	// as the demand; meaningful only when Source is DemandSourceOrders.
	ReleasedLines int
	// TasksPerOrder is the request's optional tasks-per-order factor per
	// process type (validated by ValidateTasksPerOrder).
	TasksPerOrder map[processcapacity.ProcessType]float64
}

// ValidateTasksPerOrder reports ErrNonPositiveTasksPerOrder when any factor
// is zero, negative or not finite. A nil or empty map is valid: every step
// then has no factor.
func ValidateTasksPerOrder(factors map[processcapacity.ProcessType]float64) error {
	for _, factor := range factors {
		if !(factor > 0) || math.IsInf(factor, 0) {
			return ErrNonPositiveTasksPerOrder
		}
	}
	return nil
}

// RequireLabour computes the labour requirement of one path step over window
// (docs/adr/0022 decisions 4 and 5): unknown with ReasonNoLaborStandard
// without a standard; otherwise the step's tasks are the released lines of the
// counted orders for PICK when the demand came from the order read model, and
// AssignedDemand x TasksPerOrder[step] for every other case; unknown with
// ReasonNoTaskFactor when that factor is missing.
func RequireLabour(step processcapacity.ProcessType, standard StepStandard, demand LabourDemand, window processcapacity.CapacityWindow) LabourRequirement {
	r := LabourRequirement{ProcessType: step}
	if !standard.Found {
		r.Reason = ReasonNoLaborStandard
		return r
	}
	r.StandardSeconds, r.StandardSiteScoped = standard.Seconds, standard.SiteScoped
	tasks, ok := demand.tasksOf(step)
	if !ok {
		r.Reason = ReasonNoTaskFactor
		return r
	}
	r.Known = true
	r.Tasks = tasks
	r.RequiredLabourHours = tasks * standard.Seconds / secondsPerHour
	// A zero window (never built by NewCapacityWindow) gives 0 heads, never
	// Inf/NaN, like InboundAverageHeads.
	if hours := window.Duration().Hours(); hours > 0 {
		r.RequiredAverageHeads = r.RequiredLabourHours / hours
	}
	return r
}

// tasksOf returns the tasks of one step and whether they can be counted.
func (d LabourDemand) tasksOf(step processcapacity.ProcessType) (float64, bool) {
	if step == PickStep && d.Source == DemandSourceOrders {
		return float64(d.ReleasedLines), true
	}
	factor, ok := d.TasksPerOrder[step]
	if !ok {
		return 0, false
	}
	return d.AssignedDemand * factor, true
}
