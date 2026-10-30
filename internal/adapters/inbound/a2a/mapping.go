package a2a

import (
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// problemBaseURI is the namespace of this service's RFC 7807 "type" URIs. It
// is the SAME namespace the REST adapter uses (internal/adapters/inbound/http
// errors.go), so a delegating agent sees the identical type for the identical
// rejection whichever protocol it came through.
const problemBaseURI = "https://errors.warehouse-planning.warehouse-systems.dev/"

// problem is the RFC 7807-shaped rejection an agent client receives instead of
// a result: domain rejections and request-shape failures are business
// answers, not transport errors, so they travel in the message envelope.
type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Status int    `json:"status"`
}

// summary renders the one-line form used for the reply's text part.
func (p *problem) summary() string {
	return fmt.Sprintf("%s (%d): %s", p.Title, p.Status, p.Detail)
}

func newProblem(status int, slug, title, detail string) *problem {
	return &problem{Type: problemBaseURI + slug, Title: title, Detail: detail, Status: status}
}

func problemMalformedRequest(detail string) *problem {
	return newProblem(http.StatusBadRequest, "malformed-skill-request", "Malformed skill request", detail)
}

func problemUnknownSkill(skill string) *problem {
	return newProblem(http.StatusBadRequest, "unknown-skill", "Unknown skill",
		fmt.Sprintf("skill %q is not offered by this agent; the Agent Card lists the supported skills", skill))
}

func problemInvalidArgument(detail string) *problem {
	return newProblem(http.StatusBadRequest, "invalid-argument", "Invalid skill argument", detail)
}

// problemMalformedTimestamp mirrors the REST adapter's 400 for an unparsable
// window bound (slugs malformed-window-start / malformed-window-end).
func problemMalformedTimestamp(slug, argument string, err error) *problem {
	return newProblem(http.StatusBadRequest, slug, argument+" must be an RFC3339 timestamp", err.Error())
}

// catalogEntry pins one typed domain error to the (status, slug, title) the
// REST adapter answers it with. detail, when set, REPLACES the shared domain
// error text: used where that text names a key (assigned_demand, ...) this
// surface does not offer, so the answer never teaches an agent a name it
// would then be refused as an unknown argument.
type catalogEntry struct {
	target error
	status int
	slug   string
	title  string
	detail string
}

// problemCatalog is the subset of the REST adapter's problem catalogue
// (internal/adapters/inbound/http/errors.go) reachable from the two skills,
// with the SAME status classification: 422 for a domain validation failure,
// 404 for an unknown path, 500 for anything else. Order is irrelevant: the
// targets are distinct sentinels.
func problemCatalog() []catalogEntry {
	const unprocessable = http.StatusUnprocessableEntity
	return []catalogEntry{
		{processcapacity.ErrInvalidWindow, unprocessable, "invalid-capacity-window", "Capacity window end must be strictly after start", ""},
		{processcapacity.ErrNonPositiveConversionFactor, unprocessable, "non-positive-conversion-factor", "A WorkloadProfile conversion factor must be positive", ""},
		{processcapacity.ErrMissingConversionFactor, unprocessable, "missing-conversion-factor", "The WorkloadProfile has no conversion factor for one of the path's steps' units", ""},
		{processcapacity.ErrUnsupportedNormalizationUnit, unprocessable, "unsupported-normalization-unit", "This step's native unit cannot be normalized to ORDER", ""},
		{processcapacity.ErrMissingStepCapacity, unprocessable, "missing-step-capacity", "No registered ProcessCapacity window covers one of the path's steps at this location and window", ""},
		{usecases.ErrProcessPathNotFound, http.StatusNotFound, "process-path-not-found", "No ProcessPath is registered under this id", ""},
		{capacityplan.ErrNegativeDemand, unprocessable, "negative-assigned-demand", "Assigned demand must not be negative", "assignedDemand must not be negative"},
		{usecases.ErrMissingAssignedDemand, unprocessable, "missing-assigned-demand", "assigned demand is required",
			"assignedDemand was omitted and order-management has no expected orders at this location in the window; state assignedDemand (in ORDERS) explicitly"},
		{capacityplan.ErrRequiredField, unprocessable, "missing-required-field", "warehouseId, siteId, location and pathId are required",
			"warehouseId, siteId, location and pathId are required"},
	}
}

// problemFromUseCase maps a use case error to a problem answer, using the
// SAME classification as the REST adapter so a delegating agent sees the
// outcome a REST caller would. Domain rejections are EXPECTED business
// outcomes and are answered as RFC 7807-shaped problems; anything
// unrecognised is an internal failure, still answered as a problem -- status
// 500, infrastructure detail withheld -- so the calling agent always receives
// a structured, machine-readable result.
func problemFromUseCase(err error) *problem {
	for _, entry := range problemCatalog() {
		if !errors.Is(err, entry.target) {
			continue
		}
		detail := entry.detail
		if detail == "" {
			detail = err.Error()
		}
		return newProblem(entry.status, entry.slug, entry.title, detail)
	}
	slog.Error("a2a skill failed with an unexpected error", "error", err)
	return newProblem(http.StatusInternalServerError, "internal-error", "An unexpected internal error occurred",
		"the skill failed; see server logs (trace id in the response metadata)")
}

// envelope is the structured payload of every skill answer: exactly one of
// Data (a skill DTO) or Problem (why there is no result), plus the skill id
// the answer belongs to.
type envelope struct {
	Skill   string   `json:"skill"`
	Data    skillDTO `json:"data,omitempty"`
	Problem *problem `json:"problem,omitempty"`
}

// skillDTO is the union of the skill result DTOs. A2A parts are
// machine-readable JSON; each skill fills its own field and summary, keeping
// the envelope flat and versioned as one contract.
type skillDTO struct {
	PathCapacity *pathCapacity `json:"pathCapacity,omitempty"`
	CapacityPlan *capacityPlan `json:"capacityPlan,omitempty"`
	summary      string
}

// Summary is the human-readable one-liner for the reply's text part.
func (s skillDTO) Summary() string { return s.summary }

// pathCapacity is the DTO of the path_capacity skill: the end-to-end rate in
// ORDER per HOUR, the bottleneck and each step's own rate. Domain types never
// cross the boundary.
type pathCapacity struct {
	PathID               string          `json:"pathId"`
	Location             string          `json:"location"`
	WindowStart          string          `json:"windowStart"`
	WindowEnd            string          `json:"windowEnd"`
	NormalizedRate       float64         `json:"normalizedRate"`
	NormalizedUnit       string          `json:"normalizedUnit"`
	BottleneckStep       string          `json:"bottleneckStep"`
	BottleneckConstraint string          `json:"bottleneckConstraint"`
	Steps                []stepBreakdown `json:"steps"`
	Warnings             []string        `json:"warnings"`
	summary              string
}

type stepBreakdown struct {
	Step              string  `json:"step"`
	NormalizedRate    float64 `json:"normalizedRate"`
	BindingConstraint string  `json:"bindingConstraint"`
}

func toPathCapacity(args pathCapacityArgs, start, end time.Time, result usecases.GetProcessPathCapacityResult) pathCapacity {
	steps := make([]stepBreakdown, 0, len(result.Steps))
	for _, step := range result.Steps {
		steps = append(steps, stepBreakdown{
			Step:              string(step.Step),
			NormalizedRate:    step.Rate.Quantity() / step.Rate.Period().Hours(),
			BindingConstraint: string(step.Binding),
		})
	}
	out := pathCapacity{
		PathID:               args.PathID,
		Location:             args.Location,
		WindowStart:          start.UTC().Format(time.RFC3339),
		WindowEnd:            end.UTC().Format(time.RFC3339),
		NormalizedRate:       result.NormalizedRate.Quantity(),
		NormalizedUnit:       string(result.NormalizedRate.Unit()),
		BottleneckStep:       string(result.BottleneckStep),
		BottleneckConstraint: string(result.BottleneckConstraint),
		Steps:                steps,
		Warnings:             nonNilStrings(result.Warnings),
	}
	out.summary = fmt.Sprintf("%s at %s: %g %s per hour, bottleneck %s (%s)",
		out.PathID, out.Location, out.NormalizedRate, out.NormalizedUnit, out.BottleneckStep, out.BottleneckConstraint)
	return out
}

// capacityPlan is the DTO of the evaluate_capacity_plan skill: the recorded
// DRAFT plan. Quantities are orders; pathCapacity is ORDER per HOUR. The
// aggregate never leaks across the boundary.
type capacityPlan struct {
	PlanID               string      `json:"planId"`
	WarehouseID          string      `json:"warehouseId"`
	SiteID               string      `json:"siteId"`
	Location             string      `json:"location"`
	PathID               string      `json:"pathId"`
	WindowStart          string      `json:"windowStart"`
	WindowEnd            string      `json:"windowEnd"`
	Status               string      `json:"status"`
	AssignedDemand       float64     `json:"assignedDemand"`
	DemandSource         string      `json:"demandSource"`
	PathCapacity         float64     `json:"pathCapacity"`
	CapacityOverWindow   float64     `json:"capacityOverWindow"`
	Shortage             float64     `json:"shortage"`
	BottleneckStep       string      `json:"bottleneckStep"`
	BottleneckConstraint string      `json:"bottleneckConstraint"`
	Warnings             []string    `json:"warnings"`
	Inbound              inboundView `json:"inbound"`
	CreatedAt            string      `json:"createdAt"`
	summary              string
}

// inboundView is the informational inbound labour demand stored next to (never
// summed into) the order demand (ADR 0017).
type inboundView struct {
	Appointments    int     `json:"appointments"`
	LabourMinutes   float64 `json:"labourMinutes"`
	AverageHeads    float64 `json:"averageHeads"`
	StandardMissing bool    `json:"standardMissing"`
}

func toCapacityPlan(p *capacityplan.CapacityPlan) capacityPlan {
	out := capacityPlan{
		PlanID:               p.ID(),
		WarehouseID:          p.WarehouseID(),
		SiteID:               p.SiteID(),
		Location:             p.Location(),
		PathID:               p.ProcessPathID(),
		WindowStart:          p.Window().Start().UTC().Format(time.RFC3339),
		WindowEnd:            p.Window().End().UTC().Format(time.RFC3339),
		Status:               string(p.Status()),
		AssignedDemand:       p.AssignedDemand(),
		DemandSource:         string(p.DemandSource()),
		PathCapacity:         p.PathCapacity(),
		CapacityOverWindow:   p.CapacityOverWindow(),
		Shortage:             p.Shortage(),
		BottleneckStep:       string(p.BottleneckStep()),
		BottleneckConstraint: string(p.BottleneckConstraint()),
		Warnings:             nonNilStrings(p.Warnings()),
		Inbound: inboundView{
			Appointments:    p.Inbound().Appointments,
			LabourMinutes:   p.Inbound().LabourMinutes,
			AverageHeads:    p.InboundAverageHeads(),
			StandardMissing: p.Inbound().StandardMissing,
		},
		CreatedAt: p.CreatedAt().UTC().Format(time.RFC3339),
	}
	out.summary = fmt.Sprintf("plan %s %s: %g orders against %g capacity over the window (%g per hour), shortage %g, bottleneck %s",
		out.PlanID, out.Status, out.AssignedDemand, out.CapacityOverWindow, out.PathCapacity, out.Shortage, out.BottleneckStep)
	return out
}

// nonNilStrings returns s, or an empty (non-nil) slice, so a list field always
// serializes as [] and never null.
func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
