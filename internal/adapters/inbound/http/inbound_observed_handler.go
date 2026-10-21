package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

// registerInboundAndObservedRoutes registers the ADR 0017 routes whose use cases
// are wired. Split from NewRouter only to keep it within the function-length
// budget.
func (s *Server) registerInboundAndObservedRoutes(r chi.Router) {
	if s.DeclareInboundLabourStandard != nil {
		r.Put("/inbound-labour-standards/{location}/{path_id}", s.handleDeclareInboundLabourStandard)
	}
	if s.InboundStandards != nil {
		r.Get("/inbound-labour-standards", s.handleListInboundLabourStandards)
	}
	if s.GetInboundDemand != nil {
		r.Get("/inbound-demand", s.handleGetInboundDemand)
	}
	if s.GetCapacityPlanVariance != nil {
		r.Get("/capacity-plans/{id}/variance", s.handleGetCapacityPlanVariance)
	}
}

// declareInboundLabourStandardRequest is PUT /inbound-labour-standards/
// {location}/{path_id}'s body (docs/adr/0017 decision 2). minutes_per_asn is
// optional (omitted means 0).
type declareInboundLabourStandardRequest struct {
	MinutesPerAppointment float64 `json:"minutes_per_appointment"`
	MinutesPerASN         float64 `json:"minutes_per_asn"`
}

// inboundLabourStandardResponse is one declared inbound.LabourStandard.
type inboundLabourStandardResponse struct {
	Location              string  `json:"location"`
	PathID                string  `json:"path_id"`
	MinutesPerAppointment float64 `json:"minutes_per_appointment"`
	MinutesPerASN         float64 `json:"minutes_per_asn"`
}

// inboundLabourStandardsResponse is GET /inbound-labour-standards' body.
type inboundLabourStandardsResponse struct {
	Location  string                          `json:"location,omitempty"`
	Standards []inboundLabourStandardResponse `json:"standards"`
}

// inboundDemandSourceInboundReceiving labels where GET /inbound-demand's
// figures come from: the read model fed by inbound-receiving's dock
// appointment events.
const inboundDemandSourceInboundReceiving = "inbound-receiving"

// inboundDemandResponse is GET /inbound-demand's body. appointments counts the
// BOOKED dock appointments whose door window STARTS in [window_start,
// window_end); asns sums the ASNs they cover; labour_minutes is what the
// standard declared for (location, path_id) attributes to them (0 when none is
// declared -- never a guessed default); average_heads is labour_minutes over
// the window length; standard_missing is true when appointments exist but no
// standard is declared for the path; as_of is the CloudEvents time of the
// newest dock-appointment event the site's model reflects (null when none).
type inboundDemandResponse struct {
	Location         string  `json:"location"`
	PathID           string  `json:"path_id"`
	WindowStart      string  `json:"window_start"`
	WindowEnd        string  `json:"window_end"`
	Appointments     int     `json:"appointments"`
	ASNs             int     `json:"asns"`
	LabourMinutes    float64 `json:"labour_minutes"`
	AverageHeads     float64 `json:"average_heads"`
	StandardDeclared bool    `json:"standard_declared"`
	StandardMissing  bool    `json:"standard_missing"`
	Source           string  `json:"source"`
	AsOf             *string `json:"as_of"`
}

func toInboundLabourStandardResponse(s inbound.LabourStandard) inboundLabourStandardResponse {
	return inboundLabourStandardResponse{
		Location: s.Location(), PathID: s.ProcessPathID(),
		MinutesPerAppointment: s.MinutesPerAppointment(), MinutesPerASN: s.MinutesPerASN(),
	}
}

func toInboundDemandResponse(location, pathID string, start, end time.Time, r usecases.InboundDemandResult) inboundDemandResponse {
	resp := inboundDemandResponse{
		Location: location, PathID: pathID,
		WindowStart:      start.UTC().Format(time.RFC3339),
		WindowEnd:        end.UTC().Format(time.RFC3339),
		Appointments:     r.Demand.Appointments,
		ASNs:             r.Demand.ASNs,
		LabourMinutes:    r.LabourMinutes,
		AverageHeads:     r.AverageHeads,
		StandardDeclared: r.StandardDeclared,
		StandardMissing:  r.StandardMissing,
		Source:           inboundDemandSourceInboundReceiving,
	}
	if !r.Demand.AsOf.IsZero() {
		asOf := r.Demand.AsOf.UTC().Format(time.RFC3339)
		resp.AsOf = &asOf
	}
	return resp
}

// handleDeclareInboundLabourStandard backs PUT /inbound-labour-standards/
// {location}/{path_id}: the operator declares the labour minutes a dock
// appointment costs at a site for a process path. 201 when the standard is new,
// 200 when it replaced an existing one, 422 when invalid. Nothing is ever
// defaulted: without a standard, appointments contribute no demand and say so
// (docs/adr/0017 decision 6).
func (s *Server) handleDeclareInboundLabourStandard(w http.ResponseWriter, r *http.Request) {
	var req declareInboundLabourStandardRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	standard, created, err := s.DeclareInboundLabourStandard.Handle(r.Context(), usecases.DeclareInboundLabourStandardCommand{
		Location:              chi.URLParam(r, "location"),
		ProcessPathID:         chi.URLParam(r, "path_id"),
		MinutesPerAppointment: req.MinutesPerAppointment,
		MinutesPerASN:         req.MinutesPerASN,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, toInboundLabourStandardResponse(standard))
}

// handleListInboundLabourStandards backs GET /inbound-labour-standards?
// location=: the declared standards of one site, or every standard when
// location is absent.
func (s *Server) handleListInboundLabourStandards(w http.ResponseWriter, r *http.Request) {
	location := r.URL.Query().Get("location")
	standards, err := s.InboundStandards.List(r.Context(), location)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]inboundLabourStandardResponse, 0, len(standards))
	for _, standard := range standards {
		out = append(out, toInboundLabourStandardResponse(standard))
	}
	writeJSON(w, http.StatusOK, inboundLabourStandardsResponse{Location: location, Standards: out})
}

// handleGetInboundDemand backs GET /inbound-demand?location=&path_id=&
// window_start=&window_end=: the inbound dock-appointment demand of a site over
// a half-open window, priced with the standard declared for the path, read from
// the local read model (docs/adr/0017). 200 even when nothing is known:
// appointments is 0 and as_of is null.
func (s *Server) handleGetInboundDemand(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	location, pathID := q.Get("location"), q.Get("path_id")
	if location == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-location", "location is required"}, "the location (site code) query parameter must be provided", r.URL.Path)
		return
	}
	if pathID == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-path-id", "path_id is required"}, "the path_id query parameter (the process path whose inbound standard prices the appointments) must be provided", r.URL.Path)
		return
	}
	start, err := time.Parse(time.RFC3339, q.Get("window_start"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-window-start", "window_start must be an RFC3339 timestamp"}, err.Error(), r.URL.Path)
		return
	}
	end, err := time.Parse(time.RFC3339, q.Get("window_end"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-window-end", "window_end must be an RFC3339 timestamp"}, err.Error(), r.URL.Path)
		return
	}
	result, err := s.GetInboundDemand.Handle(r.Context(), location, pathID, start, end)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toInboundDemandResponse(location, pathID, start, end, result))
}

// planVarianceResponse is GET /capacity-plans/{id}/variance's body: the
// ADVISORY planned-versus-observed comparison of the plan's bottleneck step over
// its window (docs/adr/0017 decision 9). Nothing here changes the plan.
type planVarianceResponse struct {
	PlanID                  string             `json:"plan_id"`
	Location                string             `json:"location"`
	Step                    string             `json:"step"`
	WindowStart             string             `json:"window_start"`
	WindowEnd               string             `json:"window_end"`
	PlannedOrdersPerHour    float64            `json:"planned_orders_per_hour"`
	PlannedOrdersOverWindow float64            `json:"planned_orders_over_window"`
	Observed                observedResponse   `json:"observed"`
	Comparison              comparisonResponse `json:"comparison"`
	Advisory                bool               `json:"advisory"`
}

// observedResponse is the observed side: completed TASKS of the step in the
// window, from fulfillment-execution's TaskCompleted events at minute
// resolution. has_data false means nothing was observed -- never zero
// throughput.
type observedResponse struct {
	Source          string   `json:"source"`
	HasData         bool     `json:"has_data"`
	Tasks           int64    `json:"tasks"`
	TasksPerHour    float64  `json:"tasks_per_hour"`
	MeanTaskSeconds *float64 `json:"mean_task_seconds"`
	AsOf            *string  `json:"as_of"`
}

// comparisonResponse converts the observed tasks to orders with the caller's
// tasks_per_order. comparable false carries the reason and null figures.
type comparisonResponse struct {
	Comparable      bool     `json:"comparable"`
	Reason          string   `json:"reason,omitempty"`
	TasksPerOrder   *float64 `json:"tasks_per_order"`
	ObservedOrders  *float64 `json:"observed_orders"`
	VarianceOrders  *float64 `json:"variance_orders"`
	VariancePercent *float64 `json:"variance_percent"`
}

// observedSourceFulfillment labels where the observed figures come from.
const observedSourceFulfillment = "fulfillment-execution"

func toPlanVarianceResponse(v usecases.PlanVariance) planVarianceResponse {
	resp := planVarianceResponse{
		PlanID: v.PlanID, Location: v.Location, Step: v.Step,
		WindowStart:             v.WindowStart.UTC().Format(time.RFC3339),
		WindowEnd:               v.WindowEnd.UTC().Format(time.RFC3339),
		PlannedOrdersPerHour:    v.PlannedPerHour,
		PlannedOrdersOverWindow: v.PlannedOverWindow,
		Observed: observedResponse{
			Source: observedSourceFulfillment, HasData: v.HasData, Tasks: v.Tasks,
			TasksPerHour: v.TasksPerHour, MeanTaskSeconds: v.MeanTaskSeconds,
		},
		Comparison: comparisonResponse{
			Comparable: v.Comparable, Reason: v.Reason, TasksPerOrder: v.TasksPerOrder,
			ObservedOrders: v.ObservedOrders, VarianceOrders: v.VarianceOrders, VariancePercent: v.VariancePercent,
		},
		Advisory: true,
	}
	if !v.AsOf.IsZero() {
		asOf := v.AsOf.UTC().Format(time.RFC3339)
		resp.Observed.AsOf = &asOf
	}
	return resp
}

// handleGetCapacityPlanVariance backs GET /capacity-plans/{id}/variance
// [?tasks_per_order=]. 404 for an unknown plan, 400 for a tasks_per_order that
// is not a number, 422 for one that is not positive. Without tasks_per_order the
// observed side is still reported and the comparison says why it is not
// computed.
func (s *Server) handleGetCapacityPlanVariance(w http.ResponseWriter, r *http.Request) {
	var tasksPerOrder *float64
	if raw := r.URL.Query().Get("tasks_per_order"); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-tasks-per-order", "tasks_per_order must be a number"}, err.Error(), r.URL.Path)
			return
		}
		tasksPerOrder = &parsed
	}
	variance, err := s.GetCapacityPlanVariance.Handle(r.Context(), chi.URLParam(r, "id"), tasksPerOrder)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toPlanVarianceResponse(variance))
}
