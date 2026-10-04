package http

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// handleCreateCapacityPlan backs POST /capacity-plans: evaluates a
// ProcessPath against the demand assigned to a location and window and
// stores the resulting DRAFT plan (201). The demand arrives in the body --
// a documented Phase 4 simplification (see CreateCapacityPlanCommand);
// every step's registered ProcessCapacity window must COVER
// [window_start, window_end) (docs/adr/0003).
func (s *Server) handleCreateCapacityPlan(w http.ResponseWriter, r *http.Request) {
	var req createCapacityPlanRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	windowStart, err := time.Parse(time.RFC3339, req.WindowStart)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-window-start", "window_start must be an RFC3339 timestamp"}, err.Error(), r.URL.Path)
		return
	}
	windowEnd, err := time.Parse(time.RFC3339, req.WindowEnd)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-window-end", "window_end must be an RFC3339 timestamp"}, err.Error(), r.URL.Path)
		return
	}
	// assigned_demand is OPTIONAL (docs/adr/0004): an explicit value always
	// wins and behaves exactly as before; when it is omitted the use case
	// defaults it from the expected-demand read model, or answers 422
	// missing-assigned-demand when there is no order data -- never a silent 0.
	cmd := usecases.CreateCapacityPlanCommand{
		WarehouseID:      req.WarehouseID,
		Location:         req.Location,
		WindowStart:      windowStart,
		WindowEnd:        windowEnd,
		ProcessPathID:    req.PathID,
		DemandFromOrders: req.AssignedDemand == nil,
		UnitsPerOrder:    req.UnitsPerOrder,
		PackagesPerOrder: req.PackagesPerOrder,
	}
	if req.AssignedDemand != nil {
		cmd.AssignedDemand = *req.AssignedDemand
	}

	plan, err := s.CreateCapacityPlan.Handle(r.Context(), cmd)
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, toCapacityPlanResponse(plan))
}

// handlePublishCapacityPlan backs POST /capacity-plans/{id}/publish: 200
// with the PUBLISHED plan, 404 for an unknown id, 409 if it was already
// published.
func (s *Server) handlePublishCapacityPlan(w http.ResponseWriter, r *http.Request) {
	plan, err := s.PublishCapacityPlan.Handle(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toCapacityPlanResponse(plan))
}

// handleGetCapacityPlan backs GET /capacity-plans/{id}. Like the
// ProcessCapacity read it is a direct repository lookup (no invariant).
func (s *Server) handleGetCapacityPlan(w http.ResponseWriter, r *http.Request) {
	plan, err := s.CapacityPlans.FindByID(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	if plan == nil {
		writeError(w, r, usecases.ErrCapacityPlanNotFound)
		return
	}
	writeJSON(w, http.StatusOK, toCapacityPlanResponse(plan))
}

// handleListCapacityPlans backs GET /capacity-plans?location=&limit=: the most
// recently created plans, newest first, each in GET /capacity-plans/{id}'s
// shape. location is optional (omitted = every location, and the location
// field of the body is omitted too). limit defaults to 20 and is capped at
// 100; a limit that is not a positive integer is a 400 malformed-limit. 200
// with an empty list when nothing matches. The route exists only when the
// ListCapacityPlans use case is wired.
func (s *Server) handleListCapacityPlans(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	location := q.Get("location")

	limit := 0 // 0 = the use case's default
	if raw := q.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 {
			writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-limit", "limit must be a positive integer"}, "limit must be a positive integer (at most "+strconv.Itoa(usecases.MaxCapacityPlanListLimit)+")", r.URL.Path)
			return
		}
		limit = parsed
	}

	plans, err := s.ListCapacityPlans.Handle(r.Context(), location, limit)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]capacityPlanResponse, 0, len(plans))
	for _, p := range plans {
		out = append(out, toCapacityPlanResponse(p))
	}
	writeJSON(w, http.StatusOK, capacityPlansResponse{Location: location, CapacityPlans: out})
}

func toCapacityPlanResponse(p *capacityplan.CapacityPlan) capacityPlanResponse {
	resp := capacityPlanResponse{
		ID:                 p.ID(),
		WarehouseID:        p.WarehouseID(),
		Location:           p.Location(),
		WindowStart:        p.Window().Start().UTC().Format(time.RFC3339),
		WindowEnd:          p.Window().End().UTC().Format(time.RFC3339),
		PathID:             p.ProcessPathID(),
		AssignedDemand:     p.AssignedDemand(),
		DemandSource:       string(p.DemandSource()),
		Status:             string(p.Status()),
		PathCapacity:       p.PathCapacity(),
		BottleneckStep:     string(p.BottleneckStep()),
		CapacityOverWindow: p.CapacityOverWindow(),
		Shortage:           p.Shortage(),
		CreatedAt:          p.CreatedAt().UTC().Format(time.RFC3339),

		BottleneckConstraint: string(p.BottleneckConstraint()),
		Warnings:             nonNilStrings(p.Warnings()),
	}
	if !p.PublishedAt().IsZero() {
		publishedAt := p.PublishedAt().UTC().Format(time.RFC3339)
		resp.PublishedAt = &publishedAt
	}
	return resp
}
