package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// handleCreateCapacityPlan backs POST /capacity-plans: evaluates a
// ProcessPath against the demand assigned to a location and window and
// stores the resulting DRAFT plan (201). The demand arrives in the body --
// a documented Phase 4 simplification (see CreateCapacityPlanCommand);
// every step's ProcessCapacity must have been registered for exactly
// [window_start, window_end), as in Phase 2.
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
	if req.AssignedDemand == nil {
		writeProblem(w, http.StatusUnprocessableEntity, problemInfo{"missing-assigned-demand", "assigned_demand is required"}, "assigned_demand (orders) must be provided", r.URL.Path)
		return
	}

	plan, err := s.CreateCapacityPlan.Handle(r.Context(), usecases.CreateCapacityPlanCommand{
		WarehouseID:      req.WarehouseID,
		Location:         req.Location,
		WindowStart:      windowStart,
		WindowEnd:        windowEnd,
		ProcessPathID:    req.PathID,
		AssignedDemand:   *req.AssignedDemand,
		UnitsPerOrder:    req.UnitsPerOrder,
		PackagesPerOrder: req.PackagesPerOrder,
	})
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

func toCapacityPlanResponse(p *capacityplan.CapacityPlan) capacityPlanResponse {
	resp := capacityPlanResponse{
		ID:                 p.ID(),
		WarehouseID:        p.WarehouseID(),
		Location:           p.Location(),
		WindowStart:        p.Window().Start().UTC().Format(time.RFC3339),
		WindowEnd:          p.Window().End().UTC().Format(time.RFC3339),
		PathID:             p.ProcessPathID(),
		AssignedDemand:     p.AssignedDemand(),
		Status:             string(p.Status()),
		PathCapacity:       p.PathCapacity(),
		BottleneckStep:     string(p.BottleneckStep()),
		CapacityOverWindow: p.CapacityOverWindow(),
		Shortage:           p.Shortage(),
		CreatedAt:          p.CreatedAt().UTC().Format(time.RFC3339),
	}
	if !p.PublishedAt().IsZero() {
		publishedAt := p.PublishedAt().UTC().Format(time.RFC3339)
		resp.PublishedAt = &publishedAt
	}
	return resp
}
