package http

import (
	"net/http"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/inbound/http/page"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// handleCreateCapacityPlan backs POST /capacity-plans: evaluates a
// ProcessPath against the demand assigned to a location and window and
// stores the resulting DRAFT plan (201). assigned_demand is OPTIONAL
// (docs/adr/0004): an explicit value in the body always wins; when
// omitted, CreateCapacityPlanCommand resolves it from the expected-demand
// read model instead (see createCapacityPlanRequest below). Every step's
// registered ProcessCapacity window must COVER [window_start, window_end)
// (docs/adr/0003).
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
		SiteID:           req.SiteID,
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

// handlePublishCapacityPlan backs POST /capacity-plans/{id}/publications: 200
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

// handleListCapacityPlans backs GET /capacity-plans?location=: the most
// recently created plans, newest first, each in GET /capacity-plans/{id}'s
// shape, one keyset page at a time ({items,nextCursor}; ordered by created_at
// then id, both descending). location is optional (omitted = every location)
// and is repeated on every page request. 200 with an empty items list when
// nothing matches. The route exists only when the ListCapacityPlans use case
// is wired.
func (s *Server) handleListCapacityPlans(w http.ResponseWriter, r *http.Request) {
	limit, after, ok := pageParams(w, r, "location")
	if !ok {
		return
	}
	plans, err := s.ListCapacityPlans.Handle(r.Context(), r.URL.Query().Get("location"), after, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	// Plans are rows of the page; the cursor needs the creation instant at full
	// precision, so it is built from the aggregate, not the response string.
	out := make([]capacityPlanItem, 0, len(plans))
	for _, p := range plans {
		out = append(out, capacityPlanItem{resp: toCapacityPlanResponse(p), createdAt: p.CreatedAt().UTC().Format(time.RFC3339Nano)})
	}
	pg := page.Of(out, limit, func(i capacityPlanItem) page.Position {
		return page.Position{Sort: i.createdAt, ID: i.resp.ID}
	})
	items := make([]capacityPlanResponse, 0, len(pg.Items))
	for _, i := range pg.Items {
		items = append(items, i.resp)
	}
	writeJSON(w, http.StatusOK, page.Page[capacityPlanResponse]{Items: items, NextCursor: pg.NextCursor})
}

// capacityPlanItem carries a plan response with its full-precision creation
// instant, the sort key of the list cursor.
type capacityPlanItem struct {
	resp      capacityPlanResponse
	createdAt string
}

func toCapacityPlanResponse(p *capacityplan.CapacityPlan) capacityPlanResponse {
	resp := capacityPlanResponse{
		ID:                 p.ID(),
		WarehouseID:        p.WarehouseID(),
		SiteID:             p.SiteID(),
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

		InboundAppointments:    p.Inbound().Appointments,
		InboundLabourMinutes:   p.Inbound().LabourMinutes,
		InboundAverageHeads:    p.InboundAverageHeads(),
		InboundStandardMissing: p.Inbound().StandardMissing,
	}
	if !p.PublishedAt().IsZero() {
		publishedAt := p.PublishedAt().UTC().Format(time.RFC3339)
		resp.PublishedAt = &publishedAt
	}
	return resp
}
