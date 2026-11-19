package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/adapters/inbound/http/page"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// Problem categories of the labour-plan request that the adapter itself
// detects.
var (
	problemMalformedShiftStart      = problemInfo{slug: "malformed-shift-start", title: "shift_start must be an RFC3339 timestamp"}
	problemMissingInstalledStations = problemInfo{slug: "missing-installed-stations", title: "installed_stations is required"}
	problemMissingSiteID            = problemInfo{slug: "missing-site-id", title: "site_id is required"}
	problemMissingShiftID           = problemInfo{slug: "missing-shift-id", title: "shift_id is required"}
)

// labourPlanListFilters are the declared filters of GET /labour-plans.
var labourPlanListFilters = []string{"site_id", "shift_id"}

// missingListFilterDetail is the detail of a GET /labour-plans without one
// of its two required filters.
const missingListFilterDetail = "the site_id and shift_id query parameters must both be provided (repeat them on every page request)"

// commitLabourPlanRequest is PUT /labour-plans/{site_id}/{shift_id}/
// {path_id}'s body (docs/adr/0023 decision 2). planned_heads and shift_start
// are optional; an omitted planned_heads is derived from the stored workload
// forecast, which needs shift_start. installed_stations is a pointer so an
// absent one is refused instead of read as zero stations.
type commitLabourPlanRequest struct {
	PlannedHeads      *int    `json:"planned_heads"`
	RateUnitsPerHour  float64 `json:"rate_units_per_hour"`
	Hours             float64 `json:"hours"`
	InstalledStations *int    `json:"installed_stations"`
	ShiftStart        string  `json:"shift_start"`
}

// labourPlanResponse is one stored plan: every field of the plan, where its
// heads came from, its version and the advisory warnings of this read
// (never null).
type labourPlanResponse struct {
	SiteID            string   `json:"site_id"`
	ShiftID           string   `json:"shift_id"`
	PathID            string   `json:"path_id"`
	PlannedHeads      int      `json:"planned_heads"`
	RateUnitsPerHour  float64  `json:"rate_units_per_hour"`
	Hours             float64  `json:"hours"`
	InstalledStations int      `json:"installed_stations"`
	ShiftStart        *string  `json:"shift_start,omitempty"`
	HeadsSource       string   `json:"heads_source"`
	Version           int64    `json:"version"`
	Warnings          []string `json:"warnings"`
}

// handleCommitLabourPlan backs PUT /labour-plans/{site_id}/{shift_id}/
// {path_id}: 201 with version 1 for a first commit, 200 with version + 1 for
// a revision and 200 with the stored plan and version unchanged (no event)
// when the plan equals the stored one. 400 for a malformed body or
// shift_start; 422 for a plan the aggregate refuses (heads-exceed-stations
// among them), for missing-installed-stations and for missing-planned-heads
// (omitted heads that cannot be derived); 409 when the stored version moved
// twice under the request.
func (s *Server) handleCommitLabourPlan(w http.ResponseWriter, r *http.Request) {
	var req commitLabourPlanRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	cmd, ok := toCommitLabourPlanCommand(w, r, req)
	if !ok {
		return
	}
	res, err := s.CommitLabourPlan.Handle(r.Context(), cmd)
	if err != nil {
		writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if res.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, toLabourPlanResponse(res.LabourPlanView))
}

// handleGetLabourPlan backs GET /labour-plans/{site_id}/{shift_id}/{path_id}:
// 200 with the stored plan, 404 labour-plan-not-found.
func (s *Server) handleGetLabourPlan(w http.ResponseWriter, r *http.Request) {
	view, err := s.GetLabourPlan.Handle(r.Context(), chi.URLParam(r, "site_id"), chi.URLParam(r, "shift_id"), chi.URLParam(r, "path_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toLabourPlanResponse(view))
}

// handleListLabourPlans backs GET /labour-plans?site_id=&shift_id=: the plans
// of one (site, shift) ordered by path id, one keyset page at a time
// ({items,nextCursor}). Both filters are required (400 missing-site-id /
// missing-shift-id) and are repeated on every page request.
func (s *Server) handleListLabourPlans(w http.ResponseWriter, r *http.Request) {
	limit, after, ok := pageParams(w, r, labourPlanListFilters...)
	if !ok {
		return
	}
	q := r.URL.Query()
	siteID, shiftID := q.Get("site_id"), q.Get("shift_id")
	if siteID == "" {
		writeProblem(w, http.StatusBadRequest, problemMissingSiteID, missingListFilterDetail, r.URL.Path)
		return
	}
	if shiftID == "" {
		writeProblem(w, http.StatusBadRequest, problemMissingShiftID, missingListFilterDetail, r.URL.Path)
		return
	}
	afterPathID := ""
	if after != nil {
		afterPathID = after.ID
	}
	views, err := s.ListLabourPlans.Handle(r.Context(), siteID, shiftID, afterPathID, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]labourPlanResponse, 0, len(views))
	for _, v := range views {
		out = append(out, toLabourPlanResponse(v))
	}
	writeJSON(w, http.StatusOK, page.Of(out, limit, func(p labourPlanResponse) page.Position {
		return page.Position{Sort: p.PathID, ID: p.PathID}
	}))
}

// toCommitLabourPlanCommand maps the request onto the use-case command,
// answering the problems only the adapter can see: a malformed shift_start
// (400) before an absent installed_stations (422).
func toCommitLabourPlanCommand(w http.ResponseWriter, r *http.Request, req commitLabourPlanRequest) (usecases.CommitLabourPlanCommand, bool) {
	var shiftStart *time.Time
	if req.ShiftStart != "" {
		start, err := time.Parse(time.RFC3339, req.ShiftStart)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, problemMalformedShiftStart, err.Error(), r.URL.Path)
			return usecases.CommitLabourPlanCommand{}, false
		}
		shiftStart = &start
	}
	if req.InstalledStations == nil {
		writeProblem(w, http.StatusUnprocessableEntity, problemMissingInstalledStations, "installed_stations (the stations the path has for this shift) must be provided", r.URL.Path)
		return usecases.CommitLabourPlanCommand{}, false
	}
	return usecases.CommitLabourPlanCommand{
		SiteID:            chi.URLParam(r, "site_id"),
		ShiftID:           chi.URLParam(r, "shift_id"),
		PathID:            chi.URLParam(r, "path_id"),
		PlannedHeads:      req.PlannedHeads,
		RateUnitsPerHour:  req.RateUnitsPerHour,
		Hours:             req.Hours,
		InstalledStations: *req.InstalledStations,
		ShiftStart:        shiftStart,
	}, true
}

func toLabourPlanResponse(v usecases.LabourPlanView) labourPlanResponse {
	s := v.Plan.Spec()
	resp := labourPlanResponse{
		SiteID:            s.SiteID,
		ShiftID:           s.ShiftID,
		PathID:            s.PathID,
		PlannedHeads:      s.PlannedHeads,
		RateUnitsPerHour:  s.RateUnitsPerHour,
		Hours:             s.Hours,
		InstalledStations: s.InstalledStations,
		HeadsSource:       string(s.HeadsSource),
		Version:           v.Plan.Version(),
		Warnings:          nonNilStrings(v.Warnings),
	}
	if s.ShiftStart != nil {
		start := s.ShiftStart.UTC().Format(time.RFC3339)
		resp.ShiftStart = &start
	}
	return resp
}
