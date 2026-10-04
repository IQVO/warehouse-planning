package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// handleRegisterProcessPath backs POST /process-paths: seeds a ProcessPath
// read model (id/name/ordered steps). There is no event-driven sync from
// process-path-management yet (a later phase) -- this is the only way a
// ProcessPath becomes known to this service for now.
func (s *Server) handleRegisterProcessPath(w http.ResponseWriter, r *http.Request) {
	var req registerProcessPathRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	steps := make([]processpath.ProcessType, 0, len(req.Steps))
	for _, step := range req.Steps {
		steps = append(steps, processpath.ProcessType(step))
	}

	path, err := s.RegisterProcessPath.Handle(r.Context(), usecases.RegisterProcessPathCommand{
		ID:    req.ID,
		Name:  req.Name,
		Steps: steps,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, toProcessPathResponse(path))
}

func toProcessPathResponse(path processpath.ProcessPath) processPathResponse {
	steps := path.Steps()
	out := make([]string, 0, len(steps))
	for _, step := range steps {
		out = append(out, string(step))
	}
	return processPathResponse{ID: path.ID(), Name: path.Name(), Steps: out}
}

// handleGetProcessPathCapacity backs
// GET /process-paths/{id}/capacity?location=&window_start=&window_end=&units_per_order=&packages_per_order=.
// units_per_order/packages_per_order are optional query params carrying
// the WorkloadProfile's conversion factors directly on the request --
// see GetProcessPathCapacityCommand's doc comment for why (Phase 2
// simplification, no dedicated WorkloadProfile persistence yet).
func (s *Server) handleGetProcessPathCapacity(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	q := r.URL.Query()

	windowStart, err := time.Parse(time.RFC3339, q.Get("window_start"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-window-start", "window_start must be an RFC3339 timestamp"}, err.Error(), r.URL.Path)
		return
	}
	windowEnd, err := time.Parse(time.RFC3339, q.Get("window_end"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-window-end", "window_end must be an RFC3339 timestamp"}, err.Error(), r.URL.Path)
		return
	}

	unitsPerOrder, err := parseOptionalFloatQueryParam(q.Get("units_per_order"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-units-per-order", "units_per_order must be a number"}, err.Error(), r.URL.Path)
		return
	}
	packagesPerOrder, err := parseOptionalFloatQueryParam(q.Get("packages_per_order"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-packages-per-order", "packages_per_order must be a number"}, err.Error(), r.URL.Path)
		return
	}

	result, err := s.GetProcessPathCapacity.Handle(r.Context(), usecases.GetProcessPathCapacityCommand{
		ProcessPathID:    id,
		Location:         q.Get("location"),
		WindowStart:      windowStart,
		WindowEnd:        windowEnd,
		UnitsPerOrder:    unitsPerOrder,
		PackagesPerOrder: packagesPerOrder,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	breakdown := make([]stepBreakdownItem, 0, len(result.Steps))
	for _, step := range result.Steps {
		breakdown = append(breakdown, stepBreakdownItem{
			Step:              string(step.Step),
			NormalizedRate:    step.Rate.Quantity() / step.Rate.Period().Hours(),
			BindingConstraint: string(step.Binding),
		})
	}
	writeJSON(w, http.StatusOK, processPathCapacityResponse{
		NormalizedRate: result.NormalizedRate.Quantity(),
		NormalizedUnit: string(result.NormalizedRate.Unit()),
		BottleneckStep: string(result.BottleneckStep),
		StepBreakdown:  breakdown,
		Warnings:       nonNilStrings(result.Warnings),
	})
}
