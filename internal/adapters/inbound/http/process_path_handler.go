package http

import (
	"net/http"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/inbound/http/page"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// handleListProcessPaths backs GET /process-paths: the locally declared paths
// and the ACTIVE process flows (a flow wins over a local path of the same id)
// ordered by id, one keyset page at a time ({items,nextCursor}).
// 200 with an empty items list when none follows (never an error). The route
// exists only when the ListProcessPaths use case is wired.
func (s *Server) handleListProcessPaths(w http.ResponseWriter, r *http.Request) {
	limit, after, ok := pageParams(w, r)
	if !ok {
		return
	}
	paths, err := s.ListProcessPaths.Handle(r.Context(), after, page.FetchSize(limit))
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]processPathResponse, 0, len(paths))
	for _, path := range paths {
		out = append(out, toProcessPathResponse(path))
	}
	writeJSON(w, http.StatusOK, page.Of(out, limit, func(p processPathResponse) page.Position {
		return page.Position{Sort: p.ID, ID: p.ID}
	}))
}

func toProcessPathResponse(path processpath.ProcessPath) processPathResponse {
	steps := path.Steps()
	out := make([]string, 0, len(steps))
	for _, step := range steps {
		out = append(out, string(step))
	}
	return processPathResponse{ID: path.ID(), Name: path.Name(), Steps: out, Source: string(path.Source())}
}

// handleGetProcessPath backs GET /process-paths/{id}: the ProcessPath the id
// resolves to, an ACTIVE process flow first and else the locally declared path
// (docs/adr/0025), with its read-only source. A flow with no local path is a
// 200; an id neither source knows is the 404 process-path-not-found problem.
// The route exists only when the GetProcessPath use case is wired.
func (s *Server) handleGetProcessPath(w http.ResponseWriter, r *http.Request) {
	path, err := s.GetProcessPath.Handle(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toProcessPathResponse(path))
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
		writeProblem(w, http.StatusBadRequest, problemMalformedWindowStart, err.Error(), r.URL.Path)
		return
	}
	windowEnd, err := time.Parse(time.RFC3339, q.Get("window_end"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemMalformedWindowEnd, err.Error(), r.URL.Path)
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
		Source:         string(result.Source),
		NormalizedRate: result.NormalizedRate.Quantity(),
		NormalizedUnit: string(result.NormalizedRate.Unit()),
		BottleneckStep: string(result.BottleneckStep),
		StepBreakdown:  breakdown,
		Warnings:       nonNilStrings(result.Warnings),
	})
}
