package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// Server holds every use case + read port the HTTP adapter depends on.
type Server struct {
	RegisterProcessCapacityConstraint *usecases.RegisterProcessCapacityConstraint
	// ProcessCapacities backs the read-only GET endpoint. It is the same
	// port RegisterProcessCapacityConstraint writes through -- there is
	// no dedicated "GetEffectiveProcessCapacity" use case because the
	// read is a direct, no-invariant repo lookup (matching
	// inventory-storage's GetUsable/Classifications convention).
	ProcessCapacities ports.ProcessCapacityRepository

	// RegisterProcessPath seeds a ProcessPath read model (Phase 2: no
	// event-driven sync from process-path-management yet).
	RegisterProcessPath *usecases.RegisterProcessPath
	// GetProcessPathCapacity computes a ProcessPath's normalized,
	// end-to-end capacity and bottleneck step.
	GetProcessPathCapacity *usecases.GetProcessPathCapacity
}

// DefaultServiceName labels this service for logs/telemetry when the
// caller supplies none.
const DefaultServiceName = "warehouse-planning"

// defaultCORSAllowedOrigins is the local-dev default for
// CORS_ALLOWED_ORIGINS. Overridable via the env var (comma-separated).
const defaultCORSAllowedOrigins = "http://localhost:5173"

// NewRouter builds the chi router for every endpoint in
// .claude/rules/rest-api.md. No auth middleware is ever added here --
// internal/architecture/fitness_test.go's TestNoAuthMiddlewareReintroduced
// fails CI if it is.
func NewRouter(s *Server) http.Handler {
	r := chi.NewRouter()

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsAllowedOrigins(),
		AllowedMethods:   []string{http.MethodGet, http.MethodPost},
		AllowedHeaders:   []string{"Accept", "Content-Type"},
		AllowCredentials: false,
	}))

	r.Get("/healthz", s.handleHealthz)
	r.Post("/process-capacities", s.handleRegisterProcessCapacityConstraint)
	r.Get("/process-capacities", s.handleGetEffectiveProcessCapacity)
	r.Post("/process-paths", s.handleRegisterProcessPath)
	r.Get("/process-paths/{id}/capacity", s.handleGetProcessPathCapacity)

	return r
}

// corsAllowedOrigins reads CORS_ALLOWED_ORIGINS (comma-separated), falling
// back to defaultCORSAllowedOrigins when unset/empty.
func corsAllowedOrigins() []string {
	raw := osGetenv("CORS_ALLOWED_ORIGINS")
	if raw == "" {
		raw = defaultCORSAllowedOrigins
	}
	return splitAndTrim(raw)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleRegisterProcessCapacityConstraint backs POST /process-capacities.
func (s *Server) handleRegisterProcessCapacityConstraint(w http.ResponseWriter, r *http.Request) {
	var req registerConstraintRequest
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
	if req.PeriodSeconds <= 0 {
		writeProblem(w, http.StatusUnprocessableEntity, problemInfo{"non-positive-period", "period_seconds must be positive"}, "period_seconds must be positive", r.URL.Path)
		return
	}

	result, err := s.RegisterProcessCapacityConstraint.Handle(r.Context(), usecases.RegisterProcessCapacityConstraintCommand{
		ProcessType:    processcapacity.ProcessType(req.ProcessType),
		Location:       req.Location,
		WindowStart:    windowStart,
		WindowEnd:      windowEnd,
		ConstraintType: processcapacity.ConstraintType(req.ConstraintType),
		Quantity:       req.Quantity,
		Unit:           processcapacity.CapacityUnit(req.Unit),
		Period:         secondsToDuration(req.PeriodSeconds),
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	writeJSON(w, http.StatusCreated, effectiveCapacityResponse{
		EffectiveRate:     result.EffectiveRate.Quantity(),
		EffectiveUnit:     string(result.EffectiveRate.Unit()),
		BindingConstraint: string(result.BindingConstraint),
	})
}

// handleGetEffectiveProcessCapacity backs
// GET /process-capacities?process_type=&location=&window_start=&window_end=.
func (s *Server) handleGetEffectiveProcessCapacity(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	processType := q.Get("process_type")
	location := q.Get("location")

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

	pc, err := s.ProcessCapacities.FindByProcessLocationWindow(r.Context(), processcapacity.ProcessType(processType), location, windowStart, windowEnd)
	if err != nil {
		writeError(w, r, err)
		return
	}
	if pc == nil {
		writeProblem(w, http.StatusNotFound, problemInfo{"process-capacity-not-found", "No ProcessCapacity is registered for this process, location and window"},
			"no ProcessCapacity registered for the given process_type/location/window", r.URL.Path)
		return
	}

	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		writeError(w, r, err)
		return
	}

	entries := pc.Constraints()
	constraints := make([]constraintResponse, 0, len(entries))
	for _, entry := range entries {
		constraints = append(constraints, constraintResponse{
			ConstraintType: string(entry.Type),
			Quantity:       entry.Rate.Quantity(),
			Unit:           string(entry.Rate.Unit()),
			PeriodSeconds:  entry.Rate.Period().Seconds(),
		})
	}

	writeJSON(w, http.StatusOK, effectiveCapacityResponse{
		EffectiveRate:     effective.Quantity(),
		EffectiveUnit:     string(effective.Unit()),
		BindingConstraint: string(binding),
		Constraints:       constraints,
	})
}
