package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"

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

	// CreateCapacityPlan and PublishCapacityPlan back the Phase 4
	// /capacity-plans endpoints; CapacityPlans is the read port behind
	// GET /capacity-plans/{id}.
	CreateCapacityPlan  *usecases.CreateCapacityPlan
	PublishCapacityPlan *usecases.PublishCapacityPlan
	CapacityPlans       ports.CapacityPlanRepository

	// Station capacity (ADR 0002). DeclareStationStandard backs
	// PUT /station-standards/{location}/{process_type}; StationStandards is
	// the read port behind GET /station-standards (a direct, no-invariant
	// repository read, like ProcessCapacities); GetStorageCapacity backs
	// GET /storage-capacity.
	DeclareStationStandard *usecases.DeclareStationStandard
	StationStandards       ports.StationStandardRepository
	GetStorageCapacity     *usecases.GetStorageCapacity

	// GetExpectedDemand backs GET /demand (docs/adr/0004): the expected
	// demand read model fed by order-management's order events. Nil leaves
	// the route unregistered (the router's 404), so a Server built without
	// it behaves exactly as before the model existed.
	GetExpectedDemand *usecases.GetExpectedDemand

	// ListProcessPaths backs GET /process-paths and ListCapacityPlans backs
	// GET /capacity-plans (the console remote's list reads). Either left nil
	// leaves its route unregistered, so a Server built without them behaves
	// exactly as before the lists existed.
	ListProcessPaths  *usecases.ListProcessPaths
	ListCapacityPlans *usecases.ListCapacityPlans

	// Readiness backs GET /readyz, separate from the liveness-only
	// /healthz above (graceful-shutdown fix: this service previously had
	// no readiness signal at all). A nil Readiness (the zero value, and
	// every pre-existing test/caller) means /readyz always reports ready
	// -- see Readiness's own doc comment.
	Readiness *Readiness
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
//
// Middleware order matters (standard-metrics convention, docs/adr/0011
// Tier 1): otelchi runs first so every later handler (including CORS'
// own) runs inside a span, and otelchi.WithChiRoutes resolves the route
// pattern up front, so spans/metrics are labeled e.g.
// "/capacity-plans/{id}" rather than one distinct name per plan id.
func NewRouter(s *Server) http.Handler {
	r := chi.NewRouter()

	r.Use(otelchi.Middleware(DefaultServiceName, otelchi.WithChiRoutes(r)))
	// Emits http.server.request.duration (seconds) per OTel HTTP semantic
	// conventions; no hand-rolled histogram needed.
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(DefaultServiceName)))

	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsAllowedOrigins(),
		AllowedMethods:   []string{http.MethodGet, http.MethodPost, http.MethodPut},
		AllowedHeaders:   []string{"Accept", "Content-Type"},
		AllowCredentials: false,
	}))

	r.Get("/healthz", s.handleHealthz)
	r.Get("/readyz", s.handleReadyz)
	r.Post("/process-capacities", s.handleRegisterProcessCapacityConstraint)
	r.Get("/process-capacities", s.handleGetEffectiveProcessCapacity)
	r.Post("/process-paths", s.handleRegisterProcessPath)
	r.Get("/process-paths/{id}/capacity", s.handleGetProcessPathCapacity)
	r.Post("/capacity-plans", s.handleCreateCapacityPlan)
	r.Post("/capacity-plans/{id}/publish", s.handlePublishCapacityPlan)
	r.Get("/capacity-plans/{id}", s.handleGetCapacityPlan)
	r.Put("/station-standards/{location}/{process_type}", s.handleDeclareStationStandard)
	r.Get("/station-standards", s.handleListStationStandards)
	r.Get("/storage-capacity", s.handleGetStorageCapacity)
	if s.GetExpectedDemand != nil {
		r.Get("/demand", s.handleGetExpectedDemand)
	}
	if s.ListProcessPaths != nil {
		r.Get("/process-paths", s.handleListProcessPaths)
	}
	if s.ListCapacityPlans != nil {
		r.Get("/capacity-plans", s.handleListCapacityPlans)
	}

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
