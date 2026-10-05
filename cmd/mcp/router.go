package main

import (
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/riandyrn/otelchi"
	otelchimetric "github.com/riandyrn/otelchi/metric"
)

// mcpServiceName labels the mcp binary for logs/spans/metrics, distinct
// from the main api service's DefaultServiceName.
const mcpServiceName = "warehouse-planning-mcp"

// newRouter wraps the MCP handler in a small chi router so the binary is
// deployable behind Kubernetes probes:
//
//   - GET /healthz  -> 200 {"status":"ok"} (open; liveness/readiness probes).
//   - /  and /mcp   -> the MCP Streamable HTTP handler. Both paths are served
//     so warehouse-ops-agent's *_MCP_ENDPOINT convention (".../mcp") and a
//     root-mounted endpoint both work.
//
// No auth middleware is ever added here (TestNoAuthMiddlewareReintroduced).
func newRouter(mcpHandler http.Handler) http.Handler {
	r := chi.NewRouter()
	r.Use(otelchi.Middleware(mcpServiceName, otelchi.WithChiRoutes(r)))
	r.Use(otelchimetric.NewServerRequestDuration(otelchimetric.NewBaseConfig(mcpServiceName)))
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	r.Mount("/mcp", mcpHandler)
	r.Mount("/", mcpHandler)
	return r
}
