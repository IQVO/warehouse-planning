# ADR 0011: Standard metrics adoption (otelchi + a Tier-2 business counter)

## Status

Accepted (2026-10-05). Fixes a real gap found by the 2026-10-04
ADR-conformance audit: this service had no `otelchi`/`otelchimetric` and no
business-level metric, with `go.opentelemetry.io/otel` only an indirect
dependency.

## Context

The fleet-standard-metrics convention (mirrored here from
`process-path-management`, the closest-sized reference) is two tiers:

- **Tier 1 (automatic, per request)**: `otelchi` traces + `otelchimetric`'s
  `http.server.request.duration` histogram on every HTTP router, installed
  as middleware ahead of anything else so later handlers already run inside
  a span.
- **Tier 2 (hand-written, per business outcome)**: one `metric.Int64Counter`
  per meaningfully-countable business event, with an `outcome` attribute
  distinguishing success from failure on the SAME counter rather than two
  separately-named counters.

Neither tier existed in this service before this change.

## Decision

1. `internal/adapters/outbound/telemetry` is the new outbound adapter
   (copied, per fleet convention, from `process-path-management`'s own
   package of the same name): `Setup` installs a `TracerProvider` +
   `MeterProvider` exporting OTLP/gRPC to an optional Collector
   (`OTEL_EXPORTER_OTLP_ENDPOINT`, default `localhost:4317`), non-blocking --
   a missing Collector degrades to "telemetry dropped", never to "service
   won't start". `cmd/api`, `cmd/mcp` and `cmd/planning-reports` each call it
   once at startup and defer the returned shutdown func.
2. **Tier 1** is wired onto every HTTP router this service has: the main API
   router (`internal/adapters/inbound/http.NewRouter`), the reports router
   (`NewReportsRouter`) and the MCP router (`cmd/mcp/router.go`), each under
   its own `service.name` (`warehouse-planning`, `warehouse-planning-reports`,
   `warehouse-planning-mcp`) so the three binaries' spans/metrics are
   distinguishable. `cmd/planning-projector` has no HTTP router beyond its
   `/healthz`/`/readyz` admin mux (no business HTTP traffic to instrument) and
   is deliberately left out of this phase.
3. **Tier 2**: `warehouse_planning.capacity_plans.created`
   (`internal/adapters/outbound/telemetry.PlanMetrics`, behind
   `ports.PlanMetrics`), one counter with an `outcome` attribute
   (`created`/`rejected`), recorded by `usecases.CreateCapacityPlan.Handle`
   for every attempt, wired into both `cmd/api` and `cmd/mcp` (the two
   composition roots that can create a plan). `ports.PlanMetrics` is
   nil-safe at both levels: a nil interface field is guarded in `Handle`
   itself (an interface method call on a nil interface panics, unlike a
   nil-receiver struct method), and `*telemetry.PlanMetrics`'s own methods
   are additionally nil-receiver-safe, matching
   `process-path-management`'s `PathMetrics` convention.

## Consequences

- A dashboard/alert can now be built on `capacity_plans.created{outcome=...}`
  and on the automatic `http.server.request.duration` histograms across all
  three HTTP surfaces.
- `cmd/planning-projector` remains uninstrumented beyond logs; a future
  change adding business HTTP traffic there should revisit this ADR.
- Every behaviour-preserving caller (every pre-existing test) that does not
  wire `ports.PlanMetrics` continues to work unchanged (the nil-safety point
  above).

## Alternatives considered and rejected

- **Two separately-named counters (`capacity_plans_created_total` /
  `capacity_plans_rejected_total`)**: the fleet convention is one counter
  with an outcome attribute, so a query need not sum two series to answer
  "how many attempts".
- **A hand-rolled `httpconv`/manual span wrapper instead of `otelchi`**: one
  sibling (`fulfillment-execution`) did this and the audit flagged it as a
  Tier-1 gap of its own; `otelchi`+`otelchimetric` is the convention this ADR
  adopts instead.
- **Instrument `cmd/planning-projector`'s admin mux too**: it serves only
  `/healthz`/`/readyz`, no business traffic; instrumenting liveness/readiness
  checks themselves would add noise, not insight.
