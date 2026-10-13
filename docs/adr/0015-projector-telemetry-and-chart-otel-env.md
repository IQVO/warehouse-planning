# ADR 0015: Projector telemetry and the chart's OTel environment

## Status

Accepted (2026-10-13, ecosystem gap fix D21). Extends
[ADR 0011](./0011-standard-metrics-adoption.md) (standard metrics adoption)
to the fourth binary and to the Helm chart; nothing in ADR 0011 changes.

## Context

ADR 0011 wired `internal/adapters/outbound/telemetry.Setup` into `cmd/api`,
`cmd/mcp` and `cmd/planning-reports`. Two gaps remained:

1. `cmd/planning-projector` (the analytics writer,
   [ADR 0005](./0005-analytics-read-side.md)) never called `Setup`: it
   exported no Go runtime metrics, installed no propagator and reported no
   `service.name`, so a dead or slow projector was invisible on the fleet
   dashboards that filter `service_name=~"warehouse-planning.*"`.
2. The chart rendered none of `OTEL_EXPORTER_OTLP_ENDPOINT`,
   `SERVICE_VERSION` or `ENVIRONMENT` on any Deployment, and
   `warehouse-infra` does not set them for this service either. Every pod
   therefore exported to `localhost:4317` (nothing listens there), reported
   `service.version=dev` and `deployment.environment.name=local`. The sibling
   charts (inbound-receiving, order-management, ...) already render all three
   from `environment`, `image.tag` and an `otel` block.

The fleet decision (gap-fix wave 2026-10-12, D21) is observability parity:
every projector and reports binary calls `telemetry.Setup`, and every chart
that injects the OTel environment does it the same way on every Deployment.

## Decision

1. **`cmd/planning-projector` calls `telemetry.Setup`** at boot, before it
   opens the analytical pool, with service name
   `warehouse-planning-projector`, `SERVICE_VERSION` (default `dev`),
   `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`) and `ENVIRONMENT`
   (default `local`, read by the adapter). The providers are flushed, bounded
   to 5 s, after the consumer and the pool are closed. Export never blocks:
   an unreachable Collector only drops telemetry, exactly like the other
   three binaries. The projector still creates no spans of its own (ADR 0014
   §4).
2. **The chart renders the OTel environment on every backend Deployment**
   (`api`, `mcp`, `analytics-projector`, `analytics-reports`) with the
   sibling charts' values and shape:
   - `ENVIRONMENT` from the new top-level value `environment` (default
     `local`);
   - `SERVICE_VERSION` from `image.tag`, falling back to the chart's
     `appVersion`;
   - `OTEL_EXPORTER_OTLP_ENDPOINT` from `otel.endpoint` (default
     `otel-collector.observability.svc.cluster.local:4317`, the Collector
     Service `warehouse-infra` deploys), only while `otel.enabled` (default
     `true`).
   The `frontend` (nginx) Deployment gets none: it runs no Go binary.
3. **`SHUTDOWN_DRAIN_DELAY` is rendered on the `api` Deployment only**, from
   the new value `config.shutdownDrainDelay` (empty = not rendered, the
   binary's 5 s default applies; same shape as the siblings). It is not set
   on the other Deployments because no other binary reads it
   (`cmd/api/main.go` is the only reader; `cmd/mcp`,
   `cmd/planning-projector` and `cmd/planning-reports` have no drain delay).

## Consequences

- In the kind cluster the four processes now export to the in-cluster
  Collector and carry the image tag as `service.version`. A cluster where
  the Collector lives elsewhere sets `otel.endpoint`; one without a
  Collector can set `otel.enabled=false`.
- The projector appears on the context dashboard's runtime panels as
  `warehouse-planning-projector`.
- `environment` is a new top-level value; `warehouse-infra` may override it
  per environment, nothing else changes there.
- Tests: `TestSetupTelemetry_InstallsTheSDKProviders` and
  `TestFlushTelemetry_LogsAShutdownFailure` (`cmd/planning-projector`), and
  `charts/warehouse-planning/tests/render_test.sh` (every backend Deployment
  renders `ENVIRONMENT`, `SERVICE_VERSION` and, with `otel.enabled`,
  `OTEL_EXPORTER_OTLP_ENDPOINT`; the api renders `SHUTDOWN_DRAIN_DELAY` when
  set), run in CI by the `chart-tests` job.
