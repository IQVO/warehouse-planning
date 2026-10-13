---
id: observability
title: Observability
sidebar_label: Observability
---

# Observability

Telemetry is pushed, not scraped. **No binary serves a `/metrics`
endpoint.** All four binaries, `cmd/api`, `cmd/mcp`, `cmd/planning-projector`
and `cmd/planning-reports`, call `telemetry.Setup`
(`internal/adapters/outbound/telemetry/telemetry.go`,
[ADR 0011](/docs/adr/0011-standard-metrics-adoption); the projector since
[ADR 0015](/docs/adr/0015-projector-telemetry-and-chart-otel-env)). Setup
installs a TracerProvider and a MeterProvider, both exporting over OTLP/gRPC
without TLS to `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`).
Metrics are pushed every 30 s. The exporter connects lazily, so a missing
Collector drops telemetry and never blocks boot. The projector logs
`telemetry configured` with `service_name`, `service_version`, `environment`
and `otlp_endpoint` at boot; it creates no spans and records no business
metric, only the Go runtime metrics.

The global propagator is W3C `traceparent` plus baggage, so an incoming
request's trace context continues into the server span.

## Resource attributes

| Attribute | Value |
| --- | --- |
| `service.name` | `warehouse-planning` (`cmd/api`), `warehouse-planning-mcp` (`cmd/mcp`), `warehouse-planning-projector` (`cmd/planning-projector`), `warehouse-planning-reports` (`cmd/planning-reports`) |
| `service.version` | `SERVICE_VERSION`, default `dev` |
| `deployment.environment.name` | `ENVIRONMENT`, default `local` |
| SDK defaults | `telemetry.sdk.*` from `resource.Default()` |

The chart renders `OTEL_EXPORTER_OTLP_ENDPOINT` (`otel.endpoint`, default
`otel-collector.observability.svc.cluster.local:4317`, while `otel.enabled`),
`SERVICE_VERSION` (`image.tag`) and `ENVIRONMENT` (`environment`, default
`local`) on the `api`, `mcp`, projector and reports Deployments
([ADR 0015](/docs/adr/0015-projector-telemetry-and-chart-otel-env)), the same
values as the sibling charts. `warehouse-infra` sets none of them for this
service, so in the kind cluster every pod exports to the in-cluster Collector
with the image tag as `service.version`.

## Metric instruments

| Instrument | Type | Unit | Attributes | Emitted by | Source |
| --- | --- | --- | --- | --- | --- |
| `warehouse_planning.capacity_plans.created` | Int64 counter | `{plan}` | `outcome` = `created` or `rejected` | every `CreateCapacityPlan` attempt, through REST (`cmd/api`) or MCP (`cmd/mcp`). Every error counts as `rejected`: validation failures, a missing path, missing capacity, missing demand, and infrastructure failures as well. | `internal/adapters/outbound/telemetry/metrics.go`, recorded in `internal/application/usecases/create_capacity_plan.go` |
| `http.server.request.duration` | Float64 histogram | `s` | `http.method`, `http.scheme`, `http.route` (chi route pattern). **No status-code attribute** in otelchi v0.12.3. The `service.name` is set as an instrumentation-scope attribute. | every request on the `api`, `mcp` and reports routers | `otelchimetric.NewServerRequestDuration` in `internal/adapters/inbound/http/process_capacity_handler.go`, `reports_handler.go`, `cmd/mcp/router.go` |
| `go.memory.used`, `go.memory.limit`, `go.memory.allocated`, `go.memory.allocations`, `go.memory.gc.goal`, `go.goroutine.count`, `go.processor.limit`, `go.config.gogc` | runtime gauges and counters | per OTel Go semconv | as defined by `go.opentelemetry.io/contrib/instrumentation/runtime` v0.71.0 | `cmd/api`, `cmd/mcp`, `cmd/planning-projector`, `cmd/planning-reports` | `runtime.Start` in `telemetry.go` |

The histogram's bucket boundaries are 0.005, 0.01, 0.025, 0.05, 0.075, 0.1,
0.25, 0.5, 0.75, 1, 2.5, 5, 7.5 and 10 s. In Prometheus, after the Collector's
translation, the instruments appear as
`warehouse_planning_capacity_plans_created_total`,
`http_server_request_duration_seconds_*`, `go_goroutine_count` and
`go_memory_used_bytes`.

There are **no** metrics for Kafka consumption, consumer lag, dead-lettering,
outbox backlog, relay failures or Postgres. Watch those through logs, the
database and the broker (see [Suggested alerts](#suggested-alerts)).

## Traces

- **Server spans**: `otelchi.Middleware` on the same three routers creates one span per request. With `WithChiRoutes`, the span name is the chi route pattern without the method, for example `/capacity-plans/{id}/publish` or `/reports/shortage-trend`. Spans carry `http.route` and the response `http.status_code`.
- **Trace context crosses the outbox** ([ADR 0014](/docs/adr/0014-trace-context-crosses-the-outbox)). `postgres.OutboxRepo.Insert` injects the request span's W3C context (`traceparent`, plus `tracestate` / `baggage` when present) into each row's `outbox_events.headers`, and the relay copies the stored headers onto the Kafka message. A consumer that extracts them continues the request's trace. Rows written without an active span (or before this change) carry only `content-type`. The in-memory outbox (no `DATABASE_URL`) does not inject.
- **Nothing else is traced.** The Kafka consumers, the outbox relay, the use cases and the Postgres calls create no spans of their own; this service's consumers do not extract incoming trace headers because they open no span.

## Logs

Every binary writes JSON lines to stdout through `log/slog`
(`slog.NewJSONHandler`). The level comes from `LOG_LEVEL`. Each line has
`time`, `level` and `msg`. The common structured keys are:

| Key | Appears on |
| --- | --- |
| `error` | every failure line |
| `topic`, `group_id`, `brokers` | consumer and relay start lines (`labor capacity consumer running`, `storage capacity consumer running`, `order demand consumer running`, `analytics consumer running`, `outbox relay running`) |
| `partition`, `offset`, `attempt`, `max_attempts`, `retry_in` | `... handling failed; retrying the same message` and `... offset commit failed; retrying the same message` |
| `attempts` | `... handling failed after the maximum attempts; dead-lettering the message` |
| `dlq_topic` | `analytics: dead-lettering a message that can never be projected` |
| `suppressed_since_last_warning` | `analytics: skipping message that is not a CloudEvents 1.0 event ...` (at most one line a minute) |
| `publisher`, `interval`, `max_attempts` | `outbox relay running` |
| `outbox_id`, `topic`, `event_type`, `key`, `attempts`, `max_attempts`, `error` | ERROR `outbox relay: row reached the attempt cap and is excluded from future claims; ...`, logged once for the failure that reaches `OUTBOX_MAX_ATTEMPTS` ([ADR 0013](/docs/adr/0013-outbox-relay-attempt-cap)) |
| `service_name`, `service_version`, `environment`, `otlp_endpoint` | `telemetry configured` (`cmd/planning-projector`) |
| `topic`, `type`, `subject`, `id` | `event published (log sink)`, written for every relayed event when `EVENT_PUBLISHER=log` |
| `op`, `attempt`, `in`, `err` | `retrying` and `succeeded after retry` from the boot retry (migrations, first ping) |
| `migrations_path` | `postgres adapters configured` |
| `site_id` | `order demand consumer running` |
| `drain_delay` | `shutdown: readiness flipped to not-ready; waiting for traffic to drain` |
| `consumer` | `kafka consumer did not stop before the shutdown drain deadline` |

Log lines carry **no** `trace_id` or `span_id`: no code in this repo adds them.

## Dashboards

`warehouse-infra` provisions one Grafana dashboard for this context:
`terraform/dashboards/contexts/warehouse-planning.json` (uid
`warehouse-warehouse-planning`). It has these rows:

- **API Gateway (Kong)**: request rate by status code, 5xx ratio, and p95 request latency compared with upstream latency, from `kong_*` metrics on the `httproute.warehouse-systems.warehouse-planning.*` services.
- **Service HTTP RED**: request rate by `service_name` and `http_route`, a 5xx panel, and p95 duration by `service_name`, from `http_server_request_duration_seconds_*` with `service_name=~"warehouse-planning.*"`.
- **Business metrics**: `warehouse_planning_capacity_plans_created_total` by `outcome`.
- **Go runtime**: `go_goroutine_count` and `go_memory_used_bytes`.
- **Logs (Loki)**: the live stream for `app="warehouse-planning"` with the Istio containers excluded, volume by level, and ERROR or WARN only.

Three caveats, all visible in code:

- The service 5xx panel filters on `http_response_status_code` or `http_status_code`. The histogram has neither attribute (see above), so that panel stays empty. Use the Kong 5xx panel instead.
- The projector serves no HTTP route, so it never appears on the HTTP panels; it does appear on the Go runtime panels as `warehouse-planning-projector` since ADR 0015.
- A panel description says that log lines embed `trace_id` and `span_id`. They do not in this service.

## Suggested alerts

These are built only from signals that exist today:

| Alert | Signal | Why |
| --- | --- | --- |
| Outbox backlog | `SELECT count(*), min(created_at) FROM outbox_events WHERE published_at IS NULL` rising, or `max(attempts)` growing with a `last_error` | The relay is failing (broker down, `EVENT_PUBLISHER=log` left on) and order-management is not seeing plans. |
| Outbox rows excluded by the attempt cap | `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND attempts >= 20` (the `OUTBOX_MAX_ATTEMPTS` value) above 0, or the Loki ERROR line `row reached the attempt cap` | A poison row (or a long broker outage) was excluded; it is never retried until an operator re-arms it ([Runbook](/docs/operations/runbook#outbox-relay)). |
| DLQ growth | the end offsets of `warehouse.workforce.events.dlq`, `warehouse.facility.events.dlq`, `warehouse.order-management.events.dlq` and `warehouse.warehouse-planning.analytics.dlq` increasing | A message was given up on and nothing re-drives it. |
| Consumer lag | `kafka-consumer-groups.sh --describe` for the four groups | A wedged partition: the analytics consumer retries transient errors forever. |
| Analytics freshness | `GET /reports/freshness` `lag_seconds` above a threshold | The projector is stuck or down (it has no metrics of its own). |
| Plan rejections | `rate(warehouse_planning_capacity_plans_created_total{outcome="rejected"}[15m])` as a share of all attempts | Callers send plans the invariants refuse, or capacity data or demand is missing. |
| Latency | p95 of `http_server_request_duration_seconds` by `service_name` | REST, MCP or reports slow-down. |
| Restarts and ERROR logs | pod restarts, plus the Loki `level="ERROR"` rate | Boot-retry exhaustion, or a consumer that stopped. |
