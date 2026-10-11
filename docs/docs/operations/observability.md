---
id: observability
title: Observability
sidebar_label: Observability
---

# Observability

Telemetry is pushed, not scraped. **No binary serves a `/metrics`
endpoint.** `cmd/api`, `cmd/mcp` and `cmd/planning-reports` call
`telemetry.Setup` (`internal/adapters/outbound/telemetry/telemetry.go`,
[ADR 0011](/docs/adr/0011-standard-metrics-adoption)). Setup installs a
TracerProvider and a MeterProvider, both exporting over OTLP/gRPC without TLS
to `OTEL_EXPORTER_OTLP_ENDPOINT` (default `localhost:4317`). Metrics are pushed
every 30 s. The exporter connects lazily, so a missing Collector drops
telemetry and never blocks boot. **`cmd/planning-projector` sets up no
OpenTelemetry at all**: its only signals are its logs and its `/healthz` and
`/readyz` endpoints.

The global propagator is W3C `traceparent` plus baggage, so an incoming
request's trace context continues into the server span.

## Resource attributes

| Attribute | Value |
| --- | --- |
| `service.name` | `warehouse-planning` (`cmd/api`), `warehouse-planning-mcp` (`cmd/mcp`), `warehouse-planning-reports` (`cmd/planning-reports`) |
| `service.version` | `SERVICE_VERSION`, default `dev` |
| `deployment.environment.name` | `ENVIRONMENT`, default `local` |
| SDK defaults | `telemetry.sdk.*` from `resource.Default()` |

Neither the chart nor `warehouse-infra` sets `OTEL_EXPORTER_OTLP_ENDPOINT`,
`SERVICE_VERSION` or `ENVIRONMENT` for this service, so a deployed pod reports
`service.version=dev` and `deployment.environment.name=local` and exports to
`localhost:4317`, unless a values override adds them through `extraEnv`.

## Metric instruments

| Instrument | Type | Unit | Attributes | Emitted by | Source |
| --- | --- | --- | --- | --- | --- |
| `warehouse_planning.capacity_plans.created` | Int64 counter | `{plan}` | `outcome` = `created` or `rejected` | every `CreateCapacityPlan` attempt, through REST (`cmd/api`) or MCP (`cmd/mcp`). Every error counts as `rejected`: validation failures, a missing path, missing capacity, missing demand, and infrastructure failures as well. | `internal/adapters/outbound/telemetry/metrics.go`, recorded in `internal/application/usecases/create_capacity_plan.go` |
| `http.server.request.duration` | Float64 histogram | `s` | `http.method`, `http.scheme`, `http.route` (chi route pattern). **No status-code attribute** in otelchi v0.12.3. The `service.name` is set as an instrumentation-scope attribute. | every request on the `api`, `mcp` and reports routers | `otelchimetric.NewServerRequestDuration` in `internal/adapters/inbound/http/process_capacity_handler.go`, `reports_handler.go`, `cmd/mcp/router.go` |
| `go.memory.used`, `go.memory.limit`, `go.memory.allocated`, `go.memory.allocations`, `go.memory.gc.goal`, `go.goroutine.count`, `go.processor.limit`, `go.config.gogc` | runtime gauges and counters | per OTel Go semconv | as defined by `go.opentelemetry.io/contrib/instrumentation/runtime` v0.71.0 | `cmd/api`, `cmd/mcp`, `cmd/planning-reports` | `runtime.Start` in `telemetry.go` |

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
- **Nothing else is traced.** The Kafka consumers, the outbox relay, the use cases and the Postgres calls create no spans, and the produced CloudEvents carry no trace context. A trace therefore ends at the HTTP handler.

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
| `publisher`, `interval` | `outbox relay running` |
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
- The projector emits no metrics, so it never appears on the HTTP or runtime panels, even though a panel description names it.
- A panel description says that log lines embed `trace_id` and `span_id`. They do not in this service.

## Suggested alerts

These are built only from signals that exist today:

| Alert | Signal | Why |
| --- | --- | --- |
| Outbox backlog | `SELECT count(*), min(created_at) FROM outbox_events WHERE published_at IS NULL` rising, or `max(attempts)` growing with a `last_error` | The relay is failing (broker down, `EVENT_PUBLISHER=log` left on) and order-management is not seeing plans. |
| DLQ growth | the end offsets of `warehouse.workforce.events.dlq`, `warehouse.facility.events.dlq`, `warehouse.order-management.events.dlq` and `warehouse.warehouse-planning.analytics.dlq` increasing | A message was given up on and nothing re-drives it. |
| Consumer lag | `kafka-consumer-groups.sh --describe` for the four groups | A wedged partition: the analytics consumer retries transient errors forever. |
| Analytics freshness | `GET /reports/freshness` `lag_seconds` above a threshold | The projector is stuck or down (it has no metrics of its own). |
| Plan rejections | `rate(warehouse_planning_capacity_plans_created_total{outcome="rejected"}[15m])` as a share of all attempts | Callers send plans the invariants refuse, or capacity data or demand is missing. |
| Latency | p95 of `http_server_request_duration_seconds` by `service_name` | REST, MCP or reports slow-down. |
| Restarts and ERROR logs | pod restarts, plus the Loki `level="ERROR"` rate | Boot-retry exhaustion, or a consumer that stopped. |
