---
id: troubleshooting
title: Troubleshooting
sidebar_label: Troubleshooting
---

# Troubleshooting

Each row is a failure mode visible in the code, with the log line or response
that identifies it. Log messages are quoted exactly as the binaries write them
(`msg` field of the JSON line). Variables are on
[Configuration](/docs/operations/configuration), procedures on the
[Runbook](/docs/operations/runbook).

This service has **no** idempotency-key middleware, **no** optimistic
concurrency (`If-Match`/`ETag`, so no 412) and **no** circuit breaker: it
makes no synchronous call to another context. A repeated `POST /capacity-plans`
creates a second plan; a repeated publish is a 409.

## Boot and readiness

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Pod exits at boot, log `service exited with error` with `run migrations (after 5 attempts): ...` | Postgres unreachable from the pod, wrong DSN, or the migration step running through PgBouncer (golang-migrate's advisory lock fails in transaction pooling) | the earlier `retrying` lines (`op`, `err`); whether the pod has `MIGRATIONS_DATABASE_URL` | point `MIGRATIONS_DATABASE_URL` at the direct Postgres DSN ([ADR 0010](/docs/adr/0010-migrations-over-direct-connection)); fix network/credentials |
| Boot fails with `Dirty database version N. Fix and force version.` | a previous migration failed halfway and golang-migrate marked `schema_migrations` dirty | `SELECT * FROM schema_migrations` in the OLTP database | repair the half-applied change by hand, then `migrate force <N-1 or N>` against the direct DSN and restart |
| Pod exits: `EVENT_PUBLISHER=kafka requires KAFKA_BROKERS` | publisher set to Kafka without brokers | pod env | set `KAFKA_BROKERS` (chart: `kafka.enabled=true`; the chart refuses to render this combination) |
| Pod exits: `unknown EVENT_PUBLISHER "..." (want kafka or log)` | typo in `EVENT_PUBLISHER` | pod env | use `kafka` or `log` |
| Pod exits: `DEMAND_CONSUMER_GROUP is set but DEMAND_SITE_ID is not ...` | demand consumer enabled without its site | pod env | set `DEMAND_SITE_ID` (chart `demand.siteId`) |
| Projector exits: `ANALYTICS_DATABASE_URL is required ...` or `KAFKA_BROKERS is required ...`; reports exits with the first | missing required variable | pod env, the chart's analytics Secret keys | set them (chart `analytics.database.*`, `kafka.enabled`) |
| startupProbe keeps failing and the pod restarts, no `http server listening` line | the listener opens only after migrations and the first ping succeed; the probe allows 60 s, the boot retry needs about 15 s per step | logs for `retrying` with `op` = `run migrations` / `ping postgres` | fix the database dependency; readiness itself never waits for Kafka |
| `/readyz` returns 503 `{"status":"not_ready"}` | the pod received SIGTERM: readiness flips first, then the drain delay | log `shutdown: readiness flipped to not-ready; ...` | expected during rollout; it never flips back |
| Data disappears after a restart; log `DATABASE_URL not configured; using in-memory adapters` | `DATABASE_URL` empty, in-memory adapters | pod env | set `DATABASE_URL` (the chart refuses to render without a DSN source) |
| Shutdown logs `kafka consumer did not stop before the shutdown drain deadline` or `outbox relay did not stop ...` | a handler or a relay send was still blocked after 10 s | the lines before it | usually a broker or database stall; the pod still exits. Undelivered outbox rows are sent by the next pod |

## Kafka ingestion

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| No labor or station data, log `KAFKA_BROKERS not configured; labor/storage capacity Kafka ingestion is disabled` | brokers unset | pod env | set `KAFKA_BROKERS` |
| `GET /demand` always `orders: 0`, `as_of: null`; log `order demand consumption is disabled` | `DEMAND_CONSUMER_GROUP` unset (the feature is off by default) | pod env | set `DEMAND_CONSUMER_GROUP` and `DEMAND_SITE_ID` |
| Demand exists in the database but `GET /demand?location=X` is empty | every order is stored under `DEMAND_SITE_ID`; the query uses another site code | `SELECT location, count(*) FROM order_demand GROUP BY 1` | query with the configured site (in the cluster `SIM1`) or align `DEMAND_SITE_ID` |
| Consumer lag grows on one partition, repeated `... handling failed; retrying the same message` | a transient database failure; domain consumers retry 5 times, then dead-letter | the `error`, `partition`, `offset` fields; database health | fix the database; the message is retried or dead-lettered automatically |
| Analytics lag grows forever, repeated `analytics consumer handling failed; retrying the same message` | the projector retries transient failures **without limit** by design (ADR 0005); its partition is blocked | analytical database health, `GET /reports/freshness` | fix the analytical database; nothing is lost |
| `... handling failed after the maximum attempts; dead-lettering the message` | a domain consumer gave up after 5 attempts | the `<topic>.dlq` topic and its `x-dlq-error` header | fix the cause, then re-drive (see [Runbook](/docs/operations/runbook#re-drive-a-dead-lettered-message)) |
| `analytics: dead-lettering a message that can never be projected` | poison on the analytics topic: `plan_id` empty or different from the CloudEvents `subject`, missing `warehouse_id`/`location`, zero `time`, negative `shortage`, undecodable data, or a store rejection | `x-dlq-error` on `warehouse.warehouse-planning.analytics.dlq` | fix the producer; re-drive if the payload can be corrected |
| `analytics: skipping message that is not a CloudEvents 1.0 event ...` | a non-CloudEvents message on the analytics topic | `suppressed_since_last_warning` | find and stop the producer; skips are committed past |
| Labor events consumed but the path capacity still lacks the step | upper-cased `path_id` of `ShiftPlanCommitted` is the process type, `building_id` the location; the window is `[event time, event time + planned_hours)` | `GET /process-capacities` with that exact window | plan windows must be covered by the registered window ([ADR 0003](/docs/adr/0003-window-coverage-semantics)) |

## Publishing (outbox)

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Plans created but nothing on `warehouse.warehouse-planning.events`; log `event published (log sink)` | `EVENT_PUBLISHER=log` (the default) | pod env | set `EVENT_PUBLISHER=kafka` with brokers |
| Rows pile up in `outbox_events` with `published_at IS NULL`, log `outbox relay pass failed` | broker unreachable or rejecting writes; the pass stops at the first failing row | `SELECT id, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 10` | fix the broker; the relay retries every `OUTBOX_RELAY_INTERVAL` |
| Plans created over MCP never reach Kafka | only `cmd/api` runs the relay | is an `api` pod running with `EVENT_PUBLISHER=kafka` against the same database? | run the `api` component |
| Reports are empty although plans exist | events reach the analytics topic only through the Kafka relay; projector down or behind | `GET /reports/freshness` (`as_of` null), projector logs | enable the Kafka publisher, start the projector |
| Reports return 500 `report-store-error` | analytical query failed (database down, or the projector has not created the tables yet: reports runs no migrations) | reports logs, the projector's migration step | start the projector first; fix the analytical DSN |

## REST problem types

Every error body is `application/problem+json` with `type` =
`https://errors.warehouse-planning.warehouse-systems.dev/<slug>`. The MCP tools
return the same slug as `<slug>: <message>` text. For an unexpected error the
REST `detail` carries the underlying error text; the MCP tools return only
`internal-error: an unexpected internal error occurred` and log the cause.

| Status | Slug | Cause | Fix |
| --- | --- | --- | --- |
| 400 | `malformed-json` | the body is not JSON | send valid JSON |
| 400 | `malformed-window-start`, `malformed-window-end` | not RFC 3339, often an unencoded `+` or `:` in a query string | percent-encode (`T08%3A00%3A00Z`) or use `Z` |
| 400 | `malformed-units-per-order`, `malformed-packages-per-order`, `malformed-limit` | non-numeric query value, or `limit` < 1 | send a number; `limit` is capped at 100 |
| 400 | `invalid-capacity-window` | `window_end` not after `window_start` | swap or fix the window |
| 400 | `missing-location` | `GET /storage-capacity` or `GET /demand` without `location` | add `location` |
| 400 | `invalid-report-range` | report `from`/`to` not RFC 3339, `from >= to`, or longer than 366 days | narrow the range |
| 404 | `process-capacity-not-found` | `GET /process-capacities` looks up the **exact** window (no coverage) | use the registered window, or query the path capacity instead |
| 404 | `process-path-not-found`, `capacity-plan-not-found` | unknown id | register the path / check the plan id |
| 409 | `unit-mismatch` | a constraint's unit differs from the first constraint registered for that process, location and window | register every constraint of an aggregate in one unit |
| 409 | `capacity-plan-already-published` | publish called twice | none: a plan publishes once |
| 422 | `negative-quantity`, `non-positive-period` | bad rate | quantity ≥ 0, `period_seconds` > 0 |
| 422 | `non-positive-station-standard`, `missing-station-standard-field` | standard with zero throughput or blank location/process type | send a positive quantity |
| 422 | `unsupported-normalization-unit` | a step or station standard in `LINE` (or any unit other than `UNIT`, `PACKAGE`, `ORDER`); unit strings are not validated when a constraint is registered, only when normalized | register in a normalizable unit |
| 422 | `missing-conversion-factor`, `non-positive-conversion-factor` | a `UNIT` or `PACKAGE` step without `units_per_order` / `packages_per_order`, or a factor ≤ 0 | pass the factor |
| 422 | `missing-step-capacity` | no registered window covers a step at that location (an empty location in the detail means the `location` query parameter was omitted), and no station standard can derive one | register capacity covering the window, or declare a station standard |
| 422 | `empty-process-path-steps` | path without steps | send at least one step |
| 422 | `negative-assigned-demand` | `assigned_demand` < 0 | 0 or more |
| 422 | `missing-assigned-demand` | `assigned_demand` omitted and the demand read model has no orders for that location and window (or the consumer is off) | pass `assigned_demand`, or enable demand ingestion |
| 422 | `missing-required-field` | blank `warehouse_id`, `site_id`, `location` or `path_id` (the problem title omits `site_id`, the check does not) | send all four; `site_id` is required since [ADR 0012](/docs/adr/0012-canonical-site-id-on-capacity-plans) |
| 500 | `internal-error` | infrastructure failure (database) | read `detail` and the pod logs |

## Capacity answers that look wrong

| Symptom | Cause | Fix |
| --- | --- | --- |
| A response carries a warning about stations without a declared standard | stations are tallied for a step but no `StationStandard` exists; the step uses its other constraints | declare the standard (`PUT /station-standards/{location}/{process_type}`) |
| The bottleneck is `STATION` after a standard was declared | count × standard is now lower than labor | expected ([ADR 0002](/docs/adr/0002-station-capacity-composition)) |
| Registering a constraint again does not add up | re-registering a constraint type for the same process, location and window **replaces** its rate | register the total, not an increment |
| Browser calls from the `web/` dev server fail CORS | the API allows only `CORS_ALLOWED_ORIGINS` (default `http://localhost:5173`) | start the API with `CORS_ALLOWED_ORIGINS=http://localhost:5190` |
