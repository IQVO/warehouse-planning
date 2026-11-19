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

Every REST `POST` requires an `Idempotency-Key` ([ADR 0019](/docs/adr/0019-adopt-the-fleet-rest-api-guideline));
the `PUT`s are naturally idempotent and take none. There is **no** client-side
optimistic concurrency (`If-Match`/`ETag`, so no 412); the workload forecast
and the labour plan carry a server-side `version` instead, and a `PUT` that
loses a race twice is `409 version-conflict`. There is **no** circuit breaker:
the service makes no synchronous call to another
context. A repeated `POST /capacity-plans` under a *fresh* key creates a second
plan (the same key and body replays the first answer); a repeated publish is a
409.

## Boot and readiness

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Pod exits at boot, log `service exited with error` with `run migrations (after 5 attempts): ...` | Postgres unreachable from the pod, wrong DSN, or the migration step running through PgBouncer (golang-migrate's advisory lock fails in transaction pooling) | the earlier `retrying` lines (`op`, `err`); whether the pod has `MIGRATIONS_DATABASE_URL` | point `MIGRATIONS_DATABASE_URL` at the direct Postgres DSN ([ADR 0010](/docs/adr/0010-migrations-over-direct-connection)); fix network/credentials |
| Boot fails with `Dirty database version N. Fix and force version.` | a previous migration failed halfway and golang-migrate marked `schema_migrations` dirty | `SELECT * FROM schema_migrations` in the OLTP database | repair the half-applied change by hand, then `migrate force <N-1 or N>` against the direct DSN and restart |
| Pod exits: `DATABASE_URL is required: this context runs on real Postgres, there is no in-memory mode` (`cmd/api`, `cmd/mcp`) | `DATABASE_URL` unset | pod env | set it (the chart refuses to render without `database.url` / `database.existingSecret`) |
| Pod exits: `KAFKA_BROKERS is required: this context runs on real Kafka (outbox relay, consumers, /readyz)` | brokers unset or blank | pod env | set `KAFKA_BROKERS` (chart: `kafka.brokers`; the chart refuses to render without it) |
| Pod exits: `EVENT_PUBLISHER=log is not supported: this context publishes to real Kafka` or `EVENT_PUBLISHER: unknown value "..." (want kafka)` | the log sink was removed ([ADR 0016](/docs/adr/0016-postgres-and-kafka-are-the-only-runtime)); typo in `EVENT_PUBLISHER` | pod env | use `kafka` or unset it |
| Pod exits: `LABOR_CAPACITY_CONSUMER_GROUP is required: the consumer always runs on real Kafka under a stable group id` (same for `STORAGE_CAPACITY_CONSUMER_GROUP`, `DEMAND_CONSUMER_GROUP`, `DOCK_APPOINTMENT_CONSUMER_GROUP`, `OBSERVED_CAPACITY_CONSUMER_GROUP`, `LABOR_STANDARD_CONSUMER_GROUP`) | consumer group unset; the groups have no default in the binary | pod env | set a STABLE id (chart `config.laborCapacityConsumerGroup`, `config.storageCapacityConsumerGroup`, `demand.consumerGroup`, `dockAppointments.consumerGroup`, `observed.consumerGroup`, `laborStandard.consumerGroup` (chart default `warehouse-planning-labor-standards`); the chart refuses to render an empty one) |
| Pod exits: `DEMAND_SITE_ID is required: task completions carry no site and orders without a site_id need a fallback, ...` | demand site unset | pod env | set `DEMAND_SITE_ID` (chart `demand.siteId`) |
| Pod exits: `INBOUND_DEMAND_CONSUMER_GROUP is no longer supported: the inbound labour demand is read from dock-scheduling ...` (or `helm template` fails with `inbound.consumerGroup is set`) | a stale deployment still selects the retired inbound-receiving appointment source ([ADR 0020](/docs/adr/0020-inbound-demand-from-dock-scheduling)) | pod env, chart values / `extraEnv` | unset it and set `DOCK_APPOINTMENT_CONSUMER_GROUP` (chart `dockAppointments.consumerGroup`) |
| Projector exits: `ANALYTICS_DATABASE_URL is required ...`, `KAFKA_BROKERS is required ...` or `ANALYTICS_CONSUMER_GROUP is required ...`; reports exits with the first | missing required variable | pod env, the chart's analytics Secret keys | set them (chart `analytics.database.*`, `kafka.brokers`, `analytics.projector.consumerGroup`) |
| Pod exits non-zero: `<worker> stopped unexpectedly: exiting so the orchestrator restarts the service` (`outbox-relay`, `labor-capacity consumer`, ...) | the relay or a consumer returned on its own, e.g. the broker went away for good | the `consumer stopped` / `outbox relay stopped` ERROR line before it | none needed: the pod restarts; fix the broker if it keeps happening |
| startupProbe keeps failing and the pod restarts, no `http server listening` line | the listener opens only after migrations and the first ping succeed; the probe allows 60 s, the boot retry needs about 15 s per step | logs for `retrying` with `op` = `run migrations` / `ping postgres` | fix the database dependency; readiness itself never waits for Kafka |
| `/readyz` returns 503 `{"status":"not_ready"}` | the pod received SIGTERM: readiness flips first, then the drain delay | log `shutdown: readiness flipped to not-ready; ...` | expected during rollout; it never flips back |
| `/readyz` returns 503 `application/problem+json`, type `.../dependency-unavailable`, detail `not ready: postgres` and/or `not ready: kafka` (projector: `analytics-postgres`) | Postgres does not answer a ping, or no Kafka broker answers a metadata request (2 s bound, verdict cached 3 s) | the WARN `readiness: dependency is not ready` with `dependency` and `error` | restore the dependency; the pod stays alive and is ready again on its own. Missing topics never make it not ready |
| Shutdown logs `kafka consumer did not stop before the shutdown drain deadline` or `outbox relay did not stop ...` | a handler or a relay send was still blocked after 10 s | the lines before it | usually a broker or database stall; the pod still exits. Undelivered outbox rows are sent by the next pod |

## Kafka ingestion

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| No labor or station data although the pod is up | no `ShiftPlanCommitted` / `LocationSlotRegistered` was published to `warehouse.workforce.events` / `warehouse.facility.events`, or the consumers started before the topic existed and kafka-go did not pick it up (inbound-receiving's migration documents a kafka-go group consumer that starts before its topic exists and stays silent until the pod restarts; this repo's end-to-end test creates the input topics first, so it does not exercise that case) | the `consumer running` lines (`consumer`, `topic`, `group_id`); `SELECT count(*) FROM processed_events` | create the topic first (infra owns them), then restart the pod |
| `GET /inbound-demand` shows `appointments: 0`, or `standard_missing: true` and `labour_minutes: 0` | no `DockAppointmentBooked` reached `warehouse.dock-scheduling.events` (consumer `dock-appointments` running?), the appointments are OUTBOUND, cancelled or no-show, or are stored under another `site_id` (the events' own site code, not `DEMAND_SITE_ID`), or no inbound labour standard is declared for that `path_id` (an appointment without one contributes no demand by design, and a plan warns `inbound standard missing`) | `SELECT location, status, count(*) FROM inbound_appointments GROUP BY 1, 2`; `GET /inbound-labour-standards` | produce the events; query with the events' `site_id`; `PUT /inbound-labour-standards/{location}/{path_id}` |
| `GET /capacity-plans/{id}/variance` shows `observed.has_data: false`, or `comparable: false` | no `TaskCompleted` of the plan's bottleneck step inside the plan window (the step is matched upper-case against `task_type`), or `tasks_per_order` was not supplied (tasks are not orders) | `SELECT process_type, sum(tasks_completed) FROM observed_capacity WHERE location = '<site>' GROUP BY 1`; the `consumer running` line with `consumer=observed-capacity` | produce completions; pass `?tasks_per_order=` |
| `GET /demand` always `orders: 0`, `as_of: null` | nothing consumed from `warehouse.order-management.events` yet (the consumer is always on), or the orders are stored under another site | the `consumer running` line with `consumer=order-demand`; `SELECT location, count(*) FROM order_demand GROUP BY 1` | produce `OrderAllocated`; align `DEMAND_SITE_ID` |
| Demand exists in the database but `GET /demand?location=X` is empty | each order is stored under its event's `site_id`, or under `DEMAND_SITE_ID` when the event carries none (ADR 0021); the query uses another site code | `SELECT location, count(*) FROM order_demand GROUP BY 1` | query with a site the rows are stored under (unsited orders: the configured site, in the cluster `SIM1`) |
| Consumer lag grows on one partition, repeated `... handling failed; retrying the same message` | a transient database failure; domain consumers retry 5 times, then dead-letter | the `error`, `partition`, `offset` fields; database health | fix the database; the message is retried or dead-lettered automatically |
| Analytics lag grows forever, repeated `analytics consumer handling failed; retrying the same message` | the projector retries transient failures **without limit** by design (ADR 0005); its partition is blocked | analytical database health, `GET /reports/freshness` | fix the analytical database; nothing is lost |
| `... handling failed after the maximum attempts; dead-lettering the message` | a domain consumer gave up after 5 attempts | the `<topic>.dlq` topic and its `x-dlq-error` header | fix the cause, then re-drive (see [Runbook](/docs/operations/runbook#re-drive-a-dead-lettered-message)) |
| `kafka.(*Writer): Topic must not be specified for both Writer and Message` while dead-lettering, the same offset retried and the partition's lag growing | a dead-letter writer was built with a fixed `Topic` while `publishDeadLetter` also names the DLQ topic on the message; kafka-go refuses every such write (fixed: the domain DLQ writer has no fixed topic) | the consumer's error log and the `<topic>.dlq` topic staying empty | keep the topic on the message only; `TestNewDomainDLQWriter_HasNoFixedTopic` and the real-broker `TestDomainDeadLetter_Integration_RealBrokerReceivesThePoisonMessage` pin it |
| `analytics: dead-lettering a message that can never be projected` | poison on the analytics topic: `plan_id` empty or different from the CloudEvents `subject`, missing `warehouse_id`/`location`, zero `time`, negative `shortage`, undecodable data, or a store rejection | `x-dlq-error` on `warehouse.warehouse-planning.analytics.dlq` | fix the producer; re-drive if the payload can be corrected |
| `analytics: skipping message that is not a CloudEvents 1.0 event ...` | a non-CloudEvents message on the analytics topic | `suppressed_since_last_warning` | find and stop the producer; skips are committed past |
| Labor events consumed but the path capacity still lacks the step | upper-cased `path_id` of `ShiftPlanCommitted` is the process type, `site_code` (else `building_id`) the location; the window is `[event time, event time + planned_hours)` | `GET /process-capacities` with that exact window | plan windows must be covered by the registered window ([ADR 0003](/docs/adr/0003-window-coverage-semantics)) |
| A capacity plan's `labour_requirement` step is `known: false`, `reason: no-labor-standard` | the step is not `PICK`, `PACK` or `SLAM` (labor-performance's task types), or no `LaborStandardDefined`/`Revised` with that `task_type`, no handling unit and an `effective_from` at or before the window start reached `warehouse.labor-performance.events` (consumer `labor-standards` running?) | `SELECT standard_id, task_type, site_code, effective_from FROM labor_standards ORDER BY task_type, effective_from` | define the standard in labor-performance; a plan stored before it keeps its requirement (a revision never rewrites a stored plan, ADR 0022) |
| A step is `known: false`, `reason: no-task-factor` | no `tasks_per_order` factor for the step (only `PICK` with `demand_source = orders` counts released lines instead) | the plan request | pass `tasks_per_order` on `POST /capacity-plans` |

## Publishing (outbox)

| Symptom | Cause | Check | Fix |
| --- | --- | --- | --- |
| Plans created but nothing on `warehouse.warehouse-planning.events` | the relay has not published yet, or the pass keeps failing | `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`; log `outbox relay pass failed` | see the next rows. The first write to an output topic that does not exist yet fails once with `Unknown Topic Or Partition` (the broker creates it) and succeeds on the next pass |
| Rows pile up in `outbox_events` with `published_at IS NULL`, log `outbox relay pass failed` | broker unreachable or rejecting writes; the pass stops at the first failing row | `SELECT id, attempts, last_error FROM outbox_events WHERE published_at IS NULL ORDER BY id LIMIT 10` | fix the broker; the relay retries every `OUTBOX_RELAY_INTERVAL` |
| ERROR `outbox relay: row reached the attempt cap and is excluded from future claims`; one event never reaches Kafka while later ones do | that row failed `OUTBOX_MAX_ATTEMPTS` times (default 20): a poison message or a long broker outage ([ADR 0013](/docs/adr/0013-outbox-relay-attempt-cap)) | `SELECT id, topic, event_type, key, attempts, last_error FROM outbox_events WHERE published_at IS NULL AND attempts >= 20` | fix the cause (`last_error`), then `UPDATE outbox_events SET attempts = 0 WHERE id = ...` ([Runbook](/docs/operations/runbook#outbox-relay)) |
| A consumer's span is not linked to the plan request | the row was written without an active span (MCP call without tracing, in-memory outbox, or a row from before ADR 0014), or the consumer does not `Extract` the Kafka headers | `SELECT headers FROM outbox_events WHERE id = ...` shows no `traceparent` | nothing to fix in this service for old rows; consumers must extract `traceparent` ([ADR 0014](/docs/adr/0014-trace-context-crosses-the-outbox)) |
| A `config.*` chart value has no effect on the `api` | before gap fix D14 the ConfigMap was not mounted | `kubectl get deploy <release>-warehouse-planning -o yaml` shows `envFrom: configMapRef` | upgrade the chart; only the `api` Deployment mounts the ConfigMap |
| Plans created over MCP never reach Kafka | only `cmd/api` runs the relay | is an `api` pod running against the same database? | run the `api` component |
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
| 400 | `malformed-units-per-order`, `malformed-packages-per-order` | non-numeric query value | send a number |
| 400 | `invalid-limit`, `invalid-cursor`, `unknown-query-parameter` | on a collection GET (`/capacity-plans`, `/process-paths`, `/station-standards`, inbound reads): `limit` not a single integer 1 to 500 (default 100), a cursor that is not the one the previous page returned, or a query parameter the collection does not declare (including retired `after`, `from`, `offset`, `nextToken`) | follow `nextCursor` until it is absent; repeat the filters on every page |
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
| 422 | `non-positive-tasks-per-order` | a `tasks_per_order` factor ≤ 0 on `POST /capacity-plans` (ADR 0022) or on the variance read | send a positive factor, or omit the step's factor (its labour requirement is then unknown with reason `no-task-factor`) |
| 500 | `internal-error` | infrastructure failure (database) | read `detail` and the pod logs |

### Workload forecast and labour plan (ADR 0023)

`PUT /workload-forecasts/{site_id}/{path_id}`, `PUT /labour-plans/{site_id}/{shift_id}/{path_id}`
and their reads. Slugs from `internal/adapters/inbound/http/errors.go`,
`workload_forecast_handler.go` and `labour_plan_handler.go`.

| Status | Slug | Cause | Fix |
| --- | --- | --- | --- |
| 400 | `malformed-json` | the body is not JSON, or a fractional `quantity`, `planned_heads` or `installed_stations` | send whole numbers |
| 400 | `malformed-cpt` | a bucket `cpt` that is not RFC 3339 | send `2026-11-19T07:00:00Z` (any offset is accepted and answered in UTC) |
| 400 | `malformed-shift-start` | a `shift_start` that is not RFC 3339 | as above |
| 400 | `missing-site-id`, `missing-shift-id` | `GET /labour-plans` without `site_id` or `shift_id` | send both, on every page |
| 400 | `invalid-limit`, `invalid-cursor`, `unknown-query-parameter` | as for the other collection GETs | follow `nextCursor` and repeat both filters |
| 404 | `workload-forecast-not-found` | no forecast was received for that `(site_id, path_id)`; `site_id` matches exactly as sent; `path_id` is case-insensitive (`pick` finds `PICK`) | `PUT` the forecast, or read with the site it was sent under |
| 404 | `labour-plan-not-found` | no plan was committed for that `(site_id, shift_id, path_id)`; `site_id` and `shift_id` match exactly as sent, `path_id` is case-insensitive | as above |
| 409 | `version-conflict` | another writer moved the stored version twice while this `PUT` was applied (the use case re-runs once on the first conflict) | retry the request; the `PUT` is idempotent |
| 422 | `no-forecast-buckets` | `buckets` empty or absent | send at least one bucket |
| 422 | `missing-cpt`, `missing-bucket-quantity` | a bucket without `cpt` or without `quantity` | send both on every bucket |
| 422 | `negative-bucket-quantity` | a bucket `quantity` below zero | 0 or more units |
| 422 | `missing-forecast-field`, `missing-labour-plan-field` | a blank `site_id` or `path_id` (forecast), or a blank `site_id`, `shift_id` or `path_id` (plan) | send non-blank ids |
| 422 | `missing-installed-stations` | `installed_stations` omitted | send it; it is stored as sent |
| 422 | `invalid-count` | `planned_heads` or `installed_stations` below 0 or above 2147483647, or derived heads above that bound | send a count in range |
| 422 | `heads-exceed-stations` | `planned_heads` (sent or derived) above `installed_stations` | raise `installed_stations`, lower the heads, or lower the forecast / raise the rate |
| 422 | `non-positive-rate`, `non-positive-hours` | `rate_units_per_hour` or `hours` not above zero (checked before any derivation, so a bad `hours` is never reported as missing forecast data) | send positive numbers |
| 422 | `throughput-not-finite` | `rate_units_per_hour x planned_heads x hours` overflows | send realistic numbers |
| 422 | `missing-planned-heads` | `planned_heads` omitted and it cannot be derived: no `shift_start`, no stored forecast for `(site_id, path_id)`, or no forecast bucket with a `cpt` in `[shift_start, shift_start + hours)` (zero buckets is no data, never zero heads) | send `planned_heads`, send `shift_start`, or `PUT` a forecast with a bucket inside the shift; `GET /workload-forecasts/{site_id}/{path_id}` shows the stored buckets |

### Labour plan answers that look wrong

| Symptom | Cause | Fix |
| --- | --- | --- |
| Derived heads are lower than expected | only buckets with `cpt` in `[shift_start, shift_start + hours)` count: a bucket exactly at the shift end belongs to the next shift, one before `shift_start` to the previous one | check the bucket times against the window; `ceil(units / (rate x hours))` |
| No warning although `installed_stations` is above the real station count | the warning needs a facility-layout tally that knows **at least one** station of the activity named like the path (path id upper-cased) at the site; zero tallied stations is no tally data | check `GET /storage-capacity?location=<site>`; facility-layout must have registered `WorkCenter` slots with that activity |
| The warning went away without a new `PUT` | warnings are computed against the tally on every read, never stored | expected: the tally changed |
| A `PUT` answered `200` but `version` did not move and no event was published | the plan (or the buckets, in the same order) equal the stored ones: nothing is written and nothing is raised | expected; change a field to publish |
| workforce-management does not show the new requirement | it applies a `LabourPlanCommitted` only when `version` is greater than its stored one (its ADR 0043) | check the outbox row was published, then that consumer's lag |

### Labour plan drift: why was nothing raised

`LabourPlanDriftDetected` (ADR 0023 decision 3) is raised by two triggers: a
`ShiftPlanCommitted` line (second step of the labor consumer's claim and
LABOR unit of work) and a labour plan `PUT` that **changed** the plan. It is
raised only when `committed_heads - planned_heads` is non-zero **and**
differs from the last drift recorded for the key. Every computed drift is
recorded in `labour_plan_commitments.last_drift_heads`, zero included.
Inspect a key with
`SELECT committed_heads, observed_at, last_drift_heads FROM labour_plan_commitments WHERE site_id = '<site>' AND shift_id = '<shift>' AND path_id = '<path>'`.

| Symptom | Cause | Fix |
| --- | --- | --- |
| No drift although the heads differ | the same drift was already raised for the key (`last_drift_heads` equals it): a repeat is not news | expected; a different drift, or a drift that clears to 0 and returns, is raised again |
| No drift after a labour plan `PUT` | the `PUT` changed nothing, so no trigger ran | expected; the drift is surfaced only by a changed plan |
| No drift after a `ShiftPlanCommitted`, no commitment row either | the line carried no `shift_id` (it still registers the LABOR constraint), or the event id was already claimed (a redelivery does nothing) | the producer must send `shift_id`; `SELECT * FROM processed_events WHERE consumer = 'labor-capacity-consumer' AND event_id = '<id>'` |
| A commitment row exists, `last_drift_heads` is NULL | no labour plan existed for the key when the commitment arrived | expected: the next plan `PUT` for that key compares and raises |
| The plan and the commitment are both there, but they never meet | the site (`site_code`, else `building_id`) and `shift_id` are compared exactly as sent (trimmed on the consumer side). `path_id` is NOT the cause: it is trimmed and upper-cased on both sides (`labourplan.CanonicalPathID`), so a plan under `pick` meets a commitment under `PICK` | send the same site and shift ids on both sides |
| Log `skipping the labour plan drift of a ShiftPlanCommitted with an invalid commitment` | the line's heads were below 0 (or above 2147483647); the LABOR step was still processed | fix the producer |
| A drift was raised although the LABOR constraint was skipped (`skipping ShiftPlanCommitted that failed domain validation`) | accepted limit: the commitment is workforce-management's fact, so it is recorded whatever the LABOR registration decided | none; the two steps are independent |
| `committed_heads` went back to an older value | accepted limit: there is no stale-event guard on `observed_at`; the per-key Kafka order of workforce-management's topic is relied on | the producer must keep one key on one partition |
| The very first commitment and the very first plan of a key arrived together and no drift was raised | accepted limit: each unit of work ran before the other committed, so neither saw the other side | the next change on either side raises it |
| A drift appears on the topic but nothing changed in the plan or in workforce-management | expected: the drift is surfacing only; it never corrects the plan or the commitment | a planner decides: revise the plan or ask workforce-management to commit again |

## Capacity answers that look wrong

| Symptom | Cause | Fix |
| --- | --- | --- |
| A response carries a warning about stations without a declared standard | stations are tallied for a step but no `StationStandard` exists; the step uses its other constraints | declare the standard (`PUT /station-standards/{location}/{process_type}`) |
| The bottleneck is `STATION` after a standard was declared | count × standard is now lower than labor | expected ([ADR 0002](/docs/adr/0002-station-capacity-composition)) |
| Registering a constraint again does not add up | re-registering a constraint type for the same process, location and window **replaces** its rate | register the total, not an increment |
| Browser calls from the `web/` dev server fail CORS | the API allows only `CORS_ALLOWED_ORIGINS` (default `http://localhost:5173`) | start the API with `CORS_ALLOWED_ORIGINS=http://localhost:5190` |
