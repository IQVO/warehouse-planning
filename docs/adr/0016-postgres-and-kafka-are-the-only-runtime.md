# ADR 0016: Postgres and Kafka are the only runtime

## Status

Accepted (2026-10-19). Fleet decision of 2026-10-19: "everything must use real
database and broker". Amends (does not rewrite) the in-memory, `log` and
opt-in statements of [ADR 0004](./0004-demand-ingestion-from-order-management.md)
(the demand consumer's off switch), [ADR 0007](./0007-outbox-and-resilient-consumers.md)
(`LogSink` as the default publisher) and [ADR 0008](./0008-mcp-server-adoption.md)
(`cmd/mcp` on in-memory repositories without `DATABASE_URL`). No CloudEvents
type, payload, topic or REST contract changes.

## Context

Until this ADR every binary had a degraded default that looked healthy:

- `cmd/api` and `cmd/mcp` without `DATABASE_URL` booted on the in-memory
  repositories (`memory.NewProcessCapacityRepo`, `memory.NewOutboxRepo`, ...):
  no persistence, no idempotency table, an outbox nobody could read back.
- `EVENT_PUBLISHER` defaulted to `log`: the relay drained the outbox into a
  log line and marked every row published, so a deployment without the setting
  silently lost every event (ADR 0007).
- `KAFKA_BROKERS` was optional: without it the labor and storage consumers were
  "disabled (logged, not fatal)" and the order-demand consumer stayed off
  unless `DEMAND_CONSUMER_GROUP` was set (ADR 0004).
- The labor and storage consumer groups defaulted to the names the in-cluster
  pod uses, so a process run locally joined the live group.
- `cmd/planning-projector` defaulted `ANALYTICS_CONSUMER_GROUP`, and its
  `/readyz` only reported the shutdown flag; `cmd/planning-reports` had no
  `/readyz`.
- `/readyz` of `cmd/api` only reported the shutdown flag, so a pod whose
  Postgres or Kafka was gone stayed "ready".
- `cmd/api` stayed alive with a dead consumer or relay (the failure mode found
  by inbound-receiving #48).

## Decision

1. **No in-memory runtime path in `cmd/**`.** `cmd/api`, `cmd/mcp`,
   `cmd/planning-projector` and `cmd/planning-reports` run on real Postgres
   and real Kafka only. The adapters in `internal/adapters/outbound/memory`
   (and `analyticsstore.NewMemory`) stay as TEST DOUBLES for unit and BDD
   tests. `TestRuntimeBinaryHasNoInMemoryAdapters` fails when a non-test file
   under `cmd/**` imports the memory package; `TestRuntimeBinarySensorBites`
   proves the detector.
2. **Mandatory configuration is a boot error that names the variable, before
   the listener binds** (`loadRuntime` in `cmd/api/runtime.go`):
   `DATABASE_URL`, `KAFKA_BROKERS`, `LABOR_CAPACITY_CONSUMER_GROUP`,
   `STORAGE_CAPACITY_CONSUMER_GROUP`, `DEMAND_CONSUMER_GROUP`, `DEMAND_SITE_ID`.
   `cmd/mcp` requires `DATABASE_URL`; `cmd/planning-projector` requires
   `ANALYTICS_DATABASE_URL`, `KAFKA_BROKERS` and `ANALYTICS_CONSUMER_GROUP`.
   No consumer group has a default any more.
3. **`EVENT_PUBLISHER` stays only because the chart and warehouse-infra set
   it**: unset or `kafka` is accepted, `log` and any other value are boot
   errors (`EVENT_PUBLISHER=log is not supported: ...`).
4. **Consumers and relay are always on.** The order-demand consumer loses its
   off switch (this amends ADR 0004). There is no `*_MODE` variable in this
   repo, so nothing needed the `permissive`/`off` rejection other siblings
   added.
5. **`/readyz` is real.** `cmd/api`: drain gate AND a Postgres ping AND a Kafka
   broker answering a metadata request. The request names no topics (an empty,
   non-nil list), so the answer never depends on a topic existing: the relay
   writer sets `AllowAutoTopicCreation` and the service never requires or
   creates topics. The verdict is cached 3 s and bounded 2 s. A failed
   dependency is `503 application/problem+json`, type `dependency-unavailable`,
   `detail: "not ready: postgres"` / `"not ready: kafka"`. `/healthz` stays
   liveness. `cmd/planning-projector`: drain gate, analytical Postgres ping and
   Kafka metadata. `cmd/planning-reports` gains `/readyz`: drain gate and the
   analytical Postgres ping.
6. **`cmd/api` exits non-zero, naming the worker, when the relay or a consumer
   returns on its own** (`<worker> stopped unexpectedly: exiting so the
   orchestrator restarts the service`), mirroring inbound-receiving #48. The
   graceful-shutdown order is unchanged.
7. **`rejectNUL`.** A NUL (U+0000) in the path, the query or a JSON body reaches
   a Postgres text parameter as SQLSTATE 22021 and then 25P02: a 500 on the
   real runtime that the in-memory adapters hid (found by facility-layout #209).
   The router now answers `404 resource-not-found` (path), `400
   malformed-request-body` (body, raw byte or `\u0000` escape) and `400
   malformed-request-query` before any handler runs. This service has no
   Idempotency-Key middleware, so no key can be consumed by a rejected request.
8. **Chart.** `database.url`/`database.existingSecret`, `kafka.brokers`, the
   three consumer groups, `demand.siteId` (and, with analytics on,
   `analytics.projector.consumerGroup`) are required: the render fails naming
   the value (`requireRealRuntime`). `kafka.enabled` is accepted only as `true`
   (warehouse-infra still sets it) and `config.eventPublisher` only as `kafka`.
   `KAFKA_BROKERS` and every consumer variable are always rendered. The reports
   readiness probe moves to `/readyz`. `charts/warehouse-planning/tests/
   warehouse-infra-*-values.yaml` are verbatim copies of warehouse-infra's
   `origin/develop` values and `render_test.sh` renders them and asserts that
   every mandatory variable is provided.
9. **Local runs.** `docker-compose.yml` brings up Postgres and a single-node
   Kafka (it must advertise `PLAINTEXT_HOST://localhost:<port>`, or the first
   produce fails on the inter-broker address).

## Consequences

- A misconfigured pod now refuses to start (and the chart refuses to render)
  instead of coming up healthy and losing events. warehouse-infra's current
  values already provide every mandatory variable (checked by
  `render_test.sh`); no infra change is needed.
- `GET /demand` no longer has an "off" state: with the consumer always on, the
  expected-demand model fills as soon as order-management publishes.
- A Postgres or Kafka outage turns the pod not-ready (traffic stops) without
  restarting it; a consumer or relay that dies makes the process exit.
- Kafka behaviour seen by the end-to-end test (`cmd/api/e2e_integration_test.go`,
  which boots the real `run()` against testcontainers): the first relay write
  to an output topic that does not exist yet fails once with `Unknown Topic Or
  Partition` and succeeds on the next pass (the broker creates the topic). The
  test creates the three input topics before booting, as the cluster has them;
  it therefore does not exercise inbound-receiving's finding that a kafka-go
  group consumer started BEFORE its topic exists does not pick the topic up
  until a restart (ADR 0011 there). That behaviour is a kafka-go property, not
  solved here.
- The `memory` package remains in `internal/adapters/outbound/memory` and the
  BDD suite and use-case tests keep using it.

## Not in scope

- There is no `contract` (Schemathesis) job in `.github/workflows/ci.yml` or the
  Makefile, so no CI job was invented or moved; the NUL guard is covered by
  unit tests and the end-to-end test instead.
- This service has no REST/MCP sync edge to a sibling context (`*_MODE=http`),
  so there is nothing to leave untouched on that side.
- `cmd/mcp` has no `/readyz` (its probes stay `/healthz`) and dials no Kafka:
  it only writes outbox rows that `cmd/api` relays. A Postgres-aware MCP
  readiness probe is a follow-up.
