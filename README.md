# warehouse-planning

[![CI](https://github.com/IQVO/warehouse-planning/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/IQVO/warehouse-planning/actions/workflows/ci.yml?query=branch%3Adevelop) [![coverage gate](https://img.shields.io/badge/coverage%20gate-90%25-brightgreen)](https://github.com/IQVO/warehouse-planning/blob/develop/Makefile) [![Go](https://img.shields.io/badge/go-1.27.3-00ADD8?logo=go&logoColor=white)](https://github.com/IQVO/warehouse-planning/blob/develop/go.mod) [![release](https://img.shields.io/github/v/release/IQVO/warehouse-planning?sort=semver)](https://github.com/IQVO/warehouse-planning/releases) [![docs](https://img.shields.io/badge/docs-iqvo.github.io-blue)](https://iqvo.github.io/warehouse-planning/) [![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/IQVO/warehouse-planning/blob/develop/LICENSE)

The **Warehouse Planning** bounded context of the `warehouse-systems` fleet —
a **Core Domain**. It answers: **can this warehouse process the demand
assigned to it**, given its current labor, location, equipment, station,
conveyor and buffer constraints? This is distinct from Inventory Management
("what do we have") and from every other fleet context, none of which
computes normalized, cross-process effective capacity or forward-looking
capacity shortages.

It owns the `ProcessPath` and `ProcessCapacity` aggregates (the registered
capacity facts), the `CapacityPlan` aggregate (draft/publish, shortage,
bottleneck), station and inbound-labour standards, storage-capacity tallies
and the demand read models. It does **not** own execution (`fulfillment-execution`,
`wes-work-planning`), not the warehouse map (`facility-layout` — consumed),
not workforce scheduling (`workforce-management` — consumed), not inventory.
It is an **Open Host Service** with a **Published Language**: its REST API,
its 14 MCP tools and its events on `warehouse.warehouse-planning.events`;
its downstream conformists today are `warehouse-console`'s capacity remote
(`web/`) and `warehouse-ops-agent` (MCP). Hexagonal Go, Postgres, REST,
Kafka (CloudEvents 1.0).

See `CLAUDE.md` for the full repo guide and
`.claude/rules/domain-model.md` for the ubiquitous language/aggregates.
Scaffolded from `warehouse-harness-template: v3` (sensor manifest:
`HARNESS.md`).

## Disclaimer

This project is a domain-driven design study. It is not associated with,
endorsed by or affiliated with any company, and it uses no company's code,
data, logos or trademarks. All names, figures and examples are illustrative
or synthetic.

---

## Documentation

Published at **<https://iqvo.github.io/warehouse-planning/>** (sources under
`docs/docs/` and `docs/adr/`; PRs build the site as a check, `main` deploys
via `docs.yml`; the `docs-api-drift` CI job fails a PR whose generated REST
reference differs from `apis/openapi.yaml`).

| Area | Pages |
|---|---|
| Overview | [Introduction](docs/docs/intro.md), [Bounded context](docs/docs/overview/context.md), [Runtime and binaries](docs/docs/overview/runtime.md), [Quickstart](docs/docs/overview/quickstart.md), [Aggregates](docs/docs/overview/aggregates.md), [Capacity composition](docs/docs/overview/capacity-composition.md) |
| Operations | [Configuration](docs/docs/operations/configuration.md) (every env var per binary), [Runbook](docs/docs/operations/runbook.md), [Observability](docs/docs/operations/observability.md), [Troubleshooting](docs/docs/operations/troubleshooting.md) |
| Development | [Testing](docs/docs/development/testing.md) (test pyramid, `make` targets, CI jobs) |
| Domain-Driven Design | [Use cases](docs/docs/ddd/use-cases.md), [Subdomain classification](docs/docs/ddd/subdomain-classification.md), [Ubiquitous language](docs/docs/ddd/ubiquitous-language.md), [DDD artifacts index](docs/docs/ddd/ddd-artifacts.md) |
| Ecosystem | [Integration contracts](docs/docs/ecosystem/upstream-contracts.md) (every upstream, downstream and caller) |
| Reference | [API overview](docs/docs/api-reference/overview.md), [Event catalogue](docs/docs/api-reference/events.md), [MCP tools](docs/docs/mcp/tools.md), [A2A inbound adapter (ADR 0018)](docs/adr/0018-a2a-inbound-adapter.md), [ADR index](docs/adr/index.md) |

---

## Architecture

Hexagonal / ports & adapters; dependencies point inward only:

```
cmd/api/                      composition root: REST service, outbox relay, the five fact consumers
cmd/mcp/                      MCP server (Streamable HTTP — read AND write tools)
cmd/a2a/                      A2A agent (JSON-RPC over HTTP, Agent Card at /.well-known/agent-card.json)
cmd/planning-projector/       analytics projector (the only analytical-DB writer)
cmd/planning-reports/         analytics reader (serves the /reports routes)
internal/
  domain/                     pure business logic
    processpath/              ProcessPath aggregate (steps, standards)
    processcapacity/          ProcessCapacity aggregate (registered capacity constraints)
    capacityplan/             CapacityPlan aggregate (DRAFT -> PUBLISHED, shortage, bottleneck)
    demand/                   expected-demand read model (order events)
    inbound/                  inbound labour standards + inbound demand (dock appointments, ADR 0017)
    observed/                 observed-capacity feedback (task completions, ADR 0017)
  application/
    ports/                    OUT interfaces only (repos, EventPublisher, UnitOfWork, Clock, PlanMetrics)
    usecases/                 one struct per use case
    tally/                    the capacity-composition math (window coverage, ADR 0002/0003)
    outbox/                   transactional outbox (ADR 0007)
  adapters/
    inbound/http/             chi handlers, DTOs, RFC 7807 error mapping, otelchi spans
    inbound/mcp/              the 14 MCP tools
    inbound/a2a/              A2A skills: path_capacity (read), evaluate_capacity_plan (write)
    inbound/kafka/            the five fact consumers + the projector's analytics consumer
    kafka/cloudevents/        the CloudEvents 1.0 envelope
    outbound/postgres/        pgxpool repos, golang-migrate runner (migrations under outbound/postgres/migrations), UnitOfWork, outbox + relay
    outbound/kafka/           integration + analytics topic publishers
    outbound/analyticsstore/  analytical DB projection and report queries
    outbound/memory/          in-memory test doubles (never linked into a binary)
    outbound/telemetry/       OpenTelemetry wiring + the business metric (ADR 0011)
  bootretry/                  bounded boot retry with backoff
  analytics/report/           the reports data product model
analytics/migrations/         golang-migrate SQL for the analytical DB
```

The rule is enforced as an executable test, not a convention:
`internal/architecture/architecture_test.go` (arch-go) fails the build on any
layering violation, and its sibling fleet fitness tests keep the invariants
that matter here: `runtime_no_memory_fitness_test.go`,
`kafka_writer_topic_fitness_test.go`, `kafka_writer_acks_fitness_test.go`,
`kafka_dlq_shutdown_fitness_test.go`, `outbox_relay_cap_fitness_test.go`,
`events_fitness_test.go`, `catalogue_fitness_test.go`,
`mcp_optional_inputs_test.go` and `violation_test.go`.

---

## Domain events

Six published event types, all through the transactional outbox (ADR 0007)
to BOTH `warehouse.warehouse-planning.events` (the integration topic) and
`warehouse.warehouse-planning.analytics` — the analytics twin carries **plan
facts only** (ADR 0005): `CapacityPlanCreated`,
`CapacityPlanPublished`, `CapacityShortageDetected` and `BottleneckDetected`
(entity `capacityplan`). `ProcessCapacityRegistered` and
`ProcessCapacityChanged` (entity `processcapacity`) publish on the
integration topic only. All are specified in `apis/asyncapi.yaml`.

Every Kafka message on both topics is a **CloudEvents 1.0** event in
structured content mode (ADR 0006), header
`content-type: application/cloudevents+json; charset=UTF-8`, `subject` =
the plan id, `dataschema` =
`urn:warehouse:warehouse-planning:<events|analytics>:<EventName>:v1`.
CloudEvents `type` convention, identical to the other warehouse-systems
services — this context's subdomain segment is `wes`:

```
com.warehouse.wes.warehouse-planning.<entity>.<EventName>

com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated
com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityRegistered
```

W3C trace context crosses the outbox with the event (ADR 0014). Consumed
facts (one pinned consumer group each; DLQ at `<topic>.dlq`):

| Topic | Types consumed | Consumer group env |
|---|---|---|
| `warehouse.workforce.events` | `ShiftPlanCommitted` | `LABOR_CAPACITY_CONSUMER_GROUP` |
| `warehouse.facility.events` | `LocationSlotRegistered`, `LocationSlotDecommissioned` | `STORAGE_CAPACITY_CONSUMER_GROUP` |
| `warehouse.order-management.events` | `OrderAllocated`, `OrderPartiallyAllocated` | `DEMAND_CONSUMER_GROUP` |
| `warehouse.inbound-receiving.events` | `DockAppointmentBooked`, `DockAppointmentCancelled` | `INBOUND_DEMAND_CONSUMER_GROUP` |
| `warehouse.fulfillment.events` | `TaskCompleted` | `OBSERVED_CAPACITY_CONSUMER_GROUP` |

The projector consumes `warehouse.warehouse-planning.analytics` under
`ANALYTICS_CONSUMER_GROUP`.

---

## Running it

Postgres and Kafka are the only runtime ([ADR 0016](docs/adr/0016-postgres-and-kafka-are-the-only-runtime.md)):
there is no in-memory mode, and `cmd/api` exits at boot, naming every
missing variable, unless `DATABASE_URL`, `KAFKA_BROKERS`,
`LABOR_CAPACITY_CONSUMER_GROUP`, `STORAGE_CAPACITY_CONSUMER_GROUP`,
`DEMAND_CONSUMER_GROUP`, `INBOUND_DEMAND_CONSUMER_GROUP`,
`OBSERVED_CAPACITY_CONSUMER_GROUP` and `DEMAND_SITE_ID` are set
(`EVENT_PUBLISHER=log` is refused).

```bash
docker compose up -d --wait   # Postgres :55432 (planning/planning/planning) + Kafka :9092
                              # (KAFKA_HOST_PORT=19092 picks a free broker port)
export DATABASE_URL='postgres://planning:planning@localhost:55432/planning?sslmode=disable'
export KAFKA_BROKERS=localhost:9092 DEMAND_SITE_ID=SIM1
export LABOR_CAPACITY_CONSUMER_GROUP=wp-local-labor STORAGE_CAPACITY_CONSUMER_GROUP=wp-local-storage DEMAND_CONSUMER_GROUP=wp-local-demand INBOUND_DEMAND_CONSUMER_GROUP=wp-local-inbound-demand OBSERVED_CAPACITY_CONSUMER_GROUP=wp-local-observed-capacity
go run ./cmd/api              # migrations run automatically at startup; listens on :8080
```

`cmd/mcp` needs `DATABASE_URL` (and runs its migrations) but no Kafka.

### Configuration

`cmd/api`:

| Env var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | **required** | Postgres DSN; unset ⇒ boot error naming it |
| `KAFKA_BROKERS` | **required** | comma-separated broker list (outbox relay, consumers, `/readyz`) |
| `LABOR_CAPACITY_CONSUMER_GROUP` | **required** | pinned group id on `warehouse.workforce.events` |
| `STORAGE_CAPACITY_CONSUMER_GROUP` | **required** | pinned group id on `warehouse.facility.events` |
| `DEMAND_CONSUMER_GROUP` | **required** | pinned group id on `warehouse.order-management.events` |
| `INBOUND_DEMAND_CONSUMER_GROUP` | **required** | pinned group id on `warehouse.inbound-receiving.events` (ADR 0017) |
| `OBSERVED_CAPACITY_CONSUMER_GROUP` | **required** | pinned group id on `warehouse.fulfillment.events` (ADR 0017) |
| `DEMAND_SITE_ID` | **required** | the one site every consumed order, dock appointment and task completion is attributed to (order events carry no site) |
| `EVENT_PUBLISHER` | *(unset = `kafka`)* | only `kafka` is accepted; `log` is refused at boot (ADR 0016) |
| `HTTP_ADDR` | `:8080` | REST listen address |
| `OUTBOX_RELAY_INTERVAL` | `1s` | Go duration between relay passes |
| `OUTBOX_MAX_ATTEMPTS` | `20` | attempts before the relay excludes a row (`0` = unlimited, ADR 0013) |
| `SHUTDOWN_DRAIN_DELAY` | `5s` | wait after flipping `/readyz` before closing the listener (`0` = none) |
| `MIGRATIONS_PATH` | `internal/adapters/outbound/postgres/migrations` | directory of golang-migrate SQL files |
| `MIGRATIONS_DATABASE_URL` | `DATABASE_URL` | direct (non-PgBouncer) DSN for the boot migrations (ADR 0010) |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` (`warning` accepted), case-insensitive |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | OTel Collector (OTLP/gRPC); see [Observability](#observability) |
| `SERVICE_VERSION` | `dev` | `service.version` resource attribute |
| `ENVIRONMENT` | `local` | `deployment.environment.name` resource attribute |
| `CORS_ALLOWED_ORIGINS` | `http://localhost:5173` | comma-separated browser origins allowed to call the API (the `web/` remote dev origin must be listed) |

`cmd/mcp`: `MCP_ADDR` (`:8090`), `DATABASE_URL` (**required**),
`MIGRATIONS_DATABASE_URL`, `MIGRATIONS_PATH`, `OTEL_EXPORTER_OTLP_ENDPOINT`,
`SERVICE_VERSION`, `LOG_LEVEL`.
`cmd/a2a`: `A2A_ADDR` (`:8093`), `DATABASE_URL` (**required**),
`A2A_AGENT_URL` (**required**, absolute http(s) URL advertised on the Agent
Card), `MIGRATIONS_DATABASE_URL`, `MIGRATIONS_PATH`,
`OTEL_EXPORTER_OTLP_ENDPOINT`, `SERVICE_VERSION`, `LOG_LEVEL`.
`cmd/planning-projector`: `ANALYTICS_DATABASE_URL` (**required**),
`KAFKA_BROKERS` (**required**), `ANALYTICS_CONSUMER_GROUP` (**required**),
`ADMIN_ADDR` (`:8091`), `OTEL_EXPORTER_OTLP_ENDPOINT`, `SERVICE_VERSION`,
`LOG_LEVEL`.
`cmd/planning-reports`: `ANALYTICS_DATABASE_URL` (**required**),
`HTTP_ADDR` (`:8092`), `OTEL_EXPORTER_OTLP_ENDPOINT`, `SERVICE_VERSION`,
`LOG_LEVEL`.

### Analytics read side

An additive analytical read side (ADR 0005): the four capacity-plan events
are written to `warehouse.warehouse-planning.analytics` in the same outbox
transaction, `cmd/planning-projector` (admin `:8091`) projects them into a
SEPARATE analytical database (applying `analytics/migrations` itself), and
`cmd/planning-reports` (`:8092`, read-only pool) serves the reports:

```bash
# 1. OLTP service fanning events onto both topics (the environment above)
go run ./cmd/api

# 2. Projector: the only writer of the analytical database
export ANALYTICS_DATABASE_URL='postgres://projector@localhost:55432/warehouse_planning_analytics?sslmode=disable'
export ANALYTICS_CONSUMER_GROUP=wp-local-analytics   # required, no default
go run ./cmd/planning-projector

# 3. Reports: read-only
export ANALYTICS_DATABASE_URL='postgres://reports_ro@localhost:55432/warehouse_planning_analytics?sslmode=disable'
go run ./cmd/planning-reports
curl 'http://localhost:8092/reports/shortage-trend?from=2026-10-01T00:00:00Z&to=2026-10-08T00:00:00Z'
```

Report routes: `GET /reports/{bottleneck-frequency,shortage-trend,plan-throughput,freshness}`
(`from`/`to` optional RFC 3339, from inclusive, to exclusive, default the 30
days ending now, at most 366 days). The `-tags=integration` suite starts
real Postgres and Kafka with testcontainers and covers the whole path (plan
created and published → outbox → relay → Kafka → projector → analytical DB
→ report).

---

## Observability

All five binaries wire OpenTelemetry through
`internal/adapters/outbound/telemetry` (an outbound adapter; the domain
layer never imports it): a TracerProvider and a MeterProvider, both
exporting over **OTLP/gRPC**, Go runtime metrics, and the W3C
trace-context + baggage propagator (ADR 0014, ADR 0015).

| Env var | Default | Meaning |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `localhost:4317` | Collector's gRPC receiver |
| `SERVICE_VERSION` | `dev` | `service.version` resource attribute |
| `ENVIRONMENT` | `local` | `deployment.environment.name` resource attribute |
| `LOG_LEVEL` | `info` | minimum level of the JSON `slog` logs on stdout |

Export is deliberately non-blocking: with no Collector listening, spans and
metrics are dropped silently and the service starts and serves exactly as it
would without OTel. Metrics push to the Collector every 30 s.

**Traces** — one server span per HTTP request via
[`otelchi`](https://github.com/riandyrn/otelchi) (span name = the chi route
pattern, never the raw path); W3C `traceparent` is extracted from inbound
requests and injected outbound, and the outbox carries the trace context
onto Kafka with each event (ADR 0014).

**Metrics** — `http.server.request.duration` (otelchi, per OTel HTTP
semantic conventions), Go runtime metrics, and the **business metric**
(ADR 0011): the Int64 counter
`warehouse_planning.capacity_plans.created` with attribute
`outcome` = `created` | `rejected`, recorded in the `CreateCapacityPlan` use
case (not in the handler), so every path that creates a plan counts alike.

**Logs** — structured `log/slog` JSON on stdout, one line per event, level
from `LOG_LEVEL`.

**Probes** — `GET /healthz` (liveness only) and `GET /readyz` (the drain
gate plus a Postgres ping plus a Kafka broker check).

---

## API

Errors are **RFC 7807** (`application/problem+json`) throughout. Every route
below is declared at the same path in `apis/openapi.yaml`. There is no
`Idempotency-Key` middleware on this API (unlike some siblings); write
idempotency is the consumed-events `processed_events` table.

Write side:

| Method | Path | Purpose | Success |
|---|---|---|---|
| `POST` | `/process-capacities` | RegisterProcessCapacityConstraint | `201` + `Location` |
| `POST` | `/process-paths` | RegisterProcessPath | `201` + `Location` |
| `POST` | `/capacity-plans` | CreateCapacityPlan (DRAFT; shortage + bottleneck) | `201` + `Location` |
| `POST` | `/capacity-plans/{id}/publish` | PublishCapacityPlan | `200` |
| `PUT` | `/station-standards/{location}/{process_type}` | DeclareStationStandard | `200` |
| `PUT` | `/inbound-labour-standards/{location}/{path_id}` | DeclareInboundLabourStandard | `200` |

Read side (`cmd/api`, `:8080`):

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/process-capacities` | GetEffectiveProcessCapacity (the composed effective capacity) |
| `GET` | `/process-paths` | ListProcessPaths (the read the console remote uses) |
| `GET` | `/process-paths/{id}/capacity` | GetProcessPathCapacity |
| `GET` | `/capacity-plans?location=&limit=` | ListCapacityPlans (newest first, default 20, max 100 — the read the console remote uses) |
| `GET` | `/capacity-plans/{id}` | GetCapacityPlan |
| `GET` | `/capacity-plans/{id}/variance` | GetCapacityPlanVariance |
| `GET` | `/station-standards` | ListStationStandards |
| `GET` | `/storage-capacity` | GetStorageCapacity (facility-layout-fed tally) |
| `GET` | `/demand` | GetExpectedDemand (order-fed read model) |
| `GET` | `/inbound-labour-standards` | ListInboundLabourStandards |
| `GET` | `/inbound-demand` | GetInboundDemand (dock-appointment fed) |
| `GET` | `/healthz` / `/readyz` | liveness / readiness |

Reports (`cmd/planning-reports`, `:8092`): `GET /reports/bottleneck-frequency`,
`GET /reports/shortage-trend`, `GET /reports/plan-throughput`,
`GET /reports/freshness`.

`apis/openapi.yaml` (OpenAPI 3.0.3) documents every route with full schemas
and a shared `Problem` component; `apis/asyncapi.yaml` (AsyncAPI 2.6)
specifies both topics and all messages. The OpenAPI document is linted in CI
(`api-lint`, Spectral).

---

## Helm chart

```sh
helm install warehouse-planning ./charts/warehouse-planning \
  --set database.url="postgres://planning:***@postgres:5432/planning?sslmode=disable" \
  --set kafka.brokers="kafka:9092" \
  --set demand.siteId=SIM1
```

The chart (OLTP `api` component; optional `mcp`, `a2a`, `frontend` and
analytics (`analytics-projector`, `analytics-reports`) components, all off
by default) refuses to render without `database.url` or
`database.existingSecret`, `kafka.brokers`, the three consumer groups
(`config.laborCapacityConsumerGroup`, `config.storageCapacityConsumerGroup`,
`demand.consumerGroup`) and `demand.siteId` (and, with analytics on,
`analytics.projector.consumerGroup`): Postgres and Kafka are the only
runtime ([ADR 0016](docs/adr/0016-postgres-and-kafka-are-the-only-runtime.md)).
The analytical DSN comes from `analytics.databaseUrl` or
`analytics.existingSecret`. Run:

```sh
helm lint charts/warehouse-planning -f charts/warehouse-planning/ci/default-values.yaml
bash charts/warehouse-planning/tests/render_test.sh     # also renders warehouse-infra's current values
python3 charts/warehouse-planning/tests/test_service_selectors.py
```

`helm-lint` in CI runs `ct lint --charts charts/warehouse-planning`; the
`chart-tests` job runs the Python wiring tests. The kind cluster is wired by
`warehouse-infra` (`local.services`); ArgoCD deploys the chart from this
repo's `develop`.

### Running the MCP server in Kubernetes

`cmd/mcp` (ADR 0008) serves **14 tools** over Streamable HTTP on `:8090`
(`/` and `/mcp`, `GET /healthz`, no auth) and is a separate, opt-in
deployable (`mcp.enabled=true` renders a `<release>-mcp` Deployment +
ClusterIP Service; it reads the same `DATABASE_URL` secret and runs the OLTP
migrations on start). Reads: `get_capacity_plan`,
`get_capacity_plan_variance`, `get_effective_process_capacity`,
`get_expected_demand`, `get_inbound_demand`, `get_process_path_capacity`,
`get_storage_capacity`, `list_station_standards`. Writes (this surface is
NOT read-only, unlike most siblings): `create_capacity_plan`,
`publish_capacity_plan`, `declare_inbound_labour_standard`,
`declare_station_standard`, `register_process_capacity_constraint`,
`register_process_path`. See `.claude/rules/mcp.md` and
[the MCP tools page](docs/docs/mcp/tools.md).

### Running the A2A agent in Kubernetes

`cmd/a2a` is the Agent2Agent agent ([ADR 0018](docs/adr/0018-a2a-inbound-adapter.md)):
JSON-RPC over HTTP on `:8093` (`/` and `/a2a`, the Agent Card at
`/.well-known/agent-card.json`, `GET /healthz`, no auth), the third driving
adapter over the same use cases as REST and MCP, with the same transactional
outbox and no relay. A separate Deployment + ClusterIP Service
(`<release>-a2a`, port 8093) when `a2a.enabled=true` (default `false`); the
chart defaults `A2A_AGENT_URL` to the in-cluster Service DNS
(`a2a.agentUrl` overrides). Two skills, called as a `SendMessage` data part
`{"skill": "<id>", "arguments": {...}}`:

- `path_capacity` (read): `{pathId, location, windowStart, windowEnd[,
  unitsPerOrder, packagesPerOrder]}` returns the path's end-to-end
  ORDER/HOUR capacity, the bottleneck step and each step's rate.
- `evaluate_capacity_plan` (write): `{warehouseId, siteId, location,
  pathId, windowStart, windowEnd[, assignedDemand, unitsPerOrder,
  packagesPerOrder]}` records a DRAFT capacity plan (shortage, bottleneck)
  and stores its `CapacityPlanCreated` events in the outbox. Not idempotent
  (every call records a new plan) and it never publishes the plan.

Domain rejections come back as an in-band RFC 7807 `problem` with the same
status classification as REST (`404` unknown path, `422` validation, `400`
malformed, `500`). A2A carries delegation only; the facts still leave as
CloudEvents on Kafka.

---

## Operator micro-frontend (`web/`)

`web/` is the `capacity_mfe` Module Federation remote — the operator screens
(capacity overview, path capacity, capacity plans) that `warehouse-console`
lazy-loads. It talks only to this service's own REST API:

```bash
cd web
npm install        # needs the sibling checkout ../../warehouse-ui-kit, built (npm run build there)
npm run dev        # standalone on http://localhost:5190
npm run lint && npm run typecheck && npm test && npm run build
```

Standalone dev talks to `http://localhost:8080` (`go run ./cmd/api`); the
API only allows the origins in `CORS_ALLOWED_ORIGINS`, so start it with
`CORS_ALLOWED_ORIGINS=http://localhost:5190`. Inside the console the API
origin comes from `window.__WAREHOUSE_CONFIG__.apiOrigin` (the shell's
`/config.json`); the remote calls `${apiOrigin}/api/warehouse-planning`
(`web/src/config.ts`). The image is built from `web/Dockerfile` and served
by its own nginx workload (chart `frontend.enabled=true`, image
`warehouse/warehouse-planning-frontend`) at
`http://localhost/mfes/warehouse-planning/` behind the Nginx web gateway.

Besides the endpoints in `.claude/rules/rest-api.md`, the remote needed two
list reads (REST only, no MCP tool): `GET /process-paths` and
`GET /capacity-plans?location=&limit=` (newest first, default 20, max 100).

---

## Local development / quality gate

Every CI sensor is also a `make` target (`make help` lists them all):

```sh
make check-fast   # fmt-check, vet, arch-test
make check        # fmt-check, vet, build, lint, test
make check-all    # check + coverage (90% gate) + arch-test + bdd
make integration  # testcontainers Postgres + Kafka (needs only Docker)
make mutation     # gremlins over ./internal/domain
make vuln         # govulncheck ./...
make api-lint     # spectral over apis/openapi.yaml
make guide-lint   # agent guides: references resolve, context budget
```

Git hooks are managed by [lefthook](https://github.com/evilmartians/lefthook)
(`lefthook.yml`): pre-commit runs `fmt-check` + `vet` + `lint`; pre-push runs
`make check`. Activate once per clone: `brew install lefthook && lefthook
install`.

## Tests

```sh
go test ./... -race                                  # unit
go test -tags=integration ./... -race -count=1       # testcontainers Postgres/Kafka (incl. the analytics path end to end)
go test ./features* -run TestFeatures -v             # godog/Gherkin BDD over the real HTTP API (suites per wave: demand, inbound/observed, lists, ops, orders, wave 2, events)
go test ./internal/architecture/... -v               # arch-go + fleet fitness tests
make coverage                                        # domain+application coverage, CI gate >= 90%
```

CI (`.github/workflows/ci.yml`) runs `lint`, `guide-lint`, `complexity`,
`test` (with the 90% coverage gate), `bdd`, `integration`, `mutation-fast`,
`drift` and `mutation` (scheduled), `api-lint`, `vuln`, `docs-api-drift`,
`helm-lint`, `chart-tests`, `arch-test`, `web` on every push/PR to
`develop`/`main`; `trivy-scan` only on PRs into `main`; `docker-publish` and
`release` on push to `main`.

## Architecture Decision Records

The ADR index lives at [docs/adr/index.md](docs/adr/index.md) — 18 decisions,
from the bounded context and the capacity-composition semantics to the
analytics read side, the CloudEvents catalogue, MCP adoption, HPA tuning,
OTLP telemetry and the A2A inbound adapter.

## License

MIT — see [LICENSE](LICENSE).
