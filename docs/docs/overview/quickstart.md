---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
---

# Quickstart

Run the service on your machine, call it, and run its tests. Postgres and Kafka
are the only runtime ([ADR 0016](/docs/adr/0016-postgres-and-kafka-are-the-only-runtime)):
there is no in-memory mode. The request and response shapes below are the REST
contract, unchanged by that decision; the plan id is a fresh UUID on every run.

## Prerequisites

| Tool | Why |
| --- | --- |
| Go (the version in `go.mod`, currently 1.27.2) | build and test every binary |
| Docker | `docker compose up -d --wait` for the Postgres and Kafka the binaries need, and `make integration` (each integration test starts its own Postgres (`postgres:16-alpine`) and Kafka (`confluentinc/confluent-local:7.6.1`) with testcontainers) |
| `golangci-lint` v2.14.0, `gremlins` v0.6.0, `govulncheck` | `make lint`, `make mutation-fast` and `make vuln`; each target prints the exact `go install` line when the tool is missing |
| Node 20+ | only for this docs site (`docs/`) and the `web/` remote (Node 22 in CI) |

## Build and test

The `Makefile` has no `run` target; its targets mirror the CI jobs
(`make help` lists them):

```bash
make build        # go build ./...
make test         # go test ./... -race: unit, httptest and the godog features, no database
make check        # fmt-check vet build lint test (the pre-push gate)
make check-all    # check + coverage (90 % gate) + arch-test + bdd
make integration  # testcontainers Postgres/Kafka; needs Docker, not part of check
```

See [Testing](/docs/development/testing) for every layer and CI job.

## Run the API

`cmd/api` needs a real Postgres and a real Kafka. The repo's `docker-compose.yml`
brings up both (Postgres on `:55432`, Kafka on `:9092`); use consumer group ids no
deployed instance uses, because a shared group on a shared broker splits the
partitions between your laptop and the cluster:

```bash
docker compose up -d --wait
export DATABASE_URL='postgres://planning:planning@localhost:55432/planning?sslmode=disable'
export KAFKA_BROKERS=localhost:9092
export LABOR_CAPACITY_CONSUMER_GROUP=wp-local-$USER-labor
export STORAGE_CAPACITY_CONSUMER_GROUP=wp-local-$USER-storage
export DEMAND_CONSUMER_GROUP=wp-local-$USER-demand
export INBOUND_DEMAND_CONSUMER_GROUP=wp-local-$USER-inbound-demand
export OBSERVED_CAPACITY_CONSUMER_GROUP=wp-local-$USER-observed-capacity
export DEMAND_SITE_ID=SIM1
SHUTDOWN_DRAIN_DELAY=0 go run ./cmd/api
```

The binary applies `internal/adapters/outbound/postgres/migrations` on start
(`MIGRATIONS_PATH` defaults to that path relative to the repo root), so run it
from the repo root. A missing variable is a boot error that names it, before the
listener binds (see [Troubleshooting](/docs/operations/troubleshooting)). The
first log lines confirm the wiring:

```json
{"level":"INFO","msg":"postgres adapters configured","migrations_path":"internal/adapters/outbound/postgres/migrations"}
{"level":"INFO","msg":"consumer running","consumer":"labor-capacity","topic":"warehouse.workforce.events","group_id":"wp-local-...-labor","brokers":["localhost:9092"]}
{"level":"INFO","msg":"consumer running","consumer":"storage-capacity","topic":"warehouse.facility.events", ...}
{"level":"INFO","msg":"consumer running","consumer":"order-demand","topic":"warehouse.order-management.events", ...}
{"level":"INFO","msg":"outbox relay running","publisher":"kafka","interval":1000000000,"max_attempts":20,"topic":"warehouse.warehouse-planning.events"}
{"level":"INFO","msg":"http server listening","addr":":8080"}
```

`GET /readyz` answers 200 only while Postgres answers a ping **and** a Kafka
broker answers a metadata request (no topic is ever required); `GET /healthz` is
liveness only.

Telemetry is exported to `localhost:4317`. Without a Collector there, the OTel
SDK logs an export timeout now and then; it is harmless.

## First calls: the worked example

The same example the `capacity_plan.feature` scenario pins: Pick, Rebin and
Pack over an 8-hour window, 12,000 orders of demand.

```bash
# 1. Labor capacity for each step (one ProcessCapacity per step, location and window)
curl -s -X POST localhost:8080/process-capacities -H 'Content-Type: application/json' -d '{
  "process_type":"PICK","location":"PLAN-ZONE-A",
  "window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z",
  "constraint_type":"LABOR","quantity":4000,"unit":"UNIT","period_seconds":3600}'
# {"effective_rate":4000,"effective_unit":"UNIT","binding_constraint":"LABOR"}
# Repeat with "process_type":"REBIN","quantity":2500,"unit":"UNIT"
#   -> {"effective_rate":2500,"effective_unit":"UNIT","binding_constraint":"LABOR"}
# and with "process_type":"PACK","quantity":1800,"unit":"PACKAGE"
#   -> {"effective_rate":1800,"effective_unit":"PACKAGE","binding_constraint":"LABOR"}

# 2. The process path
curl -s -X POST localhost:8080/process-paths -H 'Content-Type: application/json' \
  -d '{"id":"pick-rebin-pack","name":"Pick-Rebin-Pack","steps":["PICK","REBIN","PACK"]}'
# {"id":"pick-rebin-pack","name":"Pick-Rebin-Pack","steps":["PICK","REBIN","PACK"]}

# 3. Its capacity, normalized to ORDER/hour (2.5 units and 1 package per order)
curl -s 'localhost:8080/process-paths/pick-rebin-pack/capacity?location=PLAN-ZONE-A&window_start=2026-10-05T08%3A00%3A00Z&window_end=2026-10-05T16%3A00%3A00Z&units_per_order=2.5&packages_per_order=1'
# {"normalized_rate":1000,"normalized_unit":"ORDER","bottleneck_step":"REBIN",
#  "step_breakdown":[{"step":"PICK","normalized_rate":1600,"binding_constraint":"LABOR"},
#                    {"step":"REBIN","normalized_rate":1000,"binding_constraint":"LABOR"},
#                    {"step":"PACK","normalized_rate":1800,"binding_constraint":"LABOR"}],"warnings":[]}

# 4. A capacity plan for 12000 orders (site_id is required)
curl -s -X POST localhost:8080/capacity-plans -H 'Content-Type: application/json' -d '{
  "warehouse_id":"WH-1","site_id":"SIM1","location":"PLAN-ZONE-A",
  "window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z",
  "path_id":"pick-rebin-pack","assigned_demand":12000,"units_per_order":2.5,"packages_per_order":1}'
# 201 {"id":"68b9b1e8-...","status":"DRAFT","path_capacity":1000,"bottleneck_step":"REBIN",
#      "capacity_over_window":8000,"shortage":4000,"demand_source":"request","bottleneck_constraint":"LABOR",...}

# 5. Publish it (once), then try again
curl -s -X POST localhost:8080/capacity-plans/<id>/publish   # 200, "status":"PUBLISHED"
curl -s -X POST localhost:8080/capacity-plans/<id>/publish   # 409
# {"type":"https://errors.warehouse-planning.warehouse-systems.dev/capacity-plan-already-published",
#  "title":"This CapacityPlan has already been published","status":409,...}
```

The relay then publishes eight messages: four types
(`CapacityPlanCreated`, `CapacityPlanPublished`, `CapacityShortageDetected`,
`BottleneckDetected`), each once on `warehouse.warehouse-planning.events` and
once on `warehouse.warehouse-planning.analytics` with the same CloudEvents id.
The first write to an output topic that does not exist yet can fail once with
`Unknown Topic Or Partition` (logged as `outbox relay pass failed`); the broker
creates the topic and the next pass succeeds.

Without demand data, omitting `assigned_demand` is refused rather than treated
as zero:

```bash
curl -s 'localhost:8080/demand?location=SIM1&window_start=2026-10-05T08%3A00%3A00Z&window_end=2026-10-05T16%3A00%3A00Z'
# {"location":"SIM1",...,"orders":0,"released_lines":0,"source":"order-management","as_of":null}
# POST /capacity-plans without assigned_demand -> 422 missing-assigned-demand
```

## Seed station capacity

Station counts come from facility-layout events; their throughput is declared
here ([ADR 0002](/docs/adr/0002-station-capacity-composition)):

```bash
curl -s -X PUT localhost:8080/station-standards/SIM1/PACK -H 'Content-Type: application/json' \
  -d '{"quantity":180,"unit":"PACKAGE","period_seconds":3600}'   # 201 new, 200 replaced
curl -s 'localhost:8080/storage-capacity?location=SIM1'
```

## The other binaries

```bash
go run ./cmd/mcp                       # MCP over Streamable HTTP on :8090 (/ and /mcp); DATABASE_URL required; see MCP tools
ANALYTICS_DATABASE_URL=... KAFKA_BROKERS=localhost:9092 ANALYTICS_CONSUMER_GROUP=wp-local-$USER-analytics \
  go run ./cmd/planning-projector      # admin :8091; all three required
ANALYTICS_DATABASE_URL=... go run ./cmd/planning-reports   # :8092, GET /reports/...
```

`cmd/mcp` reads the same Postgres as `cmd/api` (`DATABASE_URL`, a boot error
without it); the analytics binaries use their own analytical database. Tools are
listed on [MCP tools](/docs/mcp/tools); all variables on
[Configuration](/docs/operations/configuration).
