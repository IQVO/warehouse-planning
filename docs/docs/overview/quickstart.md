---
id: quickstart
title: Quickstart
sidebar_label: Quickstart
---

# Quickstart

Run the service on your machine, call it, and run its tests. Every response
shown below was captured from a real `cmd/api` run on in-memory adapters.

## Prerequisites

| Tool | Why |
| --- | --- |
| Go (the version in `go.mod`, currently 1.27.2) | build and test every binary |
| Docker | only for `make integration`: each integration test starts its own Postgres (`postgres:16-alpine`) and Kafka (`confluentinc/confluent-local:7.6.1`) with testcontainers |
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

## Run the API with no dependencies

Without `DATABASE_URL` the API uses in-memory adapters, without `KAFKA_BROKERS`
the consumers stay off, and the outbox relay logs events instead of sending
them (`EVENT_PUBLISHER=log`, the default):

```bash
SHUTDOWN_DRAIN_DELAY=0 go run ./cmd/api
```

The first log lines confirm the mode:

```json
{"level":"INFO","msg":"DATABASE_URL not configured; using in-memory adapters"}
{"level":"WARN","msg":"KAFKA_BROKERS not configured; labor/storage capacity Kafka ingestion is disabled"}
{"level":"INFO","msg":"order demand consumption is disabled","enable_with":"DEMAND_CONSUMER_GROUP"}
{"level":"INFO","msg":"http server listening","addr":":8080"}
{"level":"INFO","msg":"outbox relay running","publisher":"log","interval":1000000000,"topic":"warehouse.warehouse-planning.events"}
```

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

The relay then logs eight `event published (log sink)` lines: four types
(`CapacityPlanCreated`, `CapacityPlanPublished`, `CapacityShortageDetected`,
`BottleneckDetected`), each once on `warehouse.warehouse-planning.events` and
once on `warehouse.warehouse-planning.analytics` with the same CloudEvents id.

Without demand data, omitting `assigned_demand` is refused rather than treated
as zero:

```bash
curl -s 'localhost:8080/demand?location=SIM1&window_start=2026-10-05T08%3A00%3A00Z&window_end=2026-10-05T16%3A00%3A00Z'
# {"location":"SIM1",...,"orders":0,"released_lines":0,"source":"order-management","as_of":null}
# POST /capacity-plans without assigned_demand -> 422 missing-assigned-demand
```

## Run against Postgres

There is no `docker-compose.yml` in this repo. Any Postgres 16 works, for
example a throwaway container:

```bash
docker run --rm -d --name wp-pg -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=warehouse_planning -p 5432:5432 postgres:16-alpine
DATABASE_URL='postgres://postgres:postgres@localhost:5432/warehouse_planning?sslmode=disable' go run ./cmd/api
```

The API applies `internal/adapters/outbound/postgres/migrations` on start
(`MIGRATIONS_PATH` defaults to that path relative to the repo root), so run it
from the repo root.

## Run against the fleet's Kafka

The fleet has one broker, the in-cluster Kafka of the kind cluster, reachable
from the host at `localhost:9092` through its external access. The old
docker-compose Kafka is retired.

:::warning Use your own consumer group ids
The labor and storage consumers default to the **same** group ids the
in-cluster pod uses (`warehouse-planning-labor-capacity`,
`warehouse-planning-storage-capacity`). A local process with the defaults joins
those groups and takes partitions away from the cluster. Set unique ids:
:::

```bash
KAFKA_BROKERS=localhost:9092 \
LABOR_CAPACITY_CONSUMER_GROUP=wp-local-$USER-labor \
STORAGE_CAPACITY_CONSUMER_GROUP=wp-local-$USER-storage \
DATABASE_URL='postgres://...' \
go run ./cmd/api
```

Add `EVENT_PUBLISHER=kafka` only if you really want your local plans on the
shared `warehouse.warehouse-planning.events` topic, where order-management
consumes them. `DEMAND_CONSUMER_GROUP` has no default on purpose; set it (with
`DEMAND_SITE_ID=SIM1`) to a private id if you want demand locally.

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
go run ./cmd/mcp                       # MCP over Streamable HTTP on :8090 (/ and /mcp); see MCP tools
ANALYTICS_DATABASE_URL=... KAFKA_BROKERS=localhost:9092 ANALYTICS_CONSUMER_GROUP=wp-local-$USER-analytics \
  go run ./cmd/planning-projector      # admin :8091
ANALYTICS_DATABASE_URL=... go run ./cmd/planning-reports   # :8092, GET /reports/...
```

`cmd/mcp` without `DATABASE_URL` has its own in-memory store, separate from a
running `cmd/api`; point both at the same Postgres to share data. Tools are
listed on [MCP tools](/docs/mcp/tools); all variables on
[Configuration](/docs/operations/configuration).
