# warehouse-planning

Warehouse Planning — a Core bounded context in the `warehouse-systems`
fleet. Answers: **can this warehouse process the demand assigned to it**,
given its current labor, location, equipment, station, conveyor and
buffer constraints? This is distinct from Inventory Management ("what do
we have") and from every existing fleet context, none of which currently
computes normalized, cross-process effective capacity or forward-looking
capacity shortages.

See `CLAUDE.md` for the full repo guide, `.claude/rules/domain-model.md`
for the ubiquitous language/aggregates, and
`docs/adr/0001-warehouse-planning-bounded-context.md` for why this context
exists and its context map.

Scaffolded from `warehouse-harness-template: v2`. See `HARNESS.md` for the
sensor manifest.

## Study project

This repo, like the rest of the `warehouse-systems` fleet, is a personal
study project exploring Domain-Driven Design, hexagonal architecture, and
AI-agent harness engineering. It is not production software and carries
no support guarantee.

## Deployment

- **Image**: root `Dockerfile` builds every `cmd/*` directory (`api`, `mcp`, `planning-projector` and `planning-reports`) into `/app/<name>`; `ENTRYPOINT` is `./api`. OLTP migrations are copied to `/app/migrations`, the analytical ones to `/app/analytics/migrations`.
- **Chart**: `charts/warehouse-planning` (OLTP `api` component; optional `mcp`, `frontend` and analytics (`analytics-projector`, `analytics-reports`) components, all off by default). It refuses to render without `database.url` or `database.existingSecret`. Run `helm lint charts/warehouse-planning --set database.url=postgres://u@example.invalid:5432/db` and `python3 charts/warehouse-planning/tests/test_service_selectors.py`.
- **MCP**: `cmd/mcp` serves 10 tools over Streamable HTTP on `:8090` (`/` and `/mcp`, `GET /healthz`, no auth); see `.claude/rules/mcp.md`. Enable in the chart with `mcp.enabled=true`.
- **Frontend remote** (`web/`, Module Federation container `capacity_mfe`): the operator screens (capacity overview, path capacity, capacity plans) that `warehouse-console` lazy-loads. Served by its own nginx workload (chart `frontend.enabled=true`, image `warehouse/warehouse-planning-frontend`) at `http://localhost/mfes/warehouse-planning/` behind the Nginx web gateway, and reads `apiOrigin` from the console's `/config.json` to call this service through Kong at `/api/warehouse-planning`. See `web/` and `.claude/rules/frontend.md`.
- **Kind cluster**: wired by `warehouse-infra` (`local.services`); ArgoCD deploys the chart from this repo's `develop`.
- **Analytics read side** (ADR 0005): the four capacity-plan events are also written to `warehouse.warehouse-planning.analytics` in the same outbox transaction; `cmd/planning-projector` (admin `:8091`) projects them into a SEPARATE analytical database and `cmd/planning-reports` (`:8092`, read-only) serves `GET /reports/{bottleneck-frequency,shortage-trend,plan-throughput}`. Chart: `analytics.enabled=true` (default `false`) renders both components; the analytical DSN comes from `analytics.databaseUrl` or `analytics.existingSecret`.

## Analytics read side

```bash
# 1. OLTP service fanning events onto both topics (relay publishes them)
export EVENT_PUBLISHER=kafka KAFKA_BROKERS=localhost:9092 DATABASE_URL=...   # see Makefile for the local Postgres
go run ./cmd/api

# 2. Projector: the only writer of the analytical database (applies analytics/migrations itself)
export ANALYTICS_DATABASE_URL='postgres://projector@localhost:5432/warehouse_planning_analytics?sslmode=disable'  # password via PGPASSWORD
export ANALYTICS_CONSUMER_GROUP=warehouse-planning-analytics
go run ./cmd/planning-projector

# 3. Reports: read-only
export ANALYTICS_DATABASE_URL='postgres://reports_ro@localhost:5432/warehouse_planning_analytics?sslmode=disable'
go run ./cmd/planning-reports
curl 'http://localhost:8092/reports/shortage-trend?from=2026-10-01T00:00:00Z&to=2026-10-08T00:00:00Z'
```

`from` / `to` are optional RFC 3339 (from inclusive, to exclusive, default the 30
days ending now, at most 366 days). The `-tags=integration` suite starts real
Postgres and Kafka with testcontainers and covers the whole path (plan created and
published -> outbox -> relay -> Kafka -> projector -> analytical DB -> report).

## Frontend remote (`web/`)

```bash
cd web
npm install        # needs the sibling checkout ../../warehouse-ui-kit, built (npm run build there)
npm run dev        # standalone on http://localhost:5190 (see below)
npm run lint && npm run typecheck && npm test && npm run build
```

Standalone dev talks to `http://localhost:8080` (`go run ./cmd/api`); the API only
allows the origins in `CORS_ALLOWED_ORIGINS`, so start it with
`CORS_ALLOWED_ORIGINS=http://localhost:5190`. Inside the console the API origin
comes from `window.__WAREHOUSE_CONFIG__.apiOrigin` (the shell's `/config.json`).
The image is built from `web/Dockerfile` (`docker build --build-context
uikit=../../warehouse-ui-kit -t warehouse/warehouse-planning-frontend:local web`).

## REST reads for the remote

Besides the endpoints in `.claude/rules/rest-api.md`, the remote needed two list
reads (REST only, no MCP tool): `GET /process-paths` and
`GET /capacity-plans?location=&limit=` (newest first, default 20, max 100).
