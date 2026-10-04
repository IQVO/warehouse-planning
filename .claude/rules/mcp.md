# MCP server (inbound adapter)

One MCP server for this bounded context, an additive inbound adapter
(ADR-0008) over the SAME use cases the REST adapter calls:

- Code: `internal/adapters/inbound/mcp/` (tools, error mapping) and the
  composition root `cmd/mcp/` (env, adapters, router, graceful shutdown).
  Built on the official SDK `github.com/modelcontextprotocol/go-sdk` (v1.8.0).
- Transport: **Streamable HTTP only** (no stdio, no SSE). Listens on
  `MCP_ADDR` (default `:8090`), served at **`/` and `/mcp`**; `GET /healthz`
  -> `200 {"status":"ok"}`.
- **No auth of any kind** (fleet-wide revert 2026-09-11): no keys, no bearer
  checks, no `MCP_READ_KEY`. `TestNoAuthMiddlewareReintroduced` fails CI if
  it is reintroduced. Access control is the in-cluster ClusterIP boundary.
- Architecture: the adapter depends ONLY on `internal/application` and
  `internal/domain`; nothing depends on it (`TestMCPAdapterDependencyRule`).
- Env (`cmd/mcp`): `MCP_ADDR`, `DATABASE_URL` (unset -> in-memory repos),
  `MIGRATIONS_DATABASE_URL` (direct DSN for the migration step only; falls
  back to `DATABASE_URL`), `MIGRATIONS_PATH`, `LOG_LEVEL`. Like every
  sibling's `cmd/mcp`, it runs the idempotent migrations on start (advisory
  lock; safe next to `cmd/api`) and uses `internal/bootretry` for the
  Postgres dial.
- It does NOT start the outbox relay and does NOT dial Kafka. Create/publish
  insert their CloudEvents into the outbox inside the UnitOfWork (same as
  REST); the relay in `cmd/api` drains them.

## Tools (10; budget is 10)

All argument names are snake_case, matching the REST bodies. Timestamps are
RFC3339; a window must equal the registered window EXACTLY (no overlap
matching). Failures are MCP tool errors (`isError: true`) whose text is
`<slug>: <message>`, using the REST problem slugs (`capacity-plan-not-found`,
`capacity-plan-already-published`, `process-path-not-found`,
`missing-step-capacity`, `negative-assigned-demand`, ...). Unexpected
infrastructure errors are logged and reported as a generic `internal-error`.

| Tool | R/W | Backed by | Arguments (required unless noted) | Result |
|---|---|---|---|---|
| `register_process_capacity_constraint` | write | `RegisterProcessCapacityConstraint` | `process_type`, `location`, `window_start`, `window_end`, `constraint_type` (LABOR, LOCATION, EQUIPMENT, STATION, CONVEYOR, BUFFER, REPLENISHMENT), `quantity`, `unit` (UNIT, LINE, ORDER, PACKAGE), `period_seconds` (> 0) | `effective_rate`, `effective_unit`, `binding_constraint` |
| `get_effective_process_capacity` | read | `ProcessCapacityRepository` port (no use case, as REST) | `process_type`, `location`, `window_start`, `window_end` | `effective_rate`, `effective_unit`, `binding_constraint`, `constraints[]` (`constraint_type`, `quantity`, `unit`, `period_seconds`); `process-capacity-not-found` if none |
| `register_process_path` | write | `RegisterProcessPath` | `id`, `name`, `steps` (ordered, non-empty) | `id`, `name`, `steps` |
| `get_process_path_capacity` | read | `GetProcessPathCapacity` | `id`, `location`, `window_start`, `window_end`, optional `units_per_order`, `packages_per_order` | `normalized_rate` (ORDER/hour), `normalized_unit` (`ORDER`), `bottleneck_step`, `step_breakdown[]` (`step`, `normalized_rate` ORDER/hour, `binding_constraint`), `warnings[]` (never null) |
| `create_capacity_plan` | write | `CreateCapacityPlan` | `warehouse_id`, `location`, `window_start`, `window_end`, `path_id`, `assigned_demand` (orders; required, never silently 0), optional `units_per_order`, `packages_per_order` | the plan (below), `status: DRAFT` |
| `publish_capacity_plan` | write | `PublishCapacityPlan` | `id` | the plan, `status: PUBLISHED`, `published_at`; second call -> `capacity-plan-already-published` |
| `get_capacity_plan` | read | `CapacityPlanRepository` port (no use case, as REST) | `id` | the plan |
| `declare_station_standard` | write (idempotent: same key replaces) | `DeclareStationStandard` | `location` (site code, e.g. `SIM1`), `process_type`, `quantity` (> 0, per ONE station), `unit` (UNIT, PACKAGE, ORDER), `period_seconds` (> 0) | `location`, `process_type`, `quantity`, `unit`, `period_seconds`, `created` |
| `list_station_standards` | read | `StationStandardRepository` port (no use case, as REST) | optional `location` | `location` (when filtered), `standards[]` (same fields) |
| `get_storage_capacity` | read | `GetStorageCapacity` | `location` | `location`, `storage_positions[]` (`zone_id`, `location_type`, `positions`), `stations[]` (`zone_id`, `activity`, `stations`); empty arrays when nothing is tallied |

Plan body (same as REST `capacityPlanResponse`): `id`, `warehouse_id`,
`location`, `window_start`, `window_end`, `path_id`, `assigned_demand`,
`status`, `path_capacity` (ORDER/hour), `bottleneck_step`,
`capacity_over_window`, `shortage`, `created_at`, `published_at` (omitted
while DRAFT), `bottleneck_constraint` (e.g. LABOR, STATION; additive) and
`warnings` (additive, never null).

Station capacity (docs/adr/0002): `get_process_path_capacity`,
`create_capacity_plan` and `get_capacity_plan` surface the read-time
composition (`step_breakdown`, `bottleneck_constraint`, `warnings`);
`declare_station_standard` / `list_station_standards` /
`get_storage_capacity` mirror the REST `PUT/GET /station-standards` and
`GET /storage-capacity` (errors `non-positive-station-standard`,
`negative-quantity`, `non-positive-period`, `unsupported-normalization-unit`,
`missing-station-standard-field`, `missing-location`).

Annotations (charter §4): read tools `ReadOnlyHint`; write tools destructive
and, where a repeat call creates or adds state, non-idempotent
(`register_process_path` is idempotent: same id replaces wholesale).
`TestToolSurface` pins the tool set, naming, annotations, descriptions and
snake_case documented arguments.

## Tests

- `internal/adapters/inbound/mcp/tools_test.go` drives every tool through
  the SDK in-memory transport over in-memory repos and reproduces the
  section-43 scenario through MCP tools only; the expected numbers are read
  from `features/capacity_plan.feature` so MCP and REST cannot drift.
- `cmd/mcp/main_test.go`: `/healthz`, both mount paths over real Streamable
  HTTP, no auth required. `cmd/mcp/main_integration_test.go`
  (`-tags=integration`, testcontainers Postgres): migrations on a fresh DB,
  section 43 persisted incl. the 4 outbox rows.

## Adding a tool

Add a typed input struct (snake_case `json` tags + `jsonschema:"..."`
description on every field), a `Deps` method calling an existing use case
(or a repository port for a plain read), register it in `registerTools` with
annotations, map errors through `mapError`, and extend `wantTools` in
`TestToolSurface`. Keep the surface at <= 10 tools: the budget was 8 and was raised to 10, in one
reviewed change, by the three station-capacity tools (`TestToolSurface`'s
`maxTools` pins it and the exact curated set).
