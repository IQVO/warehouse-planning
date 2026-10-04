---
paths:
  - "internal/adapters/inbound/http/**"
  - "apis/openapi*.yaml"
  - "apis/openapi/**"
---

# REST API (inbound adapter)

Routes are wired in `NewRouter` (`internal/adapters/inbound/http/process_capacity_handler.go`);
the worked recipe for adding one is `.claude/skills/how-to-add-a-rest-endpoint/SKILL.md`.
This service does NOT implement the fleet `Idempotency-Key` middleware
(`.claude/rules/fleet/idempotency-and-outbox.md` says resource-creating POSTs
require it, but no POST here does, in code, `apis/openapi.yaml` or tests).
Do not add it as a drive-by change: it alters the contract for every client.
Raise it with the user first.

Phase 1 (current):

- `POST /process-capacities`  -> `RegisterProcessCapacityConstraint`
- `GET  /process-capacities`  -> `GetEffectiveProcessCapacity`

Phase 2 (current):

- `POST /process-paths`              -> `RegisterProcessPath`
- `GET /process-paths/{id}/capacity` -> `GetProcessPathCapacity`

Phase 4 (current):

- `POST /capacity-plans`             -> `CreateCapacityPlan`
- `POST /capacity-plans/{id}/publish`-> `PublishCapacityPlan`
- `GET  /capacity-plans/{id}`        -> direct repository read

Station capacity (current, docs/adr/0002):

- `PUT /station-standards/{location}/{process_type}` -> `DeclareStationStandard`
- `GET /station-standards?location=`                 -> direct repository read
- `GET /storage-capacity?location=`                  -> `GetStorageCapacity`

Kept in sync with `apis/openapi.yaml` as each endpoint ships (the
`docs-api-drift` CI job fails if generated docs disagree with the spec).

## Conventions

- Error shape: RFC 7807 `application/problem+json`, matching the rest of
  the fleet.
- Auth: none. This fleet's REST+MCP auth was deliberately reverted
  2026-09-11 and stays unauthenticated pending a fresh fleet-wide
  decision. `internal/architecture/fitness_test.go`'s
  `TestNoAuthMiddlewareReintroduced` fails CI if auth middleware is
  reintroduced here.

## `POST /process-paths` request/response shape

Declares a ProcessPath (id/name/ordered, non-empty steps). There is no
event-driven sync from process-path-management (deliberately: its ProcessPath
carries no physical step sequence, ADR 0001 Addendum, 2026-10-03), so this
context owns its own ProcessPath and this endpoint is the only way a
ProcessPath becomes known to this service.

```json
// request
{ "id": "pick-rebin-pack", "name": "Pick-Rebin-Pack", "steps": ["PICK", "REBIN", "PACK"] }
// 201 response
{ "id": "pick-rebin-pack", "name": "Pick-Rebin-Pack", "steps": ["PICK", "REBIN", "PACK"] }
```

422 (`application/problem+json`) if `steps` is empty.

Persistence: with `DATABASE_URL` set the path is stored in Postgres
(`process_paths`, migration `0004`; `steps` is a `text[]` column, which keeps
step ORDER) and survives a restart; without it the in-memory repo is used.
Re-registering an existing `id` replaces name and steps wholesale in both.

## `GET /process-paths/{id}/capacity` request/response shape

Query parameters:

- `location` (required) -- the warehouse location every step's
  ProcessCapacity is looked up under.
- `window_start` / `window_end` (required, RFC3339) -- the planning window.
  A step's registered ProcessCapacity window applies when it COVERS
  `[window_start, window_end)` (`window_start <= requested start` AND
  `window_end >= requested end`; docs/adr/0003). `window_end` must be after
  `window_start` (`400 invalid-capacity-window` otherwise).
- `units_per_order` / `packages_per_order` (both optional, numbers) --
  the WorkloadProfile's conversion factors, carried directly on the
  request. PHASE 2 SIMPLIFICATION: there is no dedicated WorkloadProfile
  persistence yet -- a later phase will load a warehouse's profile from
  its own store instead of requiring the caller to pass it on every
  request. Only the factor(s) the path's steps actually need have to be
  supplied.

```json
// 200 response
{ "normalized_rate": 1000, "normalized_unit": "ORDER", "bottleneck_step": "REBIN" }
```

Capacity is composed at READ time (see "Station capacity" below): per step
the candidates are the constraints of the ProcessCapacity aggregates of
(process, location) whose window COVERS the requested window (per constraint
type the aggregate with the latest window start wins, tie: the narrower
window; ADR 0003) plus a DERIVED STATION constraint (stations
tallied across the site's zones x the declared StationStandard); every
candidate is normalized to ORDER/hour before the minimum is taken. Two
ADDITIVE response fields describe the composition:

```json
{ "normalized_rate": 1800, "normalized_unit": "ORDER", "bottleneck_step": "PACK",
  "step_breakdown": [
    { "step": "PICK",  "normalized_rate": 3200, "binding_constraint": "LABOR" },
    { "step": "REBIN", "normalized_rate": 2400, "binding_constraint": "LABOR" },
    { "step": "PACK",  "normalized_rate": 1800, "binding_constraint": "STATION" } ],
  "warnings": [] }
```

`step_breakdown[].normalized_rate` is ORDER per HOUR; `warnings` is always an
array (never null) -- one entry per step that has stations tallied but no
declared standard (that step then uses its registered constraints only).

Errors (`application/problem+json`):

- `404 process-path-not-found` -- no ProcessPath registered under `{id}`.
- `422 missing-step-capacity` -- a path step has neither a ProcessCapacity
  whose window covers `location`/the window nor a derived STATION constraint
  (the detail names the step, location and window).
- `422 non-positive-conversion-factor` / `422 missing-conversion-factor` /
  `422 unsupported-normalization-unit` -- a WorkloadProfile/normalization
  problem (bad factor, a needed factor never supplied, or a step's native
  unit this phase cannot normalize to ORDER).
- `400` -- `invalid-capacity-window` (`window_end` not after `window_start`;
  new with ADR 0003 -- before, an inverted window was a `422` miss) or a
  malformed `window_start`/`window_end`/`units_per_order`/
  `packages_per_order`.

## `POST /capacity-plans` request/response shape

Evaluates a ProcessPath against the demand assigned to a location and window
and stores a DRAFT plan. PHASE 4 SIMPLIFICATION: `assigned_demand` (orders)
and the WorkloadProfile factors travel in the body (the final demand
ingestion from order-management/network-fulfillment is a later decision; no
live cross-context call, no WorkloadProfile persistence yet). The factor
validation is exactly Phase 2's path-capacity endpoint. Every step's
ProcessCapacity is resolved by window COVERAGE, as in the path-capacity
endpoint (docs/adr/0003): a registered window applies when it covers
`[window_start, window_end)`. The plan stores the REQUESTED window, and
`capacity_over_window` / `shortage` are computed from its length.

```json
// request
{ "warehouse_id": "WH-1", "location": "PATH-ZONE-A",
  "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z",
  "path_id": "pick-rebin-pack", "assigned_demand": 12000,
  "units_per_order": 2.5, "packages_per_order": 1 }
// 201 response
{ "id": "<uuid>", "warehouse_id": "WH-1", "location": "PATH-ZONE-A",
  "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z",
  "path_id": "pick-rebin-pack", "assigned_demand": 12000, "status": "DRAFT",
  "path_capacity": 1000, "bottleneck_step": "REBIN",
  "capacity_over_window": 8000, "shortage": 4000, "created_at": "...",
  "bottleneck_constraint": "LABOR", "warnings": [] }
```

`path_capacity` is ORDER per HOUR; `capacity_over_window` and `shortage` are
orders. The plan uses the same read-time composition as the path capacity, so
`bottleneck_constraint` (additive; the constraint type binding the bottleneck
step, e.g. `LABOR` or `STATION`) can be `STATION`, and `warnings` (additive,
never null) carries the composition warnings. Both are stored with the plan
(migration `0005`; empty for plans created before it) and returned by GET and
publish too. The published CloudEvents payloads do not carry them. `GET /capacity-plans/{id}` returns the same body (200 / 404);
`POST /capacity-plans/{id}/publish` returns it with `status: "PUBLISHED"` and
`published_at` (200).

Errors (`application/problem+json`):

- `400 malformed-json` / `malformed-window-start` / `malformed-window-end` /
  `invalid-capacity-window` (end not after start).
- `404 process-path-not-found` (create), `404 capacity-plan-not-found`
  (publish, get).
- `409 capacity-plan-already-published` (publish twice; nothing is queued).
- `422 missing-step-capacity`, `non-positive-conversion-factor`,
  `missing-conversion-factor`, `unsupported-normalization-unit` (as Phase 2),
  plus `negative-assigned-demand`, `missing-assigned-demand` (field absent --
  never silently zero) and `missing-required-field` (blank `warehouse_id`,
  `location` or `path_id`).

Create and publish each queue their CloudEvents in the transactional outbox in
the same database transaction as the plan (see `integration-events.md`).

## Station capacity (docs/adr/0002)

A station COUNT (tallied from facility-layout) has no throughput; the
operator declares the throughput of ONE station and the service composes
`count x standard` with LABOR at read time. A planning `location` is a
site/building code; the zones of a site are the tally zones whose id starts
with `<location>-` (zone id first segment = SiteCode = building id).

### `PUT /station-standards/{location}/{process_type}`

```json
// request
{ "quantity": 180, "unit": "PACKAGE", "period_seconds": 3600 }
// 201 (new) / 200 (replaced)
{ "location": "SIM1", "process_type": "PACK", "quantity": 180, "unit": "PACKAGE", "period_seconds": 3600 }
```

`unit` is the process's natural unit: `UNIT`, `PACKAGE` or `ORDER`.
`422 application/problem+json` for `non-positive-station-standard` (zero
quantity), `negative-quantity`, `non-positive-period`,
`unsupported-normalization-unit` (LINE or unknown); `400 malformed-json`.
Persisted in `station_standards` (migration `0005`) when `DATABASE_URL` is set.

### `GET /station-standards?location=`

`200 { "location": "SIM1", "standards": [ {...same shape as above...} ] }`,
ordered by location then process type; `location` is optional (omitted =
every standard, and the `location` field is omitted too). Empty list, never an
error.

### `GET /storage-capacity?location=`

The READ MODEL of a site -- not a throughput, and with no "consumed" figure
(stock is never read from inventory-storage):

```json
{ "location": "SIM1",
  "storage_positions": [ { "zone_id": "SIM1-STOR-AMB", "location_type": "SimShelf", "positions": 24 } ],
  "stations":          [ { "zone_id": "SIM1-OPS-WC",   "activity": "PACK",          "stations": 11 } ] }
```

`200` with empty lists when nothing is tallied; `400 missing-location` when
`location` is absent. Zero-count buckets are omitted.
