<!-- TEMPLATE (warehouse-harness-template v2): fill in for THIS repo. -->
# REST API (inbound adapter)

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

Seeds a ProcessPath read model (id/name/ordered, non-empty steps). There
is no event-driven sync from process-path-management yet (a later phase)
-- this is the only way a ProcessPath becomes known to this service for
now.

```json
// request
{ "id": "pick-rebin-pack", "name": "Pick-Rebin-Pack", "steps": ["PICK", "REBIN", "PACK"] }
// 201 response
{ "id": "pick-rebin-pack", "name": "Pick-Rebin-Pack", "steps": ["PICK", "REBIN", "PACK"] }
```

422 (`application/problem+json`) if `steps` is empty.

## `GET /process-paths/{id}/capacity` request/response shape

Query parameters:

- `location` (required) -- the warehouse location every step's
  ProcessCapacity is looked up under.
- `window_start` / `window_end` (required, RFC3339) -- the capacity
  window every step's ProcessCapacity is looked up for.
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

Errors (`application/problem+json`):

- `404 process-path-not-found` -- no ProcessPath registered under `{id}`.
- `422 missing-step-capacity` -- a path step has no registered
  ProcessCapacity for `location`/the window.
- `422 non-positive-conversion-factor` / `422 missing-conversion-factor` /
  `422 unsupported-normalization-unit` -- a WorkloadProfile/normalization
  problem (bad factor, a needed factor never supplied, or a step's native
  unit this phase cannot normalize to ORDER).
- `400` -- a malformed `window_start`/`window_end`/`units_per_order`/
  `packages_per_order`.

## `POST /capacity-plans` request/response shape

Evaluates a ProcessPath against the demand assigned to a location and window
and stores a DRAFT plan. PHASE 4 SIMPLIFICATION: `assigned_demand` (orders)
and the WorkloadProfile factors travel in the body (the final demand
ingestion from order-management/network-fulfillment is a later decision; no
live cross-context call, no WorkloadProfile persistence yet). The factor
validation is exactly Phase 2's path-capacity endpoint. As in Phase 2, every
step's ProcessCapacity must have been registered for EXACTLY
`[window_start, window_end)` -- there is no overlap matching.

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
  "capacity_over_window": 8000, "shortage": 4000, "created_at": "..." }
```

`path_capacity` is ORDER per HOUR; `capacity_over_window` and `shortage` are
orders. `GET /capacity-plans/{id}` returns the same body (200 / 404);
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
