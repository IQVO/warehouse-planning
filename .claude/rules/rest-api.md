<!-- TEMPLATE (warehouse-harness-template v2): fill in for THIS repo. -->
# REST API (inbound adapter)

Phase 1 (current):

- `POST /process-capacities`  -> `RegisterProcessCapacityConstraint`
- `GET  /process-capacities`  -> `GetEffectiveProcessCapacity`

Phase 2 (current):

- `POST /process-paths`              -> `RegisterProcessPath`
- `GET /process-paths/{id}/capacity` -> `GetProcessPathCapacity`

Phase 4:

- `POST /capacity-plans`             -> `CreateCapacityPlan`
- `POST /capacity-plans/{id}/publish`-> `PublishCapacityPlan`
- `GET  /capacity-plans/{id}`        -> query

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
