<!-- TEMPLATE (warehouse-harness-template v2): fill in for THIS repo. -->
# REST API (inbound adapter)

Phase 1 (current):

- `POST /process-capacities`  -> `RegisterProcessCapacityConstraint`
- `GET  /process-capacities`  -> `GetEffectiveProcessCapacity`

Phase 2:

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
