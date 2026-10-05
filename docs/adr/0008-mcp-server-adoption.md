# ADR 0008: MCP server adoption (unauthenticated, additive)

## Status

Accepted (2026-10-05). Records a decision already in effect: `cmd/mcp` has
shipped since Phase 2/3 with no ADR, found by the 2026-10-04
ADR-conformance audit.

## Context

The fleet exposes every bounded context to `warehouse-ops-agent` (and any
other MCP client) through a second, independent deployable binary over
Streamable HTTP, alongside the REST API, never bundled into the same
process. `warehouse-planning` already does this (`cmd/mcp`) but had never
written down why.

## Decision

1. `cmd/mcp` is a **second composition root**, independent of `cmd/api`: it
   wires the SAME use cases (`usecases.CreateCapacityPlan`,
   `RegisterProcessCapacityConstraint`, ...) to the SAME adapters (in-memory
   when `DATABASE_URL` is unset, Postgres otherwise), through a dedicated
   inbound adapter (`internal/adapters/inbound/mcp`) instead of the REST
   router.
2. It serves **11 tools** today (`internal/adapters/inbound/mcp/tools.go`,
   `registerTools`/`registerReadModelTools`/`registerDemandTools`): reads
   (`get_effective_process_capacity`, `get_process_path_capacity`,
   `get_capacity_plan`, `list_station_standards`, `get_storage_capacity`,
   `get_expected_demand`) and **writes**
   (`register_process_capacity_constraint`, `register_process_path`,
   `create_capacity_plan`, `publish_capacity_plan`,
   `declare_station_standard`). A write tool goes through the exact same use
   case and transactional-outbox path (ADR 0007) as its REST equivalent --
   there is no second, parallel write path.
3. **No authentication of any kind** -- no API key, no bearer token. This
   matches the fleet-wide decision (reverted 2026-09-11 across every
   context) that REST and MCP are both unauthenticated; access control is
   the in-cluster `ClusterIP` network boundary, not an application-layer
   check. `internal/architecture/fitness_test.go`'s
   `TestNoAuthMiddlewareReintroduced` fails CI if auth middleware is ever
   added back to either router.
4. `cmd/mcp` **never starts the outbox relay and never dials Kafka itself**:
   a write tool's use case inserts its CloudEvents into the outbox inside its
   `UnitOfWork`, and the relay that drains `outbox_events` onto Kafka runs
   only in `cmd/api`. This keeps `cmd/mcp` a thin, horizontally-scalable
   front door with no Kafka broker dependency of its own.
5. `cmd/mcp` runs the idempotent `golang-migrate` step on its own boot (same
   as `cmd/api`), so it can come up against a fresh database before `cmd/api`
   has; golang-migrate's advisory lock makes the two processes' concurrent
   migration attempts safe (see `docs/adr/0010-migrations-over-direct-connection.md`).
6. `internal/architecture/fitness_test.go`'s `TestMCPAdapterDependencyRule`
   keeps this adapter additive: it may depend on the application/domain
   layers only (never on outbound adapters, never on `cmd`), and nothing
   else in this codebase may import it -- a sibling package importing
   `internal/adapters/inbound/mcp` would silently make the MCP surface
   load-bearing for something other than `cmd/mcp` itself.

## Consequences

- Two binaries now read this ADR instead of only `cmd/api`'s composition
  root comments.
- A future auth requirement (should one ever arrive) needs a NEW ADR
  explicitly superseding point 3 here, not a silent code change.

## Alternatives considered and rejected

- **Bundle MCP into `cmd/api`'s HTTP server** (one process, two route
  trees): couples the two surfaces' scaling and failure domains; the fleet
  convention (every sibling context) is two independent deployables.
- **Bearer-token auth on MCP only**: rejected fleet-wide in the 2026-09-11
  revert; MCP and REST get the same (lack of) auth so operators reason about
  one boundary, not two.
