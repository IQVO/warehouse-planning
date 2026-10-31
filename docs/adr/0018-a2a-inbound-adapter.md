---
description: "ADR 0018: warehouse-planning is an Agent2Agent (A2A) agent through a third driving adapter over the same use cases as REST and MCP. Two curated goal-shaped skills, path_capacity (read) and evaluate_capacity_plan (write), in a message-only agent served by cmd/a2a on port 8093. A2A carries delegation only; facts still leave as CloudEvents through the transactional outbox."
---

# ADR 0018: A2A inbound adapter (third driving adapter)

## Status

Accepted (2026-10-29). Additive: no CloudEvents type, payload, topic or REST
contract changes. Mirrors the reference rollout in workforce-management (ADR 0039) and
wes-work-planning (ADR 0046); follows [ADR 0008](./0008-mcp-server-adoption.md) (the MCP adapter this one is
modelled on), [ADR 0006](./0006-cloudevents-envelope-and-type-catalogue.md)
(CloudEvents on every Kafka message), [ADR 0007](./0007-outbox-and-resilient-consumers.md)
(the transactional outbox) and
[ADR 0016](./0016-postgres-and-kafka-are-the-only-runtime.md) (Postgres and
Kafka are the only runtime).

## Context

The fleet is adopting the Agent2Agent protocol so that a bounded context can
**delegate** a goal to another context's agent. This context already exposes
its use cases over REST and MCP. The question it exists to answer
(*can this warehouse process the demand assigned to it?*) has two natural
delegated halves: *how much can this process path clear in this window, and
what binds it?* and *evaluate the demand assigned to this window against that
capacity and record the result*.

Three transports now carry three different things and must not overlap:

| Transport | Carries |
|---|---|
| Kafka + CloudEvents | **facts** between contexts ([ADR 0006](./0006-cloudevents-envelope-and-type-catalogue.md)) |
| MCP | an agent's **tools** (reads and single actions) |
| A2A | **delegation** of a goal to another context's agent |

## Decision

Add `internal/adapters/inbound/a2a`, a third driving adapter over the **same**
use cases (`GetProcessPathCapacity`, `CreateCapacityPlan`) as the REST and MCP
adapters, built on the official `a2aproject/a2a-go/v2` SDK (pinned `v2.7.0`)
and served by a new `cmd/a2a` binary (JSON-RPC 2.0 over HTTP, port **8093**,
the Agent Card at `/.well-known/agent-card.json`, the endpoint mounted at `/`
and `/a2a`). `cmd/a2a` is cloned from `cmd/mcp`: same composition, same
Postgres wiring, same transactional outbox, and, like `cmd/mcp`, it never
starts the relay and never dials Kafka.

### Two curated skills, not one per endpoint

| Skill | Kind | Derived from | What it does |
|---|---|---|---|
| `path_capacity` | read | MCP `get_process_path_capacity` | The end-to-end capacity of one registered process path at a location for `[windowStart, windowEnd)` in ORDER per HOUR, the bottleneck step and the constraint binding it, each step's own rate and the composition warnings ([ADR 0002](./0002-station-capacity-composition.md), window coverage per [ADR 0003](./0003-window-coverage-semantics.md)). Persists nothing. |
| `evaluate_capacity_plan` | write | MCP `create_capacity_plan` | Evaluates the demand assigned to the window against the path's capacity and records a **DRAFT** `CapacityPlan` (capacity over the window, shortage, bottleneck), exactly as REST and MCP do. `assignedDemand` is optional: omitted, it is the number of orders order-management has promised at the location in the window ([ADR 0004](./0004-demand-ingestion-from-order-management.md)), and the call is rejected with nothing written when there are none. |

The same curation discipline as [ADR 0008](./0008-mcp-server-adoption.md):
goal-shaped, never one skill per endpoint.

### The write gate

A skill may change state only if the context's MCP surface already offers that
write. Both skills are derived from MCP tools that exist
(`get_process_path_capacity`, `create_capacity_plan`), so nothing is widened.
Deliberately **not** offered over A2A although MCP and REST have them:
`publish_capacity_plan` (publishing a plan is the decision, left to the
caller's governance and the REST/MCP surfaces), `register_process_capacity_constraint`,
`register_process_path`, `declare_station_standard`,
`declare_inbound_labour_standard` and the other reads. A request naming any of
them is answered as an in-band `unknown-skill` problem. A delegating agent gets
the evaluation (a DRAFT plus its shortage), not the authority to announce it.

### Message-only, answered in-band, REST's classification

Both skills are transactional request/response work, so the agent is
message-only (no Task lifecycle). Domain rejections and malformed requests are
**answered** as RFC 7807-shaped `problem`s inside the message envelope, never
raised as JSON-RPC transport errors. The `type` URI namespace is the REST
adapter's, and the **status classification is the REST adapter's**
(`statusFor`): `404` unknown process path, `422` domain validation (inverted
window, missing step capacity, missing or non-positive conversion factor,
negative or missing demand), `500` anything else (detail withheld, logged
server-side). Request-shape problems (malformed skill request, unknown skill,
missing or unknown argument, an unparsable window bound) are `400`, matching
REST's `malformed-window-start` / `-end`. REST has no `409` or `503` reachable
from these two skills, so neither is answered.

### Strict input, one vocabulary

- Unknown argument names are **rejected**, not ignored, and trailing data after
  the JSON document is rejected (a misspelled `unitsPerOrdr` must not silently
  drop a conversion factor). Go's `encoding/json` matches names
  case-insensitively, so `unitsperorder` still reaches `unitsPerOrder`.
- Arguments are camelCase. The snake_case names of REST and MCP
  (`assigned_demand`, `window_start`, ...) are **not accepted**, and a test pins
  that neither the card nor any answer mentions them: the shared domain error
  for an omitted demand names `assigned_demand`, so it is mapped to
  adapter-owned text that says `assignedDemand`, or an agent following the
  answer would be refused as an unknown argument.
- `siteId` is the REQUIRED canonical site code, never inferred from
  `warehouseId` or `location` ([ADR 0012](./0012-canonical-site-id-on-capacity-plans.md)).

### Facts still travel on Kafka

A2A never becomes a data-sync channel. `evaluate_capacity_plan` stores the plan
and its `CapacityPlanCreated` CloudEvents (integration and analytics topics,
[ADR 0005](./0005-analytics-read-side.md)) in `outbox_events` inside one
`UnitOfWork`, the same rows `POST /capacity-plans` writes; `cmd/a2a` never runs
the relay (only `cmd/api` drains `outbox_events`). Proven on real Postgres by
`TestA2AWriteSkillLeavesUnpublishedOutboxRows` (build tag `integration`).

### No outbound A2A, no cross-context calls

This is an inbound adapter only. The context still makes no live call to a
sibling over REST, MCP or A2A (CLAUDE.md rule 5): demand arrives as events into
the local read model, and `evaluate_capacity_plan` reads that model, not
order-management.

### Unauthenticated, like REST and MCP

The card declares no security schemes: this surface is unauthenticated by the
same fleet-wide decision as REST and MCP
(`TestNoAuthMiddlewareReintroduced`). Schemes are added to the card, not a
redesign, if that decision is ever reversed.

## Consequences

- A fourth deployable (`/app/a2a`, built by the Dockerfile's `cmd/*` loop) in
  the same image and an `a2a.*` chart block (`<release>-a2a`, default **off**),
  exposed through Kong at `/api/a2a/warehouse-planning` rewritten to `/`.
  `A2A_AGENT_URL` (the absolute URL on the card) is mandatory: the binary
  refuses to boot without it; the chart defaults it to the in-cluster Service
  DNS and `a2a.agentUrl` overrides it.
- `evaluate_capacity_plan` is **not idempotent**: every accepted call records a
  new DRAFT plan with a new id (like `POST /capacity-plans`), so a delegating
  agent must not blindly retry an ambiguous failure. The card says so. Using
  the A2A `messageId` as an idempotency key is a possible follow-up; the MCP
  tool has the same property today.
- Delegating *to* this agent does not change who decides: whether a caller may
  *ask* for a plan is that caller's governance, and publishing stays outside
  this surface.
- Port 8093 is the fleet's A2A port (8080 api, 8090 mcp, 8091 projector admin,
  8092 reports API).
- `warehouse-infra` needs a separate flip (`a2a.enabled`, the route path and the
  gateway `parentRefs`) after the image carries `/app/a2a`; the chart's
  `tests/warehouse-infra-*-values.yaml` copies carry that block as an
  anticipated change until a refresh brings it.
