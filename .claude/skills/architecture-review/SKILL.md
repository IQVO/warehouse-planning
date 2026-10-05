---
name: architecture-review
description: Bounded-context boundary and ADR-compliance review of a warehouse-planning change (expensive, post-integration) - hexagonal direction, no live cross-context calls, outbox-only publishing, read-time station composition (ADR 0002), window coverage (ADR 0003), contradicted ADRs. Invoke explicitly - /architecture-review [range].
disable-model-invocation: true
argument-hint: "[git range]"
---

Perform a bounded-context boundary and ADR-compliance review of the current
changes (or `$ARGUMENTS` if given, e.g. `origin/develop..HEAD`).

This is EXPENSIVE relative to `/code-review`: it reasons about
cross-context implications, not just this diff's local correctness. Use it
before merging a PR that touches architecture (a new integration, a new port
or adapter, a changed event contract, a changed capacity rule), not on every
small commit. The ADRs in `docs/adr/` are the standing decisions:

- ADR 0001 (`docs/adr/0001-warehouse-planning-bounded-context.md`): why this
  context exists, the context map, "no live cross-context lookup", and its
  Addendum on the confirmed upstream contracts.
- ADR 0002 (`docs/adr/0002-station-capacity-composition.md`): station
  capacity is composed at READ time; storage positions are a read model.
- ADR 0003 (`docs/adr/0003-window-coverage-semantics.md`): a registered
  window applies when it COVERS the planning window.

## What to check, in priority order

1. **Hexagonal dependency direction.** Run `make arch-test` first
   (`internal/architecture/architecture_test.go`, `TestHexagonalArchitecture`:
   domain depends on nothing internal but domain; application only on
   domain and application; inbound adapters never on outbound adapters and
   vice versa; only `cmd/` wires layers). If it is red, report that and
   stop; do not hand-review what a fitness test already caught.
2. **No live cross-context lookup (ADR 0001).** Look for any new HTTP client,
   MCP client or other synchronous call to a sibling context
   (`workforce-management`, `facility-layout`, `process-path-management`,
   `order-management`, ...) in `internal/adapters/outbound/` or in a use
   case. Capacity-relevant facts arrive as Kafka events and live in local read
   models (`location_slot_tally`, `processed_events`, `process_capacity`).
   `inventory-storage` stock is explicitly NOT capacity and must never be read.
   See `.claude/rules/fleet/context-boundaries.md`.
3. **Event contracts.** A new consumed `type`, topic or payload field is a
   cross-context decision: it must be recorded in ADR 0001's Addendum or a
   new ADR and in `.claude/rules/integration-events.md`, and the `type` string
   must match the producer's own `apis/asyncapi.yaml` byte for byte.
   `process-path-management` is deliberately not consumed (its ProcessPath has
   no step sequence); a change that starts consuming it contradicts the
   Addendum. A published-payload change needs a new `.v2` type, never a
   mutation (`internal/adapters/outbound/kafka/encoder.go`).
4. **Outbox only.** Events leave through the transactional outbox:
   `UnitOfWork.Do` saves the aggregate and inserts the encoded CloudEvents
   (`PublishCapacityPlan`, `CreateCapacityPlan`); the relay in `cmd/api`
   drains it. Flag any direct `kafkago.Writer` use in a handler or use case,
   and any attempt to start the relay or dial Kafka from `cmd/mcp`.
5. **ADR 0002 / ADR 0003 invariants.** No derived station or storage value is
   stored as a `ProcessCapacity` (no sentinel `ProcessType`, no standing
   window, no `LINE` constant); composition happens at read time in
   `ComposeStepCapacity`. Capacity lookups go through coverage
   (`ProcessCapacityRepository.FindCovering`, `CapacityWindow.Covers`), not an
   exact-window match, except the single-aggregate register/get endpoints
   that keep the exact window as identity. A change that quietly reverts either
   is NEEDS-ADR, not a code fix.
6. **MCP additive-boundary (`TestMCPAdapterDependencyRule`).** The MCP adapter
   depends only on application and domain, and nothing depends on it. Confirm
   the test is green. The tool surface is capped at 10 (`TestToolSurface`).
7. **Kafka consumer correctness.** Consumer group ids come from env vars
   (`LABOR_CAPACITY_CONSUMER_GROUP`, `STORAGE_CAPACITY_CONSUMER_GROUP` in
   `cmd/api/main.go`), never an inline literal; offsets are committed only
   after `HandleMessage` returned nil; the processed-event `Claim` and the
   effect share one `UnitOfWork`. A consumer that replays a whole topic into
   a local cache needs a per-process-unique group and `CommitInterval`
   (`.claude/rules/fleet/kafka-testing-and-consumers.md`).
8. **Auth and frontend.** No bearer/JWT/API-key layer
   (`TestNoAuthMiddlewareReintroduced`; re-adopting auth needs a user
   decision plus an ADR). `web/` is the `capacity_mfe` remote: it may call only
   this service's own REST API, and nothing under `internal/` may know it exists.
9. **Undocumented architecture.** If the change introduces or alters a
   cross-context contract, a new port family, or a new runtime process, check
   an ADR exists (see `.claude/skills/how-to-write-an-adr/SKILL.md`) and that
   `docs/docs/overview/context.md` and `.claude/rules/` still tell the truth.

## Output format

State clearly: PASS (no architectural concerns), CONCERNS (each tied to the
specific rule/ADR/test it would violate), or NEEDS-ADR (the change is sound
but undocumented; name what the ADR should cover). Cite the specific
file/rule/ADR for every finding; a finding with no citation is not actionable.

This is advisory. It never blocks a merge on its own and never modifies
files. If a finding conflicts with a decision explicitly stated in this
repo's `CLAUDE.md`, defer to that document and say so.
