---
name: code-review
description: Fast pre-commit semantic review of uncommitted changes or a git range in warehouse-planning - domain logic in the wrong layer, missing failure-path tests, adapter concerns in domain types, impure ports, errors.go mapping, outbox and consumer atomicity. Advisory counterpart to make check. Invoke explicitly - /code-review [range].
disable-model-invocation: true
argument-hint: "[git range]"
---

Perform a pre-commit semantic code review of the current uncommitted
changes (or, if `$ARGUMENTS` names a range, that diff, e.g.
`/code-review origin/develop..HEAD`).

This is a fast, local, advisory pass: the counterpart to the mechanical
checks, not a replacement. Run `make check-fast` (or `make check`) FIRST and
do not spend attention on anything the formatter, vet, linter or fitness
tests already catch.

## What to look for, in priority order

1. **Domain logic in the wrong layer.** Business rules belong in
   `internal/domain/` (`processcapacity`, `capacityplan`, `processpath`). A
   handler in `internal/adapters/inbound/http/` should do decode -> use case
   -> encode and nothing more (compare `handlePublishCapacityPlan`); a Kafka
   consumer (`internal/adapters/inbound/kafka/`) decodes, validates payload
   shape and calls a use case. Flag arithmetic, normalization or capacity
   comparison anywhere but the domain (`ComposeStepCapacity`,
   `NormalizeToOrderRate`, `CapacityWindow.Covers`).
2. **A new/changed use case with no failure-path test.** Every `Handle` in
   `internal/application/usecases/` needs a test for its domain-rule and
   infrastructure-failure paths, not just the happy path. The 90% coverage gate
   clears on happy paths alone; this is the most common gap.
3. **A domain type carrying an adapter concern.** JSON tags, SQL column
   names or HTTP status codes under `internal/domain/` (there are none today:
   DTOs live in `internal/adapters/inbound/http/dto.go`). The fitness tests
   check import direction, not tags, so flag it by eye.
4. **A port that is not a pure interface.** `internal/application/ports/` holds
   interfaces only; a use case must depend on a port, never a concrete
   adapter. Concrete structs shared by use cases and adapters (the outbox
   `Message`, the tally `Update`) live in `internal/application/outbox` and
   `internal/application/tally`, not in ports.
5. **Error mapping.** A new typed error needs an entry in BOTH `statusFor`
   and `problemCatalog` in `internal/adapters/inbound/http/errors.go`, and in
   `slugFor` in `internal/adapters/inbound/mcp/errors.go` if an MCP tool can
   reach it. A default 500 for a domain error is a bug. Array fields must
   serialize as `[]`, never null (`nonNilStrings`).
6. **Atomicity of writes that emit events or consume them.** An event-emitting
   use case saves and enqueues in ONE `UnitOfWork.Do`; no Kafka write outside
   the relay. A consumer's processed-event `Claim` and its effect share one
   `UnitOfWork.Do`, never a claim before the work. Time comes from an
   injectable `Now`, ids from an injectable `NewID`, not bare calls in the
   use case.
7. **A new Kafka `GroupID` as an inline string literal**
   (`TestKafkaConsumerGroupNeverHardcodedInline` catches it in CI; flag it here
   too), or a Kafka/Postgres integration test that skips on an env var instead
   of using testcontainers.
8. **A reintroduced auth/bearer/JWT check.** Every REST and MCP endpoint is
   deliberately unauthenticated (`.claude/rules/fleet/no-auth-and-mcp.md`); an
   "obviously missing" auth check is very likely an agent mistake.
9. **Sibling-context boundary.** No outbound REST/MCP call to another bounded
   context and no import of another context's Go packages
   (`.claude/rules/fleet/context-boundaries.md`, ADR 0001).
10. **Ubiquitous language and docs drift in the same PR.** A changed endpoint or
    event without the matching `apis/openapi.yaml` / `apis/asyncapi.yaml`
    edit, the regenerated `docs/docs/api-reference/rest/` output
    (`docs-api-drift` fails otherwise), or the matching `.claude/rules/`
    page. See `/domain-review` for the naming side.

## Output format

For each finding: file:line, a one-sentence description, and a one-sentence
suggested fix, grouped by severity (blocking / should-fix / nit). If nothing
needs fixing, say so plainly; do not manufacture findings.

This command never modifies files and never runs `git commit` or `git push`.
