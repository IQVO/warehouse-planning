# CLAUDE.md — warehouse-planning

`warehouse-planning` is a Core bounded context (`wes` tier) of the
`warehouse-systems` fleet (GitHub org `IQVO`, Go module
`github.com/claudioed/warehouse-planning`). It answers: *can this warehouse
process the demand assigned to it, given its labor, location, equipment,
station, conveyor and buffer constraints?* It owns the `ProcessCapacity` and
`CapacityPlan` aggregates, station-capacity composition and window coverage.
Study project: not production software.

## Hard rules (each one cost an incident or is fitness-tested)

1. **Hexagonal dependency rule.** `internal/domain` depends on nothing internal
   but domain; `internal/application` only on domain and application; inbound
   and outbound adapters never depend on each other; only `cmd/` wires layers.
   No JSON tags, SQL or HTTP types in the domain. `make arch-test` enforces it
   (`internal/architecture/`).
2. **CloudEvents 1.0 is MANDATORY** on every Kafka message produced or
   consumed (structured mode, no flat envelope, no dual-write, no envelope
   toggle). Build/decode ONLY through `internal/adapters/kafka/cloudevents/`;
   `type` is `com.warehouse.wes.warehouse-planning.<entity>.<EventName>`; a
   breaking payload change is a new `.v2` type, never a mutation. Fleet rule:
   `.claude/rules/fleet/cloudevents.md`; this service's types, attributes and
   payloads: `.claude/rules/integration-events.md` and `apis/asyncapi.yaml`.
3. **Events leave through the transactional outbox only**: the aggregate save
   and the encoded CloudEvents commit in one `ports.UnitOfWork.Do`; the relay in
   `cmd/api` publishes. Never write to Kafka from a handler or use case, and
   `cmd/mcp` never starts the relay or dials Kafka.
4. **Consumers are at-least-once with an atomic effect**: commit the offset only
   after success, claim the processed event and apply the effect in ONE unit of
   work, return an error only for transient failures. Every consumer group id
   MUST come from an env var, never a string literal
   (`TestKafkaConsumerGroupNeverHardcodedInline`). Kafka and Postgres
   integration tests MUST use testcontainers
   (`.claude/rules/fleet/kafka-testing-and-consumers.md`).
5. **No live cross-context calls.** Never call a sibling context over REST or
   MCP at request time and never import its Go packages: capacity facts from
   `workforce-management` and `facility-layout` arrive as Kafka events and
   live in local read models (ADR 0001). `process-path-management` is
   deliberately NOT consumed; this context owns its `ProcessPath`. Stock from
   `inventory-storage` is not capacity and is never read.
6. **Station capacity is composed at READ time** (`stationCount x
   StationStandard`, ADR 0002; nothing derived is stored as a
   `ProcessCapacity`) and a registered window applies when it COVERS the
   planning window (ADR 0003). Never compare capacity rates across units
   without normalizing through the `WorkloadProfile` to ORDER.
7. **No auth layer on REST or MCP** (fleet-wide revert 2026-09-11): never add
   bearer/JWT/API-key middleware (`TestNoAuthMiddlewareReintroduced`).
   **MCP is additive**: `cmd/mcp` and `internal/adapters/inbound/mcp/` depend
   only on application and domain, nothing depends on them, Streamable HTTP
   only, tool surface capped at 10 (`.claude/rules/mcp.md`,
   `.claude/rules/fleet/no-auth-and-mcp.md`).
8. **Frontend remote in `web/`.** `capacity_mfe` (Vite + React Module Federation
   remote) is lazy-loaded by `warehouse-console`. It talks only to this service's
   own REST API. Rules: `.claude/rules/frontend.md`; skill:
   `how-to-add-a-frontend-remote`.
9. **Generated docs.** `docs/docs/api-reference/rest/` is generated from
   `apis/openapi.yaml`; never hand-edit it. Regenerate with
   `cd docs && npm run clean-api-docs warehouse-planning && npm run gen-api-docs warehouse-planning`
   (ALWAYS clean first: the generator caches). CI `docs-api-drift` fails
   otherwise.
10. **An ADR comes first** for a new cross-context contract, a custom
    CloudEvents extension attribute or any auth. ADRs live in `docs/adr/`
    (0001-0003 today).

## Commands

```bash
make check-fast   # fmt-check + vet + arch-test + tests of changed packages: run before saying "done"
make check        # fmt-check vet build lint test
make check-all    # check + coverage (90% gate on domain+application) + arch-test + bdd
make integration  # real Postgres/Kafka via testcontainers (needs Docker); not in check-all
make mutation-fast  # gremlins on processcapacity, capacityplan, processpath (blocking in CI)
make guide-lint   # these guides: skills load, references resolve, context budget
```

Run locally: `go run ./cmd/api` (REST `:8080`; in-memory adapters without
`DATABASE_URL`, event publishing in `log` mode without a broker) and
`go run ./cmd/mcp` (`:8090`). Docs site: `cd docs && npm ci && npm run build`.

## Where things are written down (read before editing the matching area)

- `.claude/rules/domain-model.md`: ubiquitous language, aggregates, domain
  events, use cases. `.claude/rules/rest-api.md`: endpoints and error slugs.
  `.claude/rules/integration-events.md`: published/consumed events, outbox,
  idempotency. `.claude/rules/mcp.md`: the MCP tools.
- `.claude/rules/fleet/*.md`: fleet-wide rules copied from the harness
  template; never edit them.
- Skills (recipes with this repo's real files): `how-to-add-a-rest-endpoint`,
  `how-to-add-an-integration-event`, `how-to-test`, `how-to-write-an-adr`;
  review commands `/code-review`, `/domain-review`, `/architecture-review`.
- `docs/adr/`: decisions. `HARNESS.md`: what each sensor runs and why.

Scaffolded from `warehouse-harness-template`; harness-template v3 is in effect
(hooks, guide-lint).

<!-- harness:scoped-rules:start (generated by tools/migrate_v3.py in warehouse-harness-template; do not hand-edit) -->
## Scoped rules and harness

Claude Code loads each rule below automatically when you touch the matching paths. OpenCode and Codex do NOT: read the rule BEFORE editing matching files.

| When touching | Read |
|---|---|
| `internal/domain/**`, `internal/application/**`, `features/**` ... | `.claude/rules/domain-model.md` |
| `web/**` | `.claude/rules/frontend.md` |
| `internal/adapters/**/kafka/**`, `internal/adapters/outbound/events/**`, `apis/asyncapi*` | `.claude/rules/integration-events.md` |
| `internal/adapters/inbound/mcp/**`, `cmd/mcp/**` | `.claude/rules/mcp.md` |
| `internal/adapters/inbound/http/**`, `apis/openapi*.yaml`, `apis/openapi/**` | `.claude/rules/rest-api.md` |

Hooks (`scripts/harness/hook.py`, wired for Claude Code, Codex and OpenCode) block pushes to develop/main, `--no-verify`, bare `rm -rf`, and edits to generated files, and feed gofmt/vet findings back after each edit. Before saying "done" run `make check-fast`; the full gate is `make check-all`. `HARNESS_OFF=1` disables the hooks when debugging the harness itself.
<!-- harness:scoped-rules:end -->
