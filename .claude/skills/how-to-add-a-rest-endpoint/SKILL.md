---
name: how-to-add-a-rest-endpoint
description: Add or change a REST endpoint in warehouse-planning in hexagonal order (domain invariant, use case, port, chi handler, errors.go, apis/openapi.yaml, regenerated Docusaurus API reference, godog scenario). Use when touching internal/adapters/inbound/http, apis/openapi.yaml, or exposing a use case over HTTP.
---

# How to add a REST endpoint

Use when asked to add or change a REST endpoint. Go domain first, adapter
last: a handler written before the invariant it enforces validates nothing
and gets bypassed by the use case. There is no auth layer to wire (never add
one, see `.claude/rules/fleet/no-auth-and-mcp.md`).

Worked examples in this repo, read them next to this guide:

- `PUT /station-standards/{location}/{process_type}` (ADR 0002): a plain
  upsert. `internal/application/usecases/declare_station_standard.go` +
  `handleDeclareStationStandard` in `internal/adapters/inbound/http/station_handler.go`.
- `POST /capacity-plans/{id}/publish`: a write that also emits events through
  the transactional outbox. `internal/application/usecases/publish_capacity_plan.go` +
  `handlePublishCapacityPlan` in `internal/adapters/inbound/http/capacity_plan_handler.go`.

## 1. Domain first: does the invariant exist?

Look in `internal/domain/<aggregate>/` (`processcapacity`, `capacityplan`,
`processpath`). The endpoint decodes, calls a use case and encodes; business
rules live in the aggregate or value object. For the station-standard
endpoint the rule ("throughput per station is positive, unit is UNIT,
PACKAGE or ORDER") is `processcapacity.NewStationStandard` in
`internal/domain/processcapacity/station_standard.go`, tested in
`station_standard_test.go`. Add a new rule there, with a table-driven test
(including the exact boundary value), BEFORE touching the layers above.
The domain package depends on nothing internal but the domain
(`TestHexagonalArchitecture`); no JSON tags, no HTTP status codes in it.

## 2. Application: one use case per file

New file in `internal/application/usecases/`. This repo's shape is a
`<Verb><Noun>` struct holding ports plus a `Handle(ctx, cmd)` method (not
`Execute`), with a `<Verb><Noun>Command` struct carrying domain types:

```go
type DeclareStationStandard struct {
	Repo ports.StationStandardRepository // ports only, never a concrete adapter
}

func (uc *DeclareStationStandard) Handle(ctx context.Context, cmd DeclareStationStandardCommand) (processcapacity.StationStandard, bool, error)
```

- Reads that have no invariant are NOT use cases: the handler calls the
  repository port directly (`s.CapacityPlans.FindByID`,
  `s.StationStandards.List`, `s.ProcessCapacities.FindByProcessLocationWindow`).
- A write that must emit an integration event takes
  `ports.UnitOfWork`, `ports.OutboxRepository` and `ports.EventEncoder`
  and runs the save plus `enqueue(...)` inside ONE `UnitOfWork.Do`, exactly like
  `PublishCapacityPlan`. Never publish to Kafka from a handler or use case
  (see `.claude/skills/how-to-add-an-integration-event/SKILL.md`).
- Time comes in through a `Now func() time.Time` field (`utcNow` default),
  never a bare `time.Now()` in the use case.
- Add a port to `internal/application/ports/` only if none fits; ports are
  interfaces only. Implement it twice: `internal/adapters/outbound/memory/`
  (unit tests, BDD, no-DB mode) and `internal/adapters/outbound/postgres/`
  (new migration under `internal/adapters/outbound/postgres/migrations/` as
  an `NNNN_name.up.sql` + `.down.sql` pair; the next free number, check
  `ls`).
- Test in `internal/application/usecases/*_test.go` against the memory
  repositories: success path AND each domain-rule failure path. The 90%
  coverage gate (`make coverage`) clears on happy paths alone, so the
  failure path is the thing reviewers look for.

## 3. Adapter: chi handler in `internal/adapters/inbound/http/`

1. `dto.go`: request/response structs with snake_case JSON tags
   (`declareStationStandardRequest`, `stationStandardResponse`). DTOs live
   only here. Use pointer fields for "required but must not default to
   zero" values (see `AssignedDemand *float64` in `createCapacityPlanRequest`,
   answered with `missing-assigned-demand`).
2. A handler method on `*Server` in the file for that aggregate
   (`capacity_plan_handler.go`, `process_capacity_handler.go`,
   `process_path_handler.go`, `station_handler.go`; a new aggregate gets
   its own `<name>_handler.go`): `decodeJSON(w, r, &req)` (it writes the
   400 `malformed-json` itself and returns false), parse timestamps with
   `time.RFC3339` and answer `writeProblem(...)` on malformed input, call the
   use case, `writeError(w, r, err)` on error, `writeJSON(w, status, dto)`.
   Path params come from `chi.URLParam`, query from `r.URL.Query()`.
3. Add the use case/port as a field on the `Server` struct and the route in
   `NewRouter`, both in `process_capacity_handler.go`
   (`r.Put("/station-standards/{location}/{process_type}", s.handleDeclareStationStandard)`).
   A new HTTP method also goes into the CORS `AllowedMethods` list there
   (`TestRouter_CORSAllowsPut` is the regression test for PUT).
4. `errors.go`: a new typed domain/use-case error needs an entry in BOTH
   `statusFor` (HTTP status) and `problemCatalog` (the RFC 7807
   `problemInfo{slug, title}`; order matters, first `errors.Is` match
   wins). Slugs are kebab-case and are a published contract. The MCP
   adapter keeps a PARALLEL catalog (`slugFor` in
   `internal/adapters/inbound/mcp/errors.go`): if the same use case is
   reachable through an MCP tool, add the error there too.
5. Wire the use case in the composition root `cmd/api/main.go` (the
   `&inboundhttp.Server{...}` literal). `cmd/mcp` builds its own deps and is
   NOT touched unless you also add an MCP tool (see
   `.claude/rules/mcp.md`; the tool budget is pinned at 10 by
   `TestToolSurface`, so a new tool is a deliberate, reviewed change).
6. Tests next to the handler (`station_handler_test.go`,
   `capacity_plan_handler_test.go`): an `httptest.Server` over
   `NewRouter` with memory repositories, one success test and one test per
   error slug you added, asserting the problem `type`.

## 4. Contract: OpenAPI, then regenerate the docs (clean first)

Add or change the path in `apis/openapi.yaml` (operationId in camelCase,
the same `application/problem+json` responses the handler can return).
CI's `api-lint` job runs
`spectral lint apis/openapi.yaml --ruleset .spectral.yaml --fail-severity=warn`.

The Docusaurus site under `docs/` renders the REST reference from that
spec. The generator caches, so ALWAYS clean first, then commit the diff
under `docs/docs/api-reference/rest/` (CI's `docs-api-drift` job fails the
PR otherwise):

```bash
cd docs && npm ci && npm run clean-api-docs warehouse-planning && npm run gen-api-docs warehouse-planning
```

Update the prose in `.claude/rules/rest-api.md` (request/response shapes and
error list) in the same change. `docs/docs/api-reference/events.md` is
hand-written and only matters for event changes.

## 5. Behaviour: a godog scenario

If the endpoint changes planning behaviour, extend a file in `features/`
(`capacity_plan.feature`, `station_capacity.feature`,
`process_capacity.feature`, `process_path_capacity.feature`). Step
definitions are in `features_test.go` at the repo root (`InitializeScenario`
registers them against a real chi router over in-memory repositories and
outbox). Reuse existing steps first; a new step is a `world` method plus an
`sc.Step(...)` line there.

## 6. Verify

```bash
make check-fast   # fmt-check, vet, arch-test, tests of changed packages
make check-all    # check + coverage (90% gate) + arch-test + bdd
```

Integration tests (`make integration`) need Docker for testcontainers and are
not part of `check-all`.
