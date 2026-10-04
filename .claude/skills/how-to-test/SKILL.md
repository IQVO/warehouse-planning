---
name: how-to-test
description: Write or review tests in warehouse-planning and diagnose a red coverage, mutation-fast, bdd, arch-test or integration job - the test layers, the 90% gate, gremlins semantics on the processcapacity/capacityplan/processpath domain packages, the testcontainers rule. Use when adding tests, killing a surviving mutant, or fixing a red check.
---

# How to test

Passing `go test` is the WEAKEST of this repo's signals: mutation testing
exists because green tests can assert nothing. The layers, with the real
make targets and the CI job each one mirrors:

| Layer | Command | Proves |
| --- | --- | --- |
| Unit / httptest | `make test` (`go test ./... -race`) | code runs; table-driven, memory adapters only |
| Coverage | `make coverage` | lines executed; 90% gate on `./internal/domain/...,./internal/application/...` |
| Mutation (fast subset) | `make mutation-fast` | tests actually ASSERT; gremlins on `./internal/domain/processcapacity`, `./internal/domain/capacityplan`, `./internal/domain/processpath` (`MUTATION_FAST_PKGS` in the `Makefile` = the CI `mutation-fast` job) |
| BDD | `make bdd` | godog scenarios through the real chi router, in-memory repos and outbox |
| Architecture | `make arch-test` | hexagonal dependency rule and fleet fitness tests |
| Integration | `make integration` (`-tags=integration`, needs Docker) | real Postgres and Kafka via testcontainers; NOT in `check-all` |

Loop while editing: `make check-fast`. Before pushing: `make check-all`
(check + coverage + arch-test + bdd). CI also runs `mutation-fast`, `vuln`,
`api-lint` and `docs-api-drift`, none of which are in `check-all`, so run
`make mutation-fast` yourself after changing domain code.

## Where tests live and what they use

- Domain: `internal/domain/<aggregate>/*_test.go`, plain table-driven tests,
  no I/O. Worked-example numbers (the "section 43" Pick 4000 / Rebin 2500 /
  Pack 1800, demand 12000, shortage 4000) are fixtures in
  `capacity_plan_test.go` and `features/capacity_plan.feature`.
- Use cases: `internal/application/usecases/*_test.go` against
  `internal/adapters/outbound/memory/` (never a network or DB call). Cover
  the success path AND each domain-rule failure path.
- HTTP: `internal/adapters/inbound/http/*_handler_test.go`, an
  `httptest.Server` over `NewRouter` (see `newStationServer`).
- Kafka consumers: `internal/adapters/inbound/kafka/*_test.go` with a
  `fakeReader`; atomicity in `atomic_*_test.go`.
- BDD: `features/*.feature` plus the step definitions in `features_test.go`
  at the repo root.

## Mutation testing: what gremlins demands here

`.gremlins.yaml` sets `efficacy: 99` and `mutant-coverage: 99`. gremlins
fails when the measured value is `<=` the threshold, so the threshold must
sit strictly below what the code achieves; every package is currently at
100%, so ANY surviving mutant in the fast subset fails CI. The file carries
a dated re-measurement log. Never lower a threshold to make a run pass
(`.claude/rules/fleet/gitflow-and-ci.md`); kill the mutant with a better
assertion. When you add domain code, re-measure per package exactly like
CI does (one run per package) and append a dated note to the
`.gremlins.yaml` comment:

```bash
gremlins unleash ./internal/domain/processcapacity
gremlins unleash ./internal/domain/capacityplan
gremlins unleash ./internal/domain/processpath
```

(`make mutation-fast` loops them; `make mutation-full` is the exhaustive
`./internal/domain` run the weekly job does.)

### Pitfalls that apply to this domain

1. **Boundary guards need the boundary value.** `CapacityWindow.Covers`
   (`start <= W.start AND end >= W.end`, ADR 0003) is pinned by
   `TestCapacityWindow_Covers` with a case one second either side of each
   bound, and `TestComposeStepCapacity_StationCountBoundary` pins the
   "station count > 0" edge. A shortage of exactly zero when demand equals
   capacity is `TestShortageBoundary`. Every `< 0` / `> 0` / `<= 0` guard
   (negative quantity, non-positive period, non-positive station standard)
   needs the exact threshold value in a test, asserting the exact result.
2. **Distinct, non-zero fixtures.** A `CapacityRate` test with equal or zero
   operands makes `+`/`-` and `*`/`/` mutants in normalization identical.
   Use a different non-zero factor per field (`units_per_order` 2.5,
   `packages_per_order` 1) and assert the exact ORDER/hour number.
3. **Do not chase equivalent mutants.** `Shortage = math.Max(0, demand -
   capacity)` was written that way precisely because an
   `if demand > capacity` leaves a `>` -> `>=` mutant that is
   behaviourally identical (both give 0 at the boundary); and gremlins does
   not mutate `Covers`' `After`/`Before` calls at all (see the
   `.gremlins.yaml` notes). Prefer rewriting the code to remove the
   equivalent mutant over forcing a fixture that pins an unspecified
   tie-break. Tie-breaks that ARE specified (ties go to the earliest
   candidate in `ComposeStepCapacity`, narrower window wins in ADR 0003)
   do need a test each.
4. When `mutation-fast` goes red, diff against `origin/develop`: run the
   same `gremlins unleash <pkg>` on your branch and on a clean checkout of
   `origin/develop` and treat only the NEW lived mutants as your regression.

## Integration tests: testcontainers, never a skip-gate

Every `-tags=integration` test in this repo starts its own containers with
`testcontainers-go`: Postgres (`startPostgres` in
`internal/adapters/outbound/postgres/process_capacity_repository_integration_test.go`,
`startPostgresForKafkaTests`) and Kafka (`startKafkaBroker` in
`internal/adapters/inbound/kafka/main_integration_test.go`: one shared
broker per package from `TestMain`, a unique topic per test, explicit
`createTopic` before the first read/write). Never gate on an env var plus
`t.Skip`, never hardcode `localhost:9092`
(`TestKafkaIntegrationTestsUseTestcontainers`, and
`.claude/rules/fleet/kafka-testing-and-consumers.md`). CI's `integration`
job needs Docker on the runner; the Postgres service container it declares
is not what these tests use.

## Verify before opening the PR

```bash
make check-all
make mutation-fast   # if you touched internal/domain
```
