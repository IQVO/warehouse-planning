---
id: testing
title: Testing
sidebar_label: Testing
---

# Testing

The test pyramid as it exists in this repo, the `make` targets that run each
layer and the CI jobs in `.github/workflows/ci.yml` that gate a pull request.
Counts below are `func TestXxx` functions on the current `develop`, not
sub-tests.

## The layers

| Layer | Where | Count | How it runs | Needs |
| --- | --- | --- | --- | --- |
| Domain unit tests | `internal/domain/{processcapacity,capacityplan,processpath,demand}` | 91 tests in 12 files | `make test` | nothing |
| Application (use case) tests | `internal/application/usecases` | 68 tests in 10 files | `make test`, with the in-memory adapters | nothing |
| Analytics report tests | `internal/analytics/report` | 17 tests in 2 files | `make test` | nothing |
| Adapter tests (httptest, fake Kafka reader/writer, in-memory stores) | `internal/adapters/...` (http 57, kafka inbound 65, mcp 19, outbound kafka 22, memory 16, outbox 8, cloudevents 6, analyticsstore 3) | 196 tests | `make test` | nothing |
| Composition-root tests | `cmd/*/main_test.go`, `cmd/api/demand_test.go` | 26 tests | `make test` | nothing |
| BDD acceptance (godog) | `features/*.feature`, driven by `features_test.go`, `features_demand_test.go`, `features_list_test.go` at the repo root | 6 features, 34 scenarios | `make bdd` (`go test ./... -run TestFeatures -v`), also part of `make test` | nothing |
| Integration (`-tags=integration`) | 18 files: the Postgres repositories and unit of work, the outbox relay, the three Kafka consumers, the analytics store, `cmd/mcp`, and `analytics_e2e_integration_test.go` | 56 tests | `make integration` (`go test -tags=integration ./... -race -count=1`) | Docker |
| Architecture fitness | `internal/architecture` | 12 tests | `make arch-test` | nothing |
| Mutation | `.gremlins.yaml` | see below | `make mutation-fast`, `make mutation-full` | `gremlins` v0.6.0 |
| Frontend remote | `web/src/**/*.test.ts(x)` (8 files, vitest + Testing Library) | | `cd web && npm test` | Node, a built `../../warehouse-ui-kit` |
| Chart wiring | `charts/warehouse-planning/tests/test_service_selectors.py` | | `python3 charts/warehouse-planning/tests/test_service_selectors.py` | Python with `pyyaml`, Helm |
| Agent harness | `scripts/harness/test_hook.py`, `test_repo_lint.py`; `guide_lint.py`, `repo_lint.py` | | `make harness-test`, `make guide-lint` | Python |

Not present in this repo: contract tests against the live API (no schemathesis
job, no `.schemathesis/`), and evals. The OpenAPI and AsyncAPI specs are
linted with Spectral (`api-lint`), not exercised.

### BDD features

The godog suite drives the **real chi router** over `httptest` with in-memory
repositories and outbox. The Kafka-fed read models are filled by handing real
CloudEvents bytes to the real consumers' `HandleMessage`. `Strict: true` fails
a scenario with an undefined step.

| Feature file | Scenarios | What it pins |
| --- | --- | --- |
| `capacity_plan.feature` | 7 | the 12,000-order worked example (shortage 4,000, bottleneck REBIN, the four outbox events, a second publish = 409), no shortage under capacity, unknown path, negative demand, unknown plan, window coverage for plans |
| `expected_demand.feature` | 8 | the demand window `[start, end)`, latest event wins, ignored events, site attribution, `demand_source` orders vs request, no data = 422, RFC 7807 validation |
| `list_endpoints.feature` | 5 | `GET /process-paths` and `GET /capacity-plans` (empty, ordering, `PUBLISHED`, bad `limit`) |
| `process_capacity.feature` | 3 | effective rate bound by LOCATION, unknown capacity = 404, unit mismatch = 409 |
| `process_path_capacity.feature` | 5 | Pick, Rebin, Pack bound by Rebin, unknown path, missing step capacity, live-shaped windows resolved by coverage |
| `station_capacity.feature` | 6 | stations binding a path once a standard exists, the no-standard warning, declare twice = replace, the storage-capacity read model |

### Integration tests

Every integration test starts its own containers with testcontainers
(`postgres:16-alpine`, `confluentinc/confluent-local:7.6.1`); none reads
`DATABASE_URL` or `KAFKA_BROKERS`, and two fitness tests enforce that. The
widest one, `analytics_e2e_integration_test.go`, covers the whole read side:
plan created and published, outbox, relay, Kafka, projector, analytical
database, report.

### Architecture fitness tests

| Test | Enforces |
| --- | --- |
| `TestHexagonalArchitecture` | dependencies point inward; inbound and outbound adapters never import each other (arch-go) |
| `TestMCPAdapterDependencyRule` | the MCP adapter depends only on application and domain, and nothing depends on it (ADR 0008) |
| `TestNoAuthMiddlewareReintroduced` | no bearer/JWT middleware under `internal/adapters/inbound` |
| `TestKafkaConsumerGroupNeverHardcodedInline` | consumer group ids come from the composition root, never a literal |
| `TestKafkaIntegrationTestsUseTestcontainers`, `TestPostgresIntegrationTestsUseTestcontainers`, `TestPostgresIntegrationSensorFailsOnBadFixtures` | integration tests boot their own Kafka/Postgres; the sensor itself is proven to fail on bad fixtures |
| `TestNoEventEnvelopeToggleOrFlatEnvelope`, `TestCloudEventsOnly` | CloudEvents 1.0 is the only envelope; no envelope toggle; kafka-go users must build envelopes with the CloudEvents SDK |
| `TestReplayConsumersSetCommitInterval` | a full-replay cache consumer (unique group) must set its commit interval |
| `TestEventCatalogueMatchesContract`, `TestEventCatalogueDetector` | every type this service declares in `apis/asyncapi.yaml` appears in the CloudEvents catalogue ADR (`docs/adr/0006-...`); the detector is itself tested |

`internal/adapters/inbound/mcp/governance_test.go` also pins the MCP tool
budget (`maxTools` = 11).

### Coverage and mutation gates

- **Coverage**: `make coverage` and the CI `test` job run `go test ./... -race -coverprofile=coverage.out -coverpkg=./internal/domain/...,./internal/application/...,./internal/analytics/...` and fail below **90 %**.
- **Mutation** (`.gremlins.yaml`, gremlins v0.6.0): `workers: 1`, `timeout-coefficient: 30`, thresholds **efficacy 99** and **mutant-coverage 99** (gremlins fails when the value is ≤ the threshold, so any surviving mutant fails). `make mutation-fast` and the `mutation-fast` job run one `gremlins unleash` per package: `./internal/domain/processcapacity`, `./internal/domain/capacityplan`, `./internal/domain/processpath`, `./internal/domain/demand`, `./internal/analytics/report`. `make mutation-full` and the scheduled `mutation` job run `./internal/domain` as a whole. The last recorded measurement (comments in `.gremlins.yaml`) is 100 % efficacy and coverage in every package.

## Make targets

| Target | Runs |
| --- | --- |
| `make build` / `vet` / `fmt` / `fmt-check` | `go build ./...`, `go vet ./...`, `gofmt -w .`, fail on `gofmt -l .` output |
| `make lint` | `golangci-lint run ./...` (v2.14.0 in CI) |
| `make test` | `go test ./... -race` (unit, httptest, BDD) |
| `make coverage` | the CI coverage command plus the 90 % gate |
| `make bdd` | `go test ./... -run TestFeatures -v` |
| `make arch-test` | `go test ./internal/architecture/... -v` |
| `make integration` | `go test -tags=integration ./... -race -count=1` (Docker) |
| `make mutation-fast` (alias `mutation`), `make mutation-full` | gremlins, as above |
| `make vuln` | `govulncheck ./...` |
| `make check` | `fmt-check vet build lint test`: the lefthook pre-push gate |
| `make check-all` | `check coverage arch-test bdd` |
| `make check-fast` | `fmt-check vet arch-test` plus the tests of packages changed vs `HEAD` |
| `make guide-lint`, `make harness-test` | agent-guide lint and hook tests |

`lefthook.yml` runs `fmt-check`, `vet` and `lint` on commit and `make check`
on push, once `lefthook install` has been run in the clone.

## CI jobs (`.github/workflows/ci.yml`)

Triggers: push and pull request on `main` and `develop`, a weekly schedule
(Monday 06:00 UTC) and manual dispatch.

| Job | Runs | When |
| --- | --- | --- |
| `lint` | golangci-lint v2.14.0 | every push/PR |
| `guide-lint` | `guide_lint.py`, `repo_lint.py`, `test_hook.py`, `test_repo_lint.py` | every push/PR |
| `complexity` | golangci-lint with only `gocyclo`, `gocognit`, `cyclop`, `funlen`, `nestif` (cyclomatic 15, cognitive 20, nesting 5, 80 lines / 50 statements) plus an informational gocyclo > 10 report | every push/PR |
| `test` | build, vet, race tests with coverage, the 90 % gate | every push/PR |
| `bdd` | `go test ./... -run TestFeatures -v` | every push/PR |
| `integration` | `go test -tags=integration ./... -race -count=1` on the runner's Docker | every push/PR |
| `mutation-fast` | gremlins on the five packages | every push/PR |
| `mutation` | gremlins on `./internal/domain`; opens or closes a `harness:red` issue on schedule | schedule, dispatch |
| `drift` | `deadcode`, `go mod tidy -diff`, coverage-quality (high-coverage functions with a surviving mutant); advisory | schedule, dispatch |
| `api-lint` | Spectral on `apis/openapi.yaml` and `apis/asyncapi.yaml` (`--fail-severity=warn`) | every push/PR |
| `vuln` | `govulncheck ./...` | every push/PR |
| `docs-api-drift` | regenerates `docs/docs/api-reference/rest` from the OpenAPI spec and fails on a diff | every push/PR |
| `helm-lint` | `ct lint` on the chart and the Service-selector test | pull requests |
| `arch-test` | `go test ./internal/architecture/... -v` | every push/PR |
| `web` | `web/` lint (oxlint), `tsc -b`, vitest, build, against a sibling `warehouse-ui-kit` checkout | every push/PR |
| `trivy-scan` | builds the image and scans it (blocks on fixable CRITICAL/HIGH) | pull requests |
| `docker-publish`, `release` | push to GHCR with cosign signature and SBOM; tag, release and Helm chart | push to `main` only |

Two more workflows: `.github/workflows/docs.yml` builds this Docusaurus site on
every pull request touching `docs/**` or `apis/**` and deploys it to GitHub
Pages on push to `main`; `.github/workflows/ai-review.yml` posts an advisory
architecture review on PRs into `develop` that touch domain, application, APIs
or charts (a no-op without the `ANTHROPIC_API_KEY` secret).
