# 0026. Idempotency replay classes: every problem type is FINAL, OUTCOME or RETRYABLE

- Date: 2026-11-23
- Status: Accepted
- Deciders: owner (fleet decision 2026-11-18), warehouse-planning adoption

## Context

ADR 0019 made every POST sit behind a global `Idempotency-Key` and stored EVERY
answer below 500 with the request's writes in one unit of work. The fleet
replay-class decision (2026-11-18; the rule text is
`.claude/rules/fleet/idempotency-and-outbox.md` in the harness template, the
standard `docs/idempotency-replay-classes.md`) refines that: a retried request
is answered from the store only when its first answer is FINAL for that
request. A stored refusal a retry could pass (an optimistic-lock loss, a fact
not delivered yet) pins a false answer for the 24-hour window to a client that
did the right thing and retried.

## Decision

Every problem type this service can answer has exactly one replay class,
declared on its catalogue entry (`problemInfo.class` in
`internal/adapters/inbound/http/errors.go`; a test holds the table below equal
to the catalogue):

- **FINAL** — a function of the request alone (or of state that only ever
  moves one way): stored and replayed.
- **OUTCOME** — the command COMMITTED state and then answered an error that
  must be remembered: stored AND the writes commit with the stored answer.
- **RETRYABLE** — nothing committed and the answer depends on state that can
  change: NOT stored, the key is released, a retry executes again. Releasing a
  key means a different body under it afterwards is a NEW request.

2xx answers are stored; 5xx answers and panics are never stored; a replay
carries `Idempotency-Replayed: true`. The middleware reads the class the
production problem writer marks on its recorder (`writeProblem`); it holds no
list of slugs and no status ranges beyond 2xx and 5xx. A runtime 4xx no writer
marked (one written around the catalogue) is released like RETRYABLE — the
safe fallback.

### Store design (as found; unchanged by this ADR)

The whole keyed request runs in ONE `ports.UnitOfWork.Do`; the key row, the
aggregate change and the outbox rows commit together. `Lock` takes a
transaction-scoped advisory lock on the key and reads the stored record (24h
retention); the handler runs behind `SAVEPOINT handler_run`; a 5xx returns the
`errServerAnswer` sentinel (rollback, nothing stored); a failed savepoint
release rolls back to the savepoint so a truthful 4xx over an aborted
transaction is still recorded; a 2xx over an aborted transaction fails (500,
nothing cached). **A 4xx's handler writes commit with the key row**, so OUTCOME
has a mechanism in this store. Releasing a RETRYABLE key reuses the 5xx
mechanics: `runKeyed` returns `errRetryableAnswer`, the unit of work rolls back
(key row and advisory lock with it) and the refusal is flushed unstored. A
same-key duplicate in flight blocks on the advisory lock until the first
request's transaction ends and then replays the committed answer — or, when
the first request ended RETRYABLE, executes again because nothing was stored:
the claim-first shape the fleet standard blesses.

### Class table

Held equal to `allProblemTypes()` by
`TestTheADRClassTableMatchesTheCatalogue` (both directions, by (slug, status)).

| Slug | Status | Class |
|---|---|---|
| `invalid-capacity-window` | 400 | FINAL |
| `negative-quantity` | 422 | FINAL |
| `non-positive-period` | 422 | FINAL |
| `unit-mismatch` | 409 | FINAL |
| `non-positive-station-standard` | 422 | FINAL |
| `missing-station-standard-field` | 422 | FINAL |
| `empty-process-path-steps` | 422 | RETRYABLE |
| `non-positive-conversion-factor` | 422 | FINAL |
| `missing-conversion-factor` | 422 | RETRYABLE |
| `unsupported-normalization-unit` | 422 | RETRYABLE |
| `missing-step-capacity` | 422 | RETRYABLE |
| `process-path-not-found` | 404 | RETRYABLE |
| `negative-assigned-demand` | 422 | FINAL |
| `missing-assigned-demand` | 422 | RETRYABLE |
| `missing-required-field` | 422 | FINAL |
| `capacity-plan-already-published` | 409 | FINAL |
| `capacity-plan-not-found` | 404 | FINAL |
| `missing-inbound-standard-field` | 422 | FINAL |
| `non-positive-appointment-minutes` | 422 | FINAL |
| `negative-asn-minutes` | 422 | FINAL |
| `non-finite-minutes` | 422 | FINAL |
| `non-positive-tasks-per-order` | 422 | FINAL |
| `missing-forecast-field` | 422 | FINAL |
| `no-forecast-buckets` | 422 | FINAL |
| `missing-cpt` | 422 | FINAL |
| `negative-bucket-quantity` | 422 | FINAL |
| `workload-forecast-not-found` | 404 | RETRYABLE |
| `missing-labour-plan-field` | 422 | FINAL |
| `invalid-count` | 422 | FINAL |
| `heads-exceed-stations` | 422 | FINAL |
| `non-positive-rate` | 422 | FINAL |
| `non-positive-hours` | 422 | FINAL |
| `throughput-not-finite` | 422 | FINAL |
| `missing-planned-heads` | 422 | RETRYABLE |
| `labour-plan-not-found` | 404 | RETRYABLE |
| `version-conflict` | 409 | RETRYABLE |
| `unknown-demand-source` | 422 | FINAL |
| `missing-units-per-order` | 422 | FINAL |
| `demand-source-conflict` | 422 | FINAL |
| `invalid-scenario` | 422 | FINAL |
| `capacity-plan-superseded` | 409 | FINAL |
| `invalid-comparison-ids` | 422 | FINAL |
| `comparison-site-mismatch` | 422 | FINAL |
| `comparison-window-mismatch` | 422 | FINAL |
| `idempotency-key-required` | 400 | FINAL |
| `idempotency-key-invalid` | 400 | FINAL |
| `idempotency-key-reused` | 409 | FINAL |
| `malformed-request-body` | 400 | FINAL |
| `internal-error` | 500 | RETRYABLE |
| `malformed-json` | 400 | FINAL |
| `malformed-window-start` | 400 | FINAL |
| `malformed-window-end` | 400 | FINAL |
| `missing-location` | 400 | FINAL |
| `process-capacity-not-found` | 404 | RETRYABLE |
| `unknown-query-parameter` | 400 | FINAL |
| `invalid-limit` | 400 | FINAL |
| `invalid-cursor` | 400 | FINAL |
| `missing-path-id` | 400 | FINAL |
| `malformed-tasks-per-order` | 400 | FINAL |
| `malformed-units-per-order` | 400 | FINAL |
| `malformed-packages-per-order` | 400 | FINAL |
| `invalid-report-range` | 400 | FINAL |
| `report-store-error` | 500 | RETRYABLE |
| `dependency-unavailable` | 503 | RETRYABLE |
| `resource-not-found` | 404 | FINAL |
| `malformed-request-query` | 400 | FINAL |
| `malformed-shift-start` | 400 | FINAL |
| `missing-site-id` | 400 | FINAL |
| `missing-shift-id` | 400 | FINAL |
| `malformed-cpt` | 400 | FINAL |
| `missing-bucket-quantity` | 422 | FINAL |
| `missing-installed-stations` | 422 | FINAL |

### Why the non-FINAL entries are not FINAL (code-cited)

- `version-conflict` (409): the one-published-per-key unique violation
  (`capacity_plan_repository.go`, migration 0017) — the loser's writes are
  discarded (the statement aborts; the savepoint rollback removes the partial
  supersede) and the race can be won on retry.
- `process-path-not-found` (404): the path resolves flow-first
  (`usecases.FlowPathFinder`, ADR 0025) — an ACTIVE flow is event-fed state
  another request or consumer delivers later.
- `missing-step-capacity` (422): a covering `ProcessCapacity` window can be
  registered later (`GetProcessPathCapacity.stepInput` → `FindCovering`).
- `missing-assigned-demand` (422): the demand comes from the expected-demand
  read model (`resolveDemand`, ADR 0004) — order events land after the request.
- `workload-forecast-not-found` (404) / `missing-planned-heads` (422) /
  `labour-plan-not-found` (404): natural-key facts (site[/shift]/path) another
  request can PUT later.
- `empty-process-path-steps` (422): the ACTIVE flow's stages are event-fed
  producer state a newer flow version can change.
- `missing-conversion-factor` (422) / `unsupported-normalization-unit` (422):
  WHICH factor is missing, and which unit cannot be normalized, depends on the
  units of the ACTIVE flow's steps, the covering capacities and the declared
  station standards — state other requests own.
- `process-capacity-not-found` (404, the GET read): capacity is registrable
  later.
- 5xx entries (`internal-error`, `report-store-error`,
  `dependency-unavailable`): never stored by definition, classed RETRYABLE.

### No OUTCOME entry today

Verified per keyed route: `RegisterProcessCapacityConstraint` refuses at
`AddConstraint` before any `Save`; `CreateCapacityPlan` refuses before its
final save+outbox unit; `PublishCapacityPlan` raises all its mapped 4xx from
`Publish()`/`FindByID` before any `Save`, and `version-conflict` always aborts
the statement's transaction so its partial writes are discarded by the
savepoint rollback. No refusal provably commits state, so no entry is OUTCOME.
The class and the middleware path stay (the harness template covers them); the
first entry that provably commits must cite the use-case lines and carry the
OUTCOME proof test (state exists after the refusal; a replay records nothing
new).

## Consequences

- A RETRYABLE refusal is answered but never stored: the same key may be re-sent
  (automation only; callers driven by people must surface the refusal — a
  separate fleet decision) and executes again, and a different body under the
  released key is a new request, not `idempotency-key-reused`.
- FINAL and OUTCOME answers, 2xx answers, 5xx handling, replay bytes, slugs,
  titles and statuses are unchanged.
- The keyed-route surface is the three POSTs; the PUT declarations
  (`/station-standards`, `/inbound-labour-standards`, `/workload-forecasts`,
  `/labour-plans`) are naturally idempotent and take no key (ADR 0019), so
  their problem types are classified but never hit the keyed store.
- MCP and A2A write tools (`internal/adapters/inbound/mcp/tools.go`,
  `internal/adapters/inbound/a2a/server.go`) call the use cases directly with
  NO key store of their own: there is nothing to classify or release there.
- No deterministic-key caller of this service's keyed routes is known (the
  phase-0 scan found none; the REST clients are the console remote and people).
- Follow-ups recorded, not fixed here: expired `idempotency_keys` rows are
  never purged (retention is enforced at read/upsert only); the
  `missing-conversion-factor` mixed-cause slug could be split; a real
  unique-index loss through the joined REST transaction escalates to a 500
  (unstored and released, but the wrong status — see the PR body).

## Addendum — 2026-11-23: expired key rows are purged

The Consequences above recorded as a follow-up that "expired
`idempotency_keys` rows are never purged (retention is enforced at
read/upsert only)". That gap — the last one in the fleet — is now closed;
no Decision text above changes.

The retention is one constant, `ports.IdempotencyRetention` (24h), on all
three of its edges: the replay read (`Lock`'s
`created_at > now() - retention` age guard, unchanged), the upsert
(`Save` sets `created_at = now()`, unchanged) and the new purge.
`PurgeExpired` joined `ports.IdempotencyStore`, implemented by both the
Postgres store and the memory double: it deletes at most a bounded batch
of rows with `created_at <= now() - retention` — the exact complement of
the read guard — oldest first (`idx_idempotency_keys_created_at`),
`FOR UPDATE SKIP LOCKED`, the age test repeated on the DELETE itself, and
no advisory lock: a row a request holds is skipped, never waited for, and
a row `Save` just refreshed reads live and stays. The port contract is
`repocontract.RunIdempotencyPurge` (both implementations run it).
`cmd/api/retention.go` runs the purge as an always-on worker under the
same supervision as the consumers and the outbox relay: at start, then
hourly, batches of 1000, a short batch ends the pass; a failed pass is
logged and retried at the next tick; a dead worker exits the process.

No replay class flips: past the window the key is free, so a retry under
an expired key is a first execution (the age guard pinned through the
real middleware), and the purge only stops the table from growing — the
fleet pattern (order-sourcing #84, last-mile-delivery #86,
network-inventory-planning #142).
