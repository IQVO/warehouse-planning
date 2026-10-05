# ADR 0010: Database migrations run over a direct Postgres connection

## Status

Accepted (2026-10-05). Records a decision already in effect with no ADR,
found by the 2026-10-04 ADR-conformance audit.

## Context

`DATABASE_URL`, the runtime pgxpool DSN, points at PgBouncer in transaction
pooling mode in the kind cluster (and any environment that fronts Postgres
with a pooler for connection-count reasons -- see ADR 0009). PgBouncer's
transaction pooling mode cannot honour `golang-migrate`'s session-scoped
`pg_advisory_lock`, which it uses to make concurrent migration attempts
(e.g. `cmd/api` and `cmd/mcp` booting at once) safe.

## Decision

`MIGRATIONS_DATABASE_URL` is a separate, optional env var holding a DIRECT
Postgres DSN (bypassing PgBouncer). `migrationsDatabaseURL` (`cmd/api/
main.go`, mirrored in `cmd/mcp/main.go` and `cmd/planning-projector`) returns
it when set, falling back to `DATABASE_URL` itself when unset -- so an
environment with no pooler split (local dev, CI, a single-Postgres kind
deployment) behaves exactly as if this variable did not exist. ONLY the
golang-migrate boot step (`postgres.RunMigrations`) ever uses this DSN; the
runtime `pgxpool.Pool` that serves requests always dials `DATABASE_URL`.

Both `cmd/api` and `cmd/mcp` run the idempotent migration step on their own
boot (not just one of them): golang-migrate's advisory lock makes their
concurrent attempts safe, so there is no "only the API migrates" ordering
requirement between the two deployables (see `docs/adr/0008-mcp-server-adoption.md`
§5).

## Consequences

- `warehouse-infra` must provision `MIGRATIONS_DATABASE_URL` as a direct DSN
  wherever `DATABASE_URL` points at a transaction-pooling proxy; where it
  does not, this service still boots correctly (the fallback).
- A migration added without testing against a pooled `DATABASE_URL` can look
  correct locally (no pooler) and then hang or fail the advisory lock in the
  kind cluster -- this is the reason the split exists, not a hypothetical.

## Alternatives considered and rejected

- **Session pooling mode on PgBouncer instead of transaction mode**: solves
  the advisory-lock problem but reintroduces the connection-count pressure
  transaction pooling exists to relieve (ADR 0009) for every OTHER query
  this service makes, not just migrations.
- **Run migrations from a separate one-shot Job/init container with its own
  direct DSN, never from the service binaries**: an extra deployable and
  ordering dependency for a problem the existing bounded retry
  (`internal/bootretry`) already makes safe to run inline, idempotently, from
  either binary.
