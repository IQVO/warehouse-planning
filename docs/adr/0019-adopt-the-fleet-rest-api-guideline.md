---
description: "ADR 0019: warehouse-planning adopts the fleet REST API guideline as a hard cutover: POST /capacity-plans/{id}/publish becomes /publications, every POST sits behind a global Idempotency-Key (409 idempotency-key-reused, Idempotency-Replayed), and every collection is a keyset page {items,nextCursor}."
---

# ADR 0019: Adopt the fleet REST API guideline

## Status

Accepted (2026-11-02). Breaking for REST clients; no CloudEvents type or
payload changes. Follows [ADR 0006](./0006-cloudevents-envelope-and-type-catalogue.md)
and [ADR 0007](./0007-outbox-and-resilient-consumers.md) (the outbox the key row
now commits with).

## Context

The fleet standardised its REST contract (noun routes, idempotent POST/PATCH,
one paging envelope) and enforces it with a Spectral ruleset (`.spectral.yaml`,
`.spectral/functions/restGuideline.js`). This service had one verb route, four
unkeyed POSTs and five unbounded or differently-shaped collections.

## Decision

Hard cutover, no aliases, no toggles.

1. **Route.** `POST /capacity-plans/{id}/publish` becomes
   `POST /capacity-plans/{id}/publications` (`operationId` unchanged, so the
   MCP and A2A mappings do not move). The old route answers 404/405
   (`TestRetiredRoutesAreGone`).
2. **Idempotency.** Every POST requires `Idempotency-Key` (1-128 printable
   ASCII). The key is global and looked up by key alone; the fingerprint is
   method + path + sha256(body). Missing key: 400 `idempotency-key-required`;
   malformed: 400 `idempotency-key-invalid`; same key with a different request:
   409 `idempotency-key-reused`; a replay carries `Idempotency-Replayed: true`.
   Migration `0011_idempotency_keys` adds the table; the key row is saved in the
   same `UnitOfWork` as the aggregate and its outbox rows. 4xx answers are
   stored and replayed, 5xx are not. PUT takes no key.
3. **Paging.** `GET /process-paths`, `/capacity-plans`, `/station-standards`
   and `/inbound-labour-standards` answer `{items, nextCursor}` with `limit`
   (default 100, max 500) and an opaque keyset `cursor`; problems `invalid-limit`,
   `invalid-cursor`, `unknown-query-parameter`. The former `capacity_plans`,
   `process_paths`, `standards` keys and the echoed `location` are gone.
4. **Exemptions** (`x-pagination-exempt: view`): `GET /process-capacities`
   (the single effective rate of one process/location/window),
   `GET /process-paths/{id}/capacity` (one computed breakdown) and
   `GET /storage-capacity` (one site's tally); each is a single computed view,
   not a collection.

## Consequences

- `capacity_mfe` (`web/`) sends a fresh key per POST and follows `nextCursor`.
- `cursor` and `idempotency_keys` types live in `internal/domain/pagecursor` and
  `internal/domain/idempotency` because `application/ports` is interfaces-only
  (arch test).
- Other callers (`e2e-tests`) update to the new route in their own PRs.
