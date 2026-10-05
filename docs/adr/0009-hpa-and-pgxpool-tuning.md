# ADR 0009: Horizontal Pod Autoscaling and pgxpool connection-count tuning (adoption)

## Status

Accepted (2026-10-05). Records decisions already in effect with no ADR,
found by the 2026-10-04 ADR-conformance audit.

## Context

`charts/warehouse-planning/templates/hpa.yaml` autoscales the `api`,
`analytics-projector`, `analytics-reports` and `frontend` Deployments on CPU
utilization, each gated independently behind its own
`autoscaling.<component>.enabled` flag (default `false`, so a default render
is byte-identical to a chart without this file). `internal/adapters/outbound/
postgres/pool.go` caps each process's own `pgxpool.Pool` at a fixed
`MaxConns`. Neither decision had a written rationale.

## Decision

1. **HPA is per-component and off by default.** Four independent
   `HorizontalPodAutoscaler` resources (`api`, `analytics-projector`,
   `analytics-reports`, `frontend`), each behind its own
   `autoscaling.<component>.enabled` (default `false`). The
   `analytics-projector`/`analytics-reports` HPAs are additionally gated on
   `analytics.enabled` -- they cannot exist without their Deployment.
   `scaleTargetRef` is CPU `Resource` utilization only (`autoscaling/v2`);
   no custom-metrics scaler is wired today.
2. **`MaxConns = 10` per process (`internal/adapters/outbound/postgres/
   pool.go`)**, a `pgxpool.Config` ceiling applied uniformly to `cmd/api`,
   `cmd/mcp` and, when it opens a pool, `cmd/planning-projector`. This bounds
   the service's worst-case connection footprint on the shared OLTP Postgres
   instance as HPA scales replica count: `replicas * MaxConns` is the number
   a database-capacity plan must budget for, which is why the ceiling is a
   single named constant rather than left to the driver's own default.
3. **HPA and the connection ceiling are deliberately coupled, not
   independent tuning knobs**: raising `autoscaling.api.maxReplicas` without
   also reviewing `MaxConns` (or the database's own `max_connections`) can
   exhaust the shared Postgres instance under a scale-out event. Any future
   change to either value should consider the other.

## Consequences

- A reviewer changing `maxReplicas` or `MaxConns` now has a place to record
  why, and a reminder that the two are coupled.
- No vertical (memory/custom-metric) autoscaling exists yet; CPU utilization
  is the only signal.

## Alternatives considered and rejected

- **One shared HPA resource covering every Deployment**: components scale on
  different load shapes (the OLTP `api`, the Kafka-bound `analytics-projector`,
  the read-only `analytics-reports`); a single HPA would either over- or
  under-scale at least one of them.
- **No connection ceiling (driver default)**: pgxpool's default is
  effectively unbounded per process, which combined with HPA scale-out could
  silently exhaust the database's `max_connections` under load -- the
  failure this ADR's §2 decision prevents.
