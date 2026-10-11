---
id: index
title: Architecture Decision Records
sidebar_label: ADR index
sidebar_position: 0
slug: /
---

# Architecture Decision Records

Every decision record of `warehouse-planning`, in number order. The
**Status** column is copied from each ADR's own `## Status` section, which is
the ground truth; ADR bodies are never edited after acceptance, so a later
decision amends or supersedes an earlier one by its own ADR.

| ADR | Title | Status (from the ADR body) |
| --- | --- | --- |
| [0001](0001-warehouse-planning-bounded-context.md) | warehouse-planning as a new Core bounded context | Accepted (2026-10-03). Amended (2026-10-03), see its Addendum; the Addendum's handling of `facility-layout` storage and station tallies as capacity constraints is superseded by ADR 0002 |
| [0002](0002-station-capacity-composition.md) | Station capacity is composed at read time; storage positions are a read model | Accepted (2026-10-04). Supersedes the storage and station part of the ADR 0001 Addendum; its decision 2 is refined by ADR 0003 |
| [0003](0003-window-coverage-semantics.md) | A registered capacity window applies when it COVERS the planning window | Accepted (2026-10-04). Refines ADR 0002 decision 2 and the exact-window statements of the ADR 0001 Addendum |
| [0004](0004-demand-ingestion-from-order-management.md) | Demand is ingested from order-management's published events into a local read model; `assigned_demand` becomes optional | Accepted (2026-10-04). Additive |
| [0005](0005-analytics-read-side.md) | Analytics read side: an analytics stream, a projector and read-only reports over a separate analytical database | Accepted (2026-10-04). Additive |
| [0006](0006-cloudevents-envelope-and-type-catalogue.md) | CloudEvents 1.0 is the only envelope, and this is the type catalogue | Accepted (2026-10-05). Records a decision already in effect |
| [0007](0007-outbox-and-resilient-consumers.md) | Transactional outbox and resilient, idempotent Kafka consumers (adoption) | Accepted (2026-10-05). Records decisions already in effect, plus the dead-letter fix for the three domain consumers |
| [0008](0008-mcp-server-adoption.md) | MCP server adoption (unauthenticated, additive) | Accepted (2026-10-05). Records a decision already in effect |
| [0009](0009-hpa-and-pgxpool-tuning.md) | Horizontal Pod Autoscaling and pgxpool connection-count tuning (adoption) | Accepted (2026-10-05). Records decisions already in effect |
| [0010](0010-migrations-over-direct-connection.md) | Database migrations run over a direct Postgres connection | Accepted (2026-10-05). Records a decision already in effect |
| [0011](0011-standard-metrics-adoption.md) | Standard metrics adoption (otelchi + a Tier-2 business counter) | Accepted (2026-10-05) |
| [0012](0012-canonical-site-id-on-capacity-plans.md) | Canonical site_id on CapacityPlan and CapacityPlanPublished | Accepted (2026-10-06) |

All twelve are Accepted; none is Superseded as a whole. Where a page in the
main documentation states a rule, it links the ADR that decided it.
