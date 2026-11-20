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
| [0004](0004-demand-ingestion-from-order-management.md) | Demand is ingested from order-management's published events into a local read model; `assigned_demand` becomes optional | Accepted (2026-10-04). Additive. Amended by ADR 0016 (the consumer is always on) |
| [0005](0005-analytics-read-side.md) | Analytics read side: an analytics stream, a projector and read-only reports over a separate analytical database | Accepted (2026-10-04). Additive |
| [0006](0006-cloudevents-envelope-and-type-catalogue.md) | CloudEvents 1.0 is the only envelope, and this is the type catalogue | Accepted (2026-10-05). Records a decision already in effect |
| [0007](0007-outbox-and-resilient-consumers.md) | Transactional outbox and resilient, idempotent Kafka consumers (adoption) | Accepted (2026-10-05). Records decisions already in effect, plus the dead-letter fix for the three domain consumers. Amended by ADR 0013 and ADR 0016 (no `LogSink` at runtime) |
| [0008](0008-mcp-server-adoption.md) | MCP server adoption (unauthenticated, additive) | Accepted (2026-10-05). Records a decision already in effect. Amended by ADR 0016 (`DATABASE_URL` required) |
| [0009](0009-hpa-and-pgxpool-tuning.md) | Horizontal Pod Autoscaling and pgxpool connection-count tuning (adoption) | Accepted (2026-10-05). Records decisions already in effect |
| [0010](0010-migrations-over-direct-connection.md) | Database migrations run over a direct Postgres connection | Accepted (2026-10-05). Records a decision already in effect |
| [0011](0011-standard-metrics-adoption.md) | Standard metrics adoption (otelchi + a Tier-2 business counter) | Accepted (2026-10-05) |
| [0012](0012-canonical-site-id-on-capacity-plans.md) | Canonical site_id on CapacityPlan and CapacityPlanPublished | Accepted (2026-10-06) |
| [0013](0013-outbox-relay-attempt-cap.md) | Outbox relay attempt cap | Accepted (2026-10-13). Amends ADR 0007 §1's ordering guarantee |
| [0014](0014-trace-context-crosses-the-outbox.md) | The W3C trace context crosses the outbox to Kafka | Accepted (2026-10-13). Additive |
| [0015](0015-projector-telemetry-and-chart-otel-env.md) | Projector telemetry and the chart's OTel environment | Accepted (2026-10-13). Extends ADR 0011 |
| [0016](0016-postgres-and-kafka-are-the-only-runtime.md) | Postgres and Kafka are the only runtime | Accepted (2026-10-19). Amends ADR 0004 (demand consumer off switch), ADR 0007 (`LogSink` default) and ADR 0008 (in-memory `cmd/mcp`) |
| [0017](0017-inbound-labour-demand-and-observed-capacity-feedback.md) | Inbound labour demand and observed capacity feedback | Accepted (2026-10-20). Additive. Supersedes the "later phase" observed-capacity note of the ADR 0001 context map; reuses the window semantics of ADR 0003 and the read-model pattern of ADR 0004. The source of Part A (appointments) is superseded by ADR 0020 |
| [0018](0018-a2a-inbound-adapter.md) | A2A inbound adapter (third driving adapter): two curated skills, `path_capacity` (read) and `evaluate_capacity_plan` (write: a DRAFT plan), over the same use cases as REST and MCP; message-only, problems answered in-band with REST's status classification, delegation only (facts stay on Kafka via the outbox) | Accepted (2026-10-29). Additive. Mirrors [ADR 0008](0008-mcp-server-adoption.md) |
| [0019](0019-adopt-the-fleet-rest-api-guideline.md) | Adopt the fleet REST API guideline: `/publications` route, global `Idempotency-Key` on every POST (409 reuse, `Idempotency-Replayed`), keyset `{items,nextCursor}` collections | Accepted (2026-11-02). Breaking for REST clients |
| [0020](0020-inbound-demand-from-dock-scheduling.md) | Inbound demand is read from dock-scheduling's appointment events (Booked, Rescheduled, Cancelled, NoShow), attributed by `site_id` and ordered by `version`; the inbound-receiving source is refused at boot | Accepted (2026-11-05). Supersedes the source of ADR 0017 Part A (decisions 1, 3, 5 and the appointment half of 12 and 13); the labour standard, window rule and Part B stand. Companion of the fleet decision YD-0002 |
| [0021](0021-order-demand-and-labour-read-the-events-own-site.md) | Order demand reads the optional `site_id` of order-management's allocation events (its ADR 0050) and falls back to `DEMAND_SITE_ID`; the labour consumer reads `site_code` before the deprecated `building_id` | Accepted (2026-11-15). Additive. Amends ADR 0004 §4 (the site assumption) and the labour site attribution of the ADR 0001 Addendum |
| [0022](0022-labour-standards-from-labor-performance.md) | Labour standards are a local copy of labor-performance's `LaborStandardDefined`/`LaborStandardRevised` (its integration topic, its ADR 0038); a capacity plan states per step the labour its demand requires (tasks x standard seconds), unknown without a standard or task factor; the inbound labour and station standards stay declarative | Accepted (2026-11-18). Additive. Amends hard rule 5 (`CLAUDE.md`) |
| [0023](0023-workload-forecast-and-labour-plan-move-here.md) | The workload forecast and the shift labour plan move here from wes-work-planning (its ADR 0049), keyed by site, invariants unchanged; `LabourPlanCommitted` for workforce-management (its ADR 0043); plan-vs-commitment drift moves too; planned capacity vs remaining admission capacity | Accepted (2026-11-18). Additive here; wes-work-planning retires its routes on cutover |
| [0024](0024-forecast-demand-and-plan-scenarios.md) | `demand_source: forecast` on capacity plans; several drafts per window are scenarios; publishing supersedes the previously published plan (`CapacityPlanSuperseded`); side-by-side comparison read | Accepted (2026-11-18). Implemented in full (#151, #153) |
| [0025](0025-process-flows-from-process-path-management.md) | process-path-management's `ProcessFlow` (its ADR 0042) replaces locally declared process paths through a strangler; the Separate Ways edge of ADR 0001 becomes customer/supplier with an ACL | Accepted (2026-11-18). Supersedes the process-path-management part of the ADR 0001 Addendum on implementation |

All twenty-five are Accepted; none is Superseded as a whole (ADR 0017 is superseded in part by ADR 0020). Where a page in the
main documentation states a rule, it links the ADR that decided it.
