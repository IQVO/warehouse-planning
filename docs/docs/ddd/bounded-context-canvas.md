---
id: bounded-context-canvas
title: Bounded context canvas
sidebar_label: Bounded context canvas
sidebar_position: 12
---

# Bounded context canvas

Following the ddd-crew [Bounded Context Canvas v5](https://github.com/ddd-crew/bounded-context-canvas).
Every message row below maps to a route in `NewRouter`
(`internal/adapters/inbound/http/process_capacity_handler.go`), a tool in
`internal/adapters/inbound/mcp/tools.go`, or a topic and CloudEvents type in
`internal/adapters/inbound/kafka/*_consumer.go` /
`internal/adapters/outbound/kafka/encoder.go`.

## Name

**Warehouse Planning** (`warehouse-planning`, Go module
`github.com/claudioed/warehouse-planning`, GitHub `IQVO/warehouse-planning`).

## Purpose

Answer *can this warehouse process the demand assigned to it, given its
labor, location, equipment, station, conveyor and buffer constraints?* It
holds the usable throughput of each process at each site and window
(`ProcessCapacity`), composes it at read time into a normalized end-to-end
path capacity, compares it with the demand assigned to a window in a
`CapacityPlan`, and announces shortages and bottlenecks as events. It does not
own labor scheduling, storage slotting, stock, or path capability and
eligibility authoring (ADR 0001, Consequences).

## Strategic Classification

| Axis | Verdict | Evidence |
| --- | --- | --- |
| Domain | **Core** | ADR 0001; see the [core domain chart](/docs/ddd/core-domain-chart) |
| Business model | **Risk reduction / engagement creator**: it does not earn revenue itself; it protects promised delivery by detecting a capacity shortage before the window starts | `CapacityShortageDetected` is consumed by `order-management` |
| Evolution | **Custom-built** (recently out of genesis) | Created 2026-10-03, core rules changed by ADRs 0002, 0003 and 0004 |

## Domain Roles

| Role | Applies? | Notes |
| --- | --- | --- |
| Analysis context | **Yes** (primary) | Composes upstream facts into capacity, shortage and bottleneck; never executes work. |
| Specification context | **Yes** | Operators declare process paths, station standards and constraints that the computation uses. |
| Gateway / Open Host Service | **Yes** | REST, MCP and the `warehouse.warehouse-planning.events` Published Language. |
| Execution context | No | It never dispatches, routes or assigns work. |
| Analytics / reporting | **Yes**, inside the context | `cmd/planning-projector` and `cmd/planning-reports` over a separate analytical database (ADR 0005). |

## Inbound Communication

| Collaborator | Message | Type | Channel | Relationship |
| --- | --- | --- | --- | --- |
| `workforce-management` | `ShiftPlanCommitted` (one message per PathPlan line) | Event | Kafka `warehouse.workforce.events`, `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | Upstream OHS + PL; this side is an ACL (`labor_capacity_consumer.go`) |
| `facility-layout` | `LocationSlotRegistered` | Event | Kafka `warehouse.facility.events`, `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` | Upstream OHS + PL; ACL (`storage_capacity_consumer.go`) |
| `facility-layout` | `LocationSlotDecommissioned` | Event | Kafka `warehouse.facility.events`, `com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned` | Upstream OHS + PL; ACL |
| `order-management` | `OrderAllocated` | Event | Kafka `warehouse.order-management.events`, `com.warehouse.wes.order-management.order.OrderAllocated` | Upstream OHS + PL; ACL (`order_demand_consumer.go`, opt-in) |
| `order-management` | `OrderPartiallyAllocated` | Event | Kafka `warehouse.order-management.events`, `com.warehouse.wes.order-management.order.OrderPartiallyAllocated` | Upstream OHS + PL; ACL (opt-in) |
| Operator / `warehouse-console` remote | Register process capacity constraint | Command | REST `POST /process-capacities`; MCP `register_process_capacity_constraint` | This context is the OHS |
| Operator / remote | Get effective process capacity | Query | REST `GET /process-capacities`; MCP `get_effective_process_capacity` | OHS |
| Operator / remote | Register process path | Command | REST `POST /process-paths`; MCP `register_process_path` | OHS |
| `warehouse-console` remote | List process paths | Query | REST `GET /process-paths` | OHS |
| Operator / remote / `warehouse-ops-agent` | Get process path capacity | Query | REST `GET /process-paths/{id}/capacity`; MCP `get_process_path_capacity` | OHS (the agent is a Customer of the MCP tool) |
| Operator / remote | Create capacity plan | Command | REST `POST /capacity-plans`; MCP `create_capacity_plan` | OHS |
| Operator / remote | Publish capacity plan | Command | REST `POST /capacity-plans/{id}/publish`; MCP `publish_capacity_plan` | OHS |
| Operator / remote / `warehouse-ops-agent` | Get capacity plan | Query | REST `GET /capacity-plans/{id}`; MCP `get_capacity_plan` | OHS |
| `warehouse-console` remote | List capacity plans | Query | REST `GET /capacity-plans` | OHS |
| Operator / remote | Declare station standard | Command | REST `PUT /station-standards/{location}/{process_type}`; MCP `declare_station_standard` | OHS |
| Operator / remote / `warehouse-ops-agent` | List station standards | Query | REST `GET /station-standards`; MCP `list_station_standards` | OHS |
| Operator / remote / `warehouse-ops-agent` | Get storage capacity | Query | REST `GET /storage-capacity`; MCP `get_storage_capacity` | OHS |
| Operator / remote | Get expected demand | Query | REST `GET /demand`; MCP `get_expected_demand` | OHS |
| Analyst / dashboard | Bottleneck frequency, shortage trend, plan throughput, freshness | Query | REST `GET /reports/bottleneck-frequency`, `/reports/shortage-trend`, `/reports/plan-throughput`, `/reports/freshness` on `cmd/planning-reports` | OHS (read-only) |

No REST or MCP endpoint is authenticated (fleet-wide revert of 2026-09-11).

## Outbound Communication

The context makes **no** synchronous call to any sibling (ADR 0001). Everything
outbound leaves through the transactional outbox and the relay in `cmd/api`.

| Collaborator | Message | Type | Channel | Relationship |
| --- | --- | --- | --- | --- |
| `order-management` | `CapacityPlanCreated` | Event | Kafka `warehouse.warehouse-planning.events`, `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated` | This context upstream, OHS + PL; `order-management` downstream with an ACL (its `planned_capacity_consumer.go`, opt-in) |
| `order-management` | `CapacityPlanPublished` | Event | same topic, `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished` | OHS + PL |
| `order-management` | `CapacityShortageDetected` | Event | same topic, `com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected` | OHS + PL |
| none today (`order-management` ignores it) | `BottleneckDetected` | Event | same topic, `com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected` | OHS + PL |
| own `cmd/planning-projector` | the same four types | Event | Kafka `warehouse.warehouse-planning.analytics` (dataschema `...:analytics:<EventName>:v1`) | Internal to this context (ADR 0005) |
| operators (dead-letter triage) | a message that failed 5 times | Event (copy) | Kafka `warehouse.workforce.events.dlq`, `warehouse.facility.events.dlq`, `warehouse.order-management.events.dlq`, `warehouse.warehouse-planning.analytics.dlq` | Internal operational channel |

## Ubiquitous Language

Full glossary with code identifiers: [Ubiquitous language](/docs/ddd/ubiquitous-language).
Top terms: **ProcessCapacity**, **CapacityConstraint**, **CapacityRate**,
**CapacityWindow** (and *covers*), **WorkloadProfile**, **ProcessPath**,
**ProcessPathCapacity**, **StationStandard**, **Station count**, **Site /
location**, **CapacityPlan**, **Shortage**, **Bottleneck**, **Expected
demand**, **DemandSource**.

## Business Decisions

1. A capacity number always carries a window, and a registered window applies
   to a planning window only when it **covers** it; per constraint type the
   newest window start wins (ADR 0003).
2. Station capacity is `count x StationStandard`, composed at read time and
   never stored; stations without a standard give a warning, never an invented
   throughput (ADR 0002).
3. Every candidate rate is normalized to ORDER per hour through the
   `WorkloadProfile` before it is compared; `LINE` cannot be normalized and is
   rejected (`ErrUnsupportedNormalizationUnit`).
4. A `ProcessCapacity` holds one native unit (`ErrUnitMismatch`).
5. `shortage = max(0, demand - capacity over window)`; demand equal to capacity
   is not a shortage, and the plan keeps the requested window.
6. A plan is published at most once (`ErrAlreadyPublished`, `409`); shortage and
   bottleneck events are raised only at publish and only when `shortage > 0`.
7. An omitted `assigned_demand` defaults to the expected orders; zero orders is
   *no data* and rejects the plan (`ErrMissingAssignedDemand`), never a
   zero-demand plan (ADR 0004).
8. No live cross-context lookup, ever; stock from `inventory-storage` is never
   read (ADR 0001).

## Assumptions

- The labor window is `[event time, event time + planned_hours)`: no upstream
  field carries a shift start (ADR 0001 Addendum).
- `planned_rate` is in units per hour (`laborRateUnit = UnitUnit`), a
  documented default.
- Site = building id = the first dash-separated segment of a facility zone id.
- Every order is attributed to one configured site (`DEMAND_SITE_ID`) because
  order-management events carry none (ADR 0004).
- The WorkloadProfile factors are supplied on each request (no persistence).

## Verification Metrics

- Golden exact-JSON tests per published type on both topics
  (`encoder_test.go`, `analytics_encoder_test.go`).
- `make mutation-fast` (gremlins) is blocking in CI on `processcapacity`,
  `capacityplan` and `processpath`; coverage gate 90% on domain + application.
- Outbox atomicity and redelivery proven with testcontainers Postgres and
  Kafka (`capacity_plan_outbox_integration_test.go`,
  `relay_integration_test.go`, `atomic_consumers_integration_test.go`).
- Business: the Tier-2 counter `warehouse_planning.capacity_plans.created`
  with an outcome attribute (ADR 0011); the `GET /reports/freshness` lag of the
  analytical projection.

## Open Questions

- When will `workforce-management` publish a real shift start and an explicit
  unit for `planned_rate`?
- Should the `WorkloadProfile` be persisted per warehouse instead of travelling
  on every request?
- How should demand be attributed to sites once order-management events carry
  a fulfillment site, and should cancellations be netted?
- Observed-capacity feedback from `fulfillment-execution` / `wes-work-planning`
  and demand from `network-fulfillment` are anticipated by ADR 0001 but have no
  code yet.
- `ProcessCapacityRegistered` / `ProcessCapacityChanged` are vocabulary only:
  should a capacity change ever be published?
