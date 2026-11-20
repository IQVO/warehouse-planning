---
id: context-map
title: Context map
sidebar_label: Context map
sidebar_position: 13
---

# Context map

This context's slice of the fleet map, following ddd-crew
[Context Mapping](https://github.com/ddd-crew/context-mapping). Every edge is
labelled `U` (upstream) / `D` (downstream) with the pattern on each side and
the technology. Solid edges are live in code, dotted edges are wired but opt-in
on the sibling's side or partly unused, and the grey nodes are deliberately absent relationships.

```mermaid
flowchart LR
  WP(("warehouse-planning<br/>Core"))

  WFM["workforce-management<br/>Supporting<br/>as labour supply"]
  WFMD["workforce-management<br/>Supporting<br/>as requirement consumer"]
  FL["facility-layout<br/>Generic"]
  OMU["order-management<br/>Generic/Supporting<br/>as demand source"]
  OMD["order-management<br/>Generic/Supporting<br/>as plan consumer"]
  DSC["dock-scheduling<br/>Supporting<br/>as inbound demand source"]
  FEX["fulfillment-execution<br/>Core<br/>as observed capacity source"]
  LPF["labor-performance<br/>Supporting<br/>as labour standard source"]
  OPS["warehouse-ops-agent<br/>Supporting"]
  CON["warehouse-console<br/>frontend shell"]
  PPM["process-path-management<br/>Generic"]
  INV["inventory-storage<br/>Core"]
  PM["product-master<br/>Supporting"]
  NIP["network-inventory-planning<br/>Core<br/>as plan consumer"]
  LATER["wes-work-planning Core<br/>network-fulfillment Supporting"]

  WFM -- "U: OHS+PL / D: ACL<br/>Kafka ShiftPlanCommitted (LABOR, committed heads)" --> WP
  FL -- "U: OHS+PL / D: ACL<br/>Kafka LocationSlotRegistered, LocationSlotDecommissioned" --> WP
  OMU -. "U: OHS+PL / D: ACL<br/>Kafka OrderAllocated, OrderPartiallyAllocated" .-> WP
  DSC -- "U: OHS+PL / D: ACL<br/>Kafka DockAppointmentBooked, Rescheduled, Cancelled, NoShow (ADR 0017, 0020)" --> WP
  FEX -- "U: OHS+PL / D: ACL<br/>Kafka TaskCompleted (ADR 0017)" --> WP
  LPF -- "U: OHS+PL / D: ACL<br/>Kafka LaborStandardDefined, Revised (ADR 0022)" --> WP
  WP -. "U: OHS+PL / D: ACL, opt-in<br/>Kafka CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected" .-> OMD
  WP -- "U: OHS+PL / D: ACL<br/>Kafka CapacityPlanPublished (with site_id, ADR 0012)" --> NIP
  WP -- "U: OHS+PL / D: ACL<br/>Kafka LabourPlanCommitted (ADR 0023)" --> WFMD
  WP -- "U: OHS / D: Customer, ACL<br/>MCP get_process_path_capacity, 3 more read tools wired" --> OPS
  WP -- "U: OHS / D: hosts the remote<br/>REST via capacity_mfe" --> CON
  PPM -- "U: OHS+PL / D: ACL, always on<br/>Kafka ProcessFlowDefined, Revised, Retired (ADR 0025)" --> WP
  INV ~~~ WP
  PM ~~~ WP
  LATER ~~~ WP

  classDef absent fill:#eeeeee,stroke:#999999,color:#555555,stroke-dasharray: 4 4
  class INV,PM,LATER absent
```

Source: `internal/adapters/inbound/kafka/process_flow_consumer.go`,
`labor_capacity_consumer.go`,
`storage_capacity_consumer.go`, `order_demand_consumer.go`,
`internal/adapters/outbound/kafka/encoder.go`, `cmd/api/main.go`,
`cmd/api/runtime.go`, `internal/adapters/inbound/mcp/tools.go`, `web/src/api.ts`,
ADR 0001 and its Addendum, ADR 0004; on the sibling side
`order-management` `internal/adapters/inbound/kafka/planned_capacity_consumer.go`,
`network-inventory-planning` `internal/adapters/inbound/kafka/consumers.go`,
`warehouse-ops-agent` `internal/adapters/outbound/mcpclient/warehouse_planning.go`
and `warehouse-console` `src/App.tsx` (all on `develop`).
Omits: this context's own analytics topic and projector (internal, not a
context relationship), the dead-letter topics, and the Kafka broker, Kong and
Nginx infrastructure. `order-management` and `workforce-management` are drawn
twice only to keep the upstream and downstream edges readable; each is one
context. wes-work-planning has no runtime edge: it handed its charge forecast
and path plan to this context (ADR 0023, its ADR 0049) and keeps execution.

## Relationships

| # | Upstream | Downstream | Patterns (U / D) | Technology | Status | Evidence |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `workforce-management` | `warehouse-planning` | OHS + Published Language / Anti-Corruption Layer: the payload is hand-mirrored (`shiftPlanCommittedData`) and translated into a LABOR `CapacityConstraint` with an assumed UNIT/HOUR unit and a derived window, and, for a line with a `shift_id`, into the committed heads of `(site, shift, path)` that the labour plan drift compares (ADR 0023 decision 3) | Kafka `warehouse.workforce.events`, `com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted` | **Live** (group `LABOR_CAPACITY_CONSUMER_GROUP`) | `internal/adapters/inbound/kafka/labor_capacity_consumer.go`, `internal/application/usecases/labour_plan_drift.go` |
| 2 | `facility-layout` | `warehouse-planning` | OHS + PL / ACL: slots are folded into a tally, never into a capacity (ADR 0002) | Kafka `warehouse.facility.events`, `com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered` and `...LocationSlotDecommissioned` | **Live** (group `STORAGE_CAPACITY_CONSUMER_GROUP`) | `internal/adapters/inbound/kafka/storage_capacity_consumer.go` |
| 3 | `order-management` | `warehouse-planning` | OHS + PL / ACL: an order becomes a `demand.Order` attributed to its event's `site_id`, else to the fallback `DEMAND_SITE_ID` (ADR 0021) | Kafka `warehouse.order-management.events`, `com.warehouse.wes.order-management.order.OrderAllocated` and `...OrderPartiallyAllocated` | **Wired, always on** (ADR 0004; ADR 0016 removed the off switch): `DEMAND_CONSUMER_GROUP` and `DEMAND_SITE_ID` are required at boot | `internal/adapters/inbound/kafka/order_demand_consumer.go`, `cmd/api/runtime.go` |
| 4 | `warehouse-planning` | `order-management` | OHS + PL / ACL (local read model, per order-management's own context map) | Kafka `warehouse.warehouse-planning.events`, `com.warehouse.wes.warehouse-planning.capacityplan.*` | **Wired, opt-in** on the consumer side (`PLANNED_CAPACITY_CONSUMER_GROUP` in order-management); it acts on Created, Published and ShortageDetected and ignores `BottleneckDetected` | this repo: `internal/adapters/outbound/kafka/encoder.go`; order-management: `planned_capacity_consumer.go` (its ADR 0031) |
| 5a | `dock-scheduling` | `warehouse-planning` | OHS + PL / ACL: an INBOUND appointment becomes an `inbound.Appointment` (window start, ASN count) attributed to the `site_id` of its events; labour comes only from the operator-declared inbound standard | Kafka `warehouse.dock-scheduling.events`, `com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentBooked`, `...DockAppointmentRescheduled`, `...DockAppointmentCancelled` and `...DockAppointmentNoShow` | **Live, always on** (ADR 0017, source switched by ADR 0020; it was inbound-receiving): `DOCK_APPOINTMENT_CONSUMER_GROUP` required at boot, the retired `INBOUND_DEMAND_CONSUMER_GROUP` refused | `internal/adapters/inbound/kafka/dock_appointment_consumer.go`, `cmd/api/runtime.go` |
| 5b | `fulfillment-execution` | `warehouse-planning` | OHS + PL / ACL: a completion becomes a minute bucket of `observed_capacity` (location, upper-cased `task_type`); advisory only, never feeds a plan back | Kafka `warehouse.fulfillment.events`, `com.warehouse.wes.fulfillment-execution.task.TaskCompleted` | **Live, always on** (ADR 0017): `OBSERVED_CAPACITY_CONSUMER_GROUP` required at boot | `internal/adapters/inbound/kafka/observed_capacity_consumer.go`, `cmd/api/runtime.go` |
| 5 | `warehouse-planning` | `warehouse-ops-agent` | OHS / Customer with an ACL (its MCP client adapter) | MCP Streamable HTTP, read tools only | **Live** for `get_process_path_capacity` (the agent's daily-brief capacity outlook); `get_capacity_plan`, `get_storage_capacity`, `list_station_standards` **wired but unused** (its ADR 0013) | `internal/adapters/inbound/mcp/tools.go`; ops-agent `internal/adapters/outbound/mcpclient/warehouse_planning.go` |
| 6 | `warehouse-planning` | `warehouse-console` | OHS; the console only hosts this context's own Module Federation remote `capacity_mfe`, which calls this service's REST API through Kong | REST (`/api/warehouse-planning`) | **Live** | `web/src/api.ts`, `web/vite.config.ts`; console `src/App.tsx` |
| 7 | `process-path-management` | `warehouse.process-path-management.events`: `ProcessFlowDefined`, `ProcessFlowRevised`, `ProcessFlowRetired` (each the FULL flow state with a `version`) | **Customer/Supplier with an anti-corruption layer**: the consumer translates a flow's stages into this context's ordered steps; a message applies only when its `version` is greater than the stored one | Kafka consumer `ProcessFlowConsumer` (`PROCESS_FLOW_CONSUMER_GROUP`, required; the `process_flows` copy) | **Live, always on** (ADR 0025 stage 1; stages 2-3 decided, not built: the local `POST /process-paths` retires once every local path has a flow) | `internal/adapters/inbound/kafka/process_flow_consumer.go`, `cmd/api/runtime.go` |
| 8 | `inventory-storage` | none | **Separate Ways**: stock is not capacity | none | **Deliberately absent** (ADR 0001) | no consumer, no client |
| 9 | `fulfillment-execution`, `wes-work-planning` (observed capacity), `network-fulfillment` (demand) | `warehouse-planning` | Published Language intended | Kafka (intended) | **Planned, not implemented** (ADR 0001 context map) | none |
| 10 | `product-master` | none | No relationship: SKU master data (handling classification, unit dimensions and weight) is not a capacity input; `product-master` publishes `warehouse.product-master.events` and calls no sibling | none | **Absent** | no consumer of `warehouse.product-master.events` and no client in this repo; `product-master` (its ADR 0001) has no outbound client and consumes nothing from this context |
| 11 | `warehouse-planning` | `network-inventory-planning` | OHS + PL / ACL on NIP's side (it folds each published plan into its own `PublishedCapacityPlan` read model and refuses to plan from a stale or missing one) | Kafka `warehouse.warehouse-planning.events`, `com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished`; NIP needs the additive canonical `site_id` this context adds in ADR 0012 | **Live** on NIP's side (its `CAPACITY_PLAN_CONSUMER_GROUP`); exercised by the e2e inter-warehouse-transfer scenario, where plans created through this service's REST API and relayed by its outbox are what let NIP approve a transfer | this context: `capacityplan.CapacityPlanPublished` and its `site_id` (ADR 0012); NIP: `internal/adapters/inbound/kafka/consumers.go` (capacity-plan consumer) |
| 12 | `labor-performance` | `warehouse-planning` | OHS + PL / ACL: each standard event becomes an immutable labour-standard fact (one per `standard_id`); the standard in force for a plan's site, step and window is resolved at read time; only labor-performance's INTEGRATION topic is read | Kafka `warehouse.labor-performance.events`, `com.warehouse.wes.labor-performance.standard.LaborStandardDefined` and `...LaborStandardRevised` | **Live, always on** (ADR 0022; producer side labor-performance ADR 0038): `LABOR_STANDARD_CONSUMER_GROUP` required at boot | `internal/adapters/inbound/kafka/labor_standard_consumer.go`, `cmd/api/runtime.go` |
| 13 | `warehouse-planning` | `workforce-management` | OHS + PL / ACL on workforce-management's side (a `labour_requirements` copy, applied only when `version` is greater; it proposes heads from it and shows them as `requiredHeads`, its ADR 0043). Requirement here, supply there: workforce-management does not refuse a commitment below the requirement, this context surfaces the difference as `LabourPlanDriftDetected` | Kafka `warehouse.warehouse-planning.events`, `com.warehouse.wes.warehouse-planning.labourplan.LabourPlanCommitted` | **Live** on workforce-management's side (group `workforce-management-labour-plans`, env `LABOUR_PLAN_CONSUMER_GROUP` there) | this context: `internal/domain/labourplan/events.go`, `internal/application/usecases/labour_plan.go`; workforce-management: `cmd/workforce/labour_plan.go` |
| 14 | `wes-work-planning` | none | No runtime relationship: its `ChargeForecast` and `PathPlan` were handed over (this context's ADR 0023, its ADR 0049; its `/paths/{pathId}/charge` and `/paths/{pathId}/plan` answer `410`). No importer: both facts are shift-scoped and sent again every shift | none | **Absent** (hand-over done) | `internal/domain/workloadforecast`, `internal/domain/labourplan` |

There is no Shared Kernel, no Partnership and no Conformist relationship: no
sibling Go package is imported (hard rule 5 in `CLAUDE.md`) and every upstream
payload is translated locally.
