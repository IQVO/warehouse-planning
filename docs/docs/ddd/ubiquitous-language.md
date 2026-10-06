---
id: ubiquitous-language
title: Ubiquitous language
sidebar_label: Ubiquitous language
sidebar_position: 1
---

# Ubiquitous language

Use these exact names in code, API and conversation. The repository's
`.claude/rules/domain-model.md` is the agent-facing source; every term below
maps to a code identifier. Terms whose code name differs from the spoken name
are flagged in the last column.

| Term | Meaning | Code identifier | Name differs? |
| --- | --- | --- | --- |
| **ProcessCapacity** | The usable throughput of one warehouse process at one site for one time window; the minimum across its registered constraints. | `processcapacity.ProcessCapacity` | no |
| **CapacityConstraint** | One named limiting factor (type + rate) attached to a ProcessCapacity: `LABOR`, `LOCATION`, `EQUIPMENT`, `STATION`, `CONVEYOR`, `BUFFER`, `REPLENISHMENT`. | `processcapacity.ConstraintType` + `CapacityRate`, paired in `ConstraintEntry` | **yes**: no `CapacityConstraint` type exists; the pair is `ConstraintEntry` |
| **Binding constraint** | The constraint type producing the minimum (of an aggregate, a step or the bottleneck step). | `EffectiveRate()` second result; `StepResult.Binding`; `CapacityPlan.BottleneckConstraint()`; JSON `binding_constraint` / `bottleneck_constraint` | **yes**: `Binding` / `BottleneckConstraint` |
| **CapacityRate** | A quantity + native unit + period, for example `4000 UNIT / HOUR`. Never compared across units without a WorkloadProfile. | `processcapacity.CapacityRate` | no |
| **Native unit** | The unit of every constraint on one ProcessCapacity: `UNIT`, `LINE`, `ORDER` or `PACKAGE`. | `processcapacity.CapacityUnit` (`UnitUnit`, `UnitLine`, `UnitOrder`, `UnitPackage`) | **yes**: `CapacityUnit` |
| **CapacityWindow** | The `[start, end)` period a capacity is valid for. A capacity with no window is incomplete by definition. | `processcapacity.CapacityWindow` | no |
| **Covers** | Window `C` covers planning window `W` when `C.start <= W.start` and `C.end >= W.end`; a window covers itself (ADR 0003). | `CapacityWindow.Covers`; `ProcessCapacityRepository.FindCovering` | no |
| **WorkloadProfile** | Per-warehouse conversion factors (units per order, packages per order) used to normalize native rates into ORDER. Supplied on each request. | `processcapacity.WorkloadProfile`, `NormalizeToOrderRate`; JSON `units_per_order`, `packages_per_order` | no |
| **ProcessType** | A warehouse process step such as `PICK`, `REBIN`, `PACK`. | `processcapacity.ProcessType` and `processpath.ProcessType` (two string types, to avoid an import cycle) | no |
| **ProcessPath** | An ordered, non-empty sequence of process types a workload flows through. Locally owned and operator-declared (ADR 0001 Addendum). | `processpath.ProcessPath` | no |
| **ProcessPathCapacity** | The normalized end-to-end throughput of a ProcessPath: the minimum of its steps, plus the bottleneck step and its binding constraint. | `processcapacity.PathCapacityResult` from `ComposeProcessPathCapacity` | **yes**: `PathCapacityResult` |
| **Step composition** | A step's candidates (covering constraints, newest window start per type, plus the derived STATION constraint) normalized to ORDER; the minimum wins. | `processcapacity.ComposeStepCapacity`, `StepInput`, `StepResult` | no |
| **StationStandard** | Operator-declared throughput of ONE station of a process at a site, in `UNIT`, `PACKAGE` or `ORDER`. | `processcapacity.StationStandard` | no |
| **Station count** | How many work-center stations `facility-layout` tallied for an activity across a site's zones. Has no throughput of its own. | `StorageTallyReader.StationCount`; table `location_slot_tally` (`tally_type = 'STATION'`) | **yes**: a tally bucket, not a type |
| **Storage positions** | Storage slots per zone and location type; a read model, not a throughput. | `tally.Bucket`; `GetStorageCapacity`; JSON `storage_positions` | **yes**: `Bucket` |
| **Site / location** | A planning `location` is a site (building) code such as `SIM1`, also the first dash-separated segment of that site's facility zone ids. | the `location` string on every type; `building_id` in the labor payload | **yes**: `building_id` upstream |
| **CapacityPlan** | Assigned demand for a site and window, compared with the ProcessPathCapacity; yields shortage and bottleneck. Lifecycle `DRAFT` then `PUBLISHED`. | `capacityplan.CapacityPlan`, `StatusDraft`, `StatusPublished` | no |
| **Site id** | The canonical site a plan is scoped to: a facility-layout Site `site_code`. Stated on create, never inferred from warehouse or location. | `CapacityPlan.SiteID()`; JSON `site_id` | no |
| **Assigned demand** | The orders a plan must serve in its window, stated or defaulted from expected demand. | `CapacityPlan.AssignedDemand()`; JSON `assigned_demand` | no |
| **DemandSource** | Where the assigned demand came from: `request` or `orders`. | `capacityplan.DemandSource` (`DemandSourceRequest`, `DemandSourceOrders`) | no |
| **Capacity over window** | Path capacity (ORDER per hour) times the window's hours. | `CapacityPlan.CapacityOverWindow()`; JSON `capacity_over_window` | no |
| **Shortage** | `max(0, assigned demand - capacity over window)`, in orders. Equal is not a shortage. | `CapacityPlan.Shortage()` | no |
| **Bottleneck** | The process-path step limiting end-to-end flow, and the constraint type binding it. | `PathCapacityResult.BottleneckStep`; `CapacityPlan.BottleneckStep()`; event `BottleneckDetected` | no |
| **Expected demand** | Orders order-management has promised at a site, counted by promise cutoff in `[start, end)`. Zero orders means *no data*. | `demand.Order`, `demand.Summary`, `GetExpectedDemand`; table `order_demand` | no |
| **Promise cutoff** | The instant by which an order is promised; decides which window it counts in. | `demand.Order.PromiseAt()`; upstream `promise_date` | **yes**: `promise_date` upstream, `promise_at` here |
| **Released lines** | Lines released by an order's latest allocation pass. NOT units. | `demand.Order.ReleasedLines()`; JSON `released_lines` | no |

## Vocabulary only (not implemented)

`ProcessCapacityRegistered` (a native constraint was registered) and
`ProcessCapacityChanged` (the effective rate changed) exist as domain-model
vocabulary. Nothing raises or publishes them.
