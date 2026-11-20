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
| **ProcessPath** | An ordered, non-empty sequence of process types a workload flows through. Operator-declared locally (ADR 0001 Addendum) or resolved from a consumed `process-path-management` flow, which wins over a local path of the same id (`source: flow`, ADR 0025). | `processpath.ProcessPath` | no |
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
| **Bottleneck** | The process-path step limiting end-to-end flow, and the constraint type binding it. |
| **Process flow** | An ordered sequence of `path_id` + `process_type` stages owned by `process-path-management` (its ADR 0042); this context keeps an event-fed copy (`process_flows`, ADR 0025) and a flow's steps are its stages' `process_type` values in order. | `processflow.Flow`; table `process_flows` |
| **Path source** | Which of the two coexisting declarations a read resolved: `flow` (an ACTIVE flow won) or `local` (the local `POST /process-paths` declaration). Until ADR 0025 stage 2. | `ProcessPath.Source`; JSON `source` | `PathCapacityResult.BottleneckStep`; `CapacityPlan.BottleneckStep()`; event `BottleneckDetected` | no |
| **Expected demand** | Orders order-management has promised at a site, counted by promise cutoff in `[start, end)`. Zero orders means *no data*. | `demand.Order`, `demand.Summary`, `GetExpectedDemand`; table `order_demand` | no |
| **Promise cutoff** | The instant by which an order is promised; decides which window it counts in. | `demand.Order.PromiseAt()`; upstream `promise_date` | **yes**: `promise_date` upstream, `promise_at` here |
| **Released lines** | Lines released by an order's latest allocation pass. NOT units. | `demand.Order.ReleasedLines()`; JSON `released_lines` | no |

## ProcessCapacity events

| Term | Meaning | Where | Upstream name |
| --- | --- | --- | --- |
| **ProcessCapacityRegistered** | The aggregate's first (native) constraint was registered: it came into being. | `processcapacity.ProcessCapacityRegistered`; `ProcessCapacity.AddConstraint` | no |
| **ProcessCapacityChanged** | A registration moved the effective rate — its value, its binding constraint, or both. | `processcapacity.ProcessCapacityChanged`; `ProcessCapacity.AddConstraint` | no |
| **Planned capacity** | This context's capacity figure for a path and a window: the path capacity of a capacity plan (and, with ADR 0023, a shift's labour plan). Not wes-work-planning's live remaining admission capacity. | `CapacityPlan.PathCapacity`, `CapacityPlanPublished` | no |
| **Remaining admission capacity** | wes-work-planning's live figure: how much more work a release-fed path admits before its WIP limit, per CPT cutoff (`PathCapacityChanged`). Read by order promising, never computed here. | none here (wes-work-planning `WorkPool.RemainingCapacity()`) | no |

## Requirement side (ADR 0022, ADR 0023)

The terms of the labour *requirement*: how much labour the demand needs.
Labour *supply* (who is available, certified, assigned, and how many heads
are committed) belongs to `workforce-management`; the engineered standards
belong to `labor-performance`; execution (release, balancing) belongs to
`wes-work-planning`.

| Term | Meaning | Code identifier | Name differs? |
| --- | --- | --- | --- |
| **Labour standard** | labor-performance's engineered seconds for one task of a task type (`PICK`, `PACK`, `SLAM`), optionally per handling unit and per site, with an effective start. This context keeps a local copy of the facts and resolves the one in force at read time: a site-scoped row beats a fleet-wide one, then the latest `effective_from` at or before the window start, then the latest `as_of`. Only task-type-only rows (no handling unit) price a plan. | `laborstandard.Standard`, `laborstandard.Resolve`; table `labor_standards`; events `LaborStandardDefined` / `LaborStandardRevised` | **yes**: the code and the producer spell it `labor` |
| **Labour requirement** | Per step of a capacity plan's path: `tasks x standard_seconds / 3600` labour hours and those hours divided by the window hours as average heads. `tasks` is the released lines of the counted orders for `PICK` when the demand came from orders, else `assigned_demand x tasks_per_order[step]`. Unknown (never guessed) with reason `no-labor-standard` or `no-task-factor`. Computed at plan creation and stored with the plan; informational, on no event. | `capacityplan.LabourRequirement`, `RequireLabour`; JSON `labour_requirement` (`process_type`, `standard_seconds`, `standard_site_scoped`, `tasks`, `required_labour_hours`, `required_average_heads`, `known`, `reason`) | no |
| **Workload forecast** | The volume a process path is expected to receive at a site, as CPT buckets. One per `(site_id, path_id)`; a receipt replaces the buckets as a whole; at least one bucket. It was wes-work-planning's charge forecast. | `workloadforecast.Forecast`; table `workload_forecasts`; event `WorkloadForecastReceived` | **yes**: wes called it `ChargeForecast` |
| **Forecast bucket** | `{cpt, quantity}`: a non-negative whole number of units due at one critical pull time (RFC 3339). | `workloadforecast.Bucket`; JSON `buckets[].cpt`, `buckets[].quantity` | no |
| **Labour plan** | The heads a process path needs for one shift at one site, at a planned rate for a number of hours. One per `(site_id, shift_id, path_id)`. Invariants: `planned_heads <= installed_stations`, `hours > 0`, rate `> 0`, `rate x heads x hours` finite. It was wes-work-planning's path plan. | `labourplan.Plan`, `labourplan.Spec`; table `labour_plans`; event `LabourPlanCommitted` | **yes**: wes called it `PathPlan` |
| **Planned heads** | The labour plan's head count: the requirement this context publishes. | `Spec.PlannedHeads`; JSON `planned_heads` | no |
| **Heads source** | Where the planned heads came from: `request` (the caller sent them) or `forecast` (derived). | `labourplan.HeadsSource` (`HeadsFromRequest`, `HeadsFromForecast`); JSON `heads_source` | no |
| **Derived heads** | Planned heads computed when the caller omits them: `ceil(units with a cpt in [shift_start, shift_start + hours) / (rate x hours))`. Needs `shift_start` and a stored forecast with at least one bucket in that window; otherwise `422 missing-planned-heads`. Zero buckets in the shift is no data, never zero heads. | `labourplan.DeriveHeads`; `Forecast.QuantityDueIn`; `CommitLabourPlan.deriveHeads` | no |
| **Committed heads** | The heads workforce-management committed for the same `(site, shift, path)` in its `ShiftPlanCommitted` (its `planned_heads` field). Supply, not requirement; recorded, never used to change the plan. | `labourplan.Commitment.Heads`; table `labour_plan_commitments.committed_heads` | **yes**: the upstream field is `planned_heads` |
| **Labour plan drift** | `committed_heads - planned_heads` for one key: positive when workforce committed more heads than the plan needs. Raised as `LabourPlanDriftDetected` only when non-zero and different from the last drift recorded for the key; every computed drift is recorded (zero included). Never corrects either side. | `labourplan.Drift`, `CompareHeads`, `ShouldRaise`; column `last_drift_heads`; event `LabourPlanDriftDetected` (`drift_heads`) | no |
| **Station warning** | An advisory string on a labour plan answer when the facility-layout tally knows at least one but fewer stations of the activity named like the path (path id upper-cased) than `installed_stations`. Computed on every read, never stored; the number stays as sent. | `usecases.stationWarnings`; JSON `warnings` | no |
| **Process-path code** | The `path_id` of a forecast, a labour plan or a committed-heads row, in its canonical form: trimmed and upper case (`pick` and ` PICK ` are `PICK`). REST accepts any case; storage and published events carry the canonical form. Same rule as the LABOR constraint's `ProcessType`. | `workloadforecast.CanonicalPathID`, `labourplan.CanonicalPathID` | yes (`path_id`) |
| **Shift** | The shift a labour plan is for, as a caller-chosen `shift_id` (matched exactly as sent); `shift_start` and `hours` give its window `[shift_start, shift_start + hours)`. | `Spec.ShiftID`, `Spec.ShiftStart`, `labourplan.ShiftEnd` | no |
