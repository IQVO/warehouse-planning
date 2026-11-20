---
description: "ADR 0024: a capacity plan can take its demand from the workload forecast (demand_source forecast); several draft plans of one window are what-if scenarios, publishing one supersedes the plan published before it for the same site, path and window (CapacityPlanSuperseded), and a comparison read lines scenarios up side by side."
---

# ADR 0024: Forecast demand and what-if capacity plan scenarios

## Status

Accepted (2026-11-18). Third of the four requirement-side decisions (ADR 0022 to ADR 0025).
Extends [ADR 0004](0004-demand-ingestion-from-order-management.md) (demand sources) and the
plan lifecycle of [ADR 0001](0001-warehouse-planning-bounded-context.md). Builds on the
`WorkloadForecast` of [ADR 0023](0023-workload-forecast-and-labour-plan-move-here.md). Adds one
published type; existing types and fields are unchanged.

Implemented in full (2026-11-19): decisions 1, 2 and 4 in PR #151 (migration `0016`); decision 3 in
PR #153 (migration `0017`), after order-management consumed `CapacityPlanSuperseded` (its ADR
0052). Decision 5's check found network-inventory-planning keeping the newest published plan per
site, and order-management keeping every published plan, hence its ADR 0052. The same check also
found order-management matching plans by `location`; its ADR 0053 matches by the plan's `site_id`
when it has one (`CapacityPlanPublished` carries it, ADR 0012), else by `location`.

## Context

A plan's demand is either stated by the caller (`request`) or read from the orders
order-management has allocated (`orders`, ADR 0004). Allocated orders arrive hours before the
cutoff at best, so a plan for tomorrow, or for this afternoon's peak, has nothing to read: the
planner types a number. Labour-planning products plan from a workload forecast first and
replace it with released orders as they arrive.

A plan is also one-shot: create a DRAFT, publish it. Planners compare options before they
commit ("what if we open the second pack line", "what if volume comes in 15% higher").
Today that means creating drafts and reading them one by one, and nothing records which
published plan replaced which.

## Decision

1. **`demand_source: forecast`.** `POST /capacity-plans` (and the MCP `create_capacity_plan`
   tool) accept an optional `demand_source` of `request` (today's default when
   `assigned_demand` is given), `orders` (today's default when it is omitted) or `forecast`.
   With `forecast`, the assigned demand in ORDERS is the sum of the quantities of the
   `WorkloadForecast` of `(site_id, forecast_path_id)` whose bucket `cpt` falls in the plan
   window (`cpt >= start AND cpt < end`, the window rule of ADR 0003 and ADR 0004), divided by
   the request's `units_per_order`, rounded up. `forecast_path_id` is optional and defaults to
   the plan's `path_id`: a forecast is kept per process path the volume enters (a wes path such
   as `PICK` in the simulation), which need not be the id of the path a plan evaluates, so the
   request names it instead of this context guessing the link. `units_per_order` is required
   for this source (`422` otherwise): units and orders are never compared without the
   WorkloadProfile.
   No bucket in the window is no data (`422 missing-assigned-demand`, as for orders), never
   zero demand. `demand_source` is stored and returned as today.
2. **Scenarios.** A plan may carry an optional `scenario` label (1 to 64 characters, stored
   and returned). Several DRAFT plans may exist for the same `(site_id, path_id, window)`;
   each is a scenario. Creating, reading and listing work exactly as for any plan
   (`GET /capacity-plans?site_id=&path_id=&window_start=&window_end=` filters additively).
3. **Publishing supersedes.** Publishing a plan when another PUBLISHED plan exists for the
   same `(site_id, location, path_id, window)` moves that older plan to a new terminal status
   `SUPERSEDED` and records `CapacityPlanSuperseded`
   (`com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanSuperseded`, integration
   and analytics; `plan_id`, `superseded_by`, `site_id`, `path_id`, `window_start`,
   `window_end`) in the same unit of work as the publication. The event order on publish
   becomes: Superseded (of the older plan), Published, then ShortageDetected and
   BottleneckDetected when short. A superseded plan cannot be published again.
4. **Comparison read.** `GET /capacity-plans/comparisons?ids=a,b[,c...]` (2 to 5 ids, all of
   one site and window; `422` otherwise) returns, per plan: `scenario`, `status`,
   `assigned_demand`, `demand_source`, `path_capacity`, `capacity_over_window`, `shortage`,
   `bottleneck_step`, `bottleneck_constraint` and the labour requirement of ADR 0022. It is a
   read over stored plans: nothing is recomputed, so a comparison is reproducible.
5. **Consumers.** order-management (its ADR 0031) and network-inventory-planning store
   published plans per window. The implementation verifies, in each consumer's code on
   `develop`, that an unknown type is ignored and that the newest published plan for a
   window wins. If a consumer keeps the FIRST published plan instead, the implementation
   stops and that consumer's change becomes its own decision before anything is merged here.

### Implementation notes

- `CapacityPlanSuperseded` also carries `warehouse_id` and `location`, a superset of decision
  3's field list: they are the fields every other capacityplan event carries, and
  order-management lists its rows by `location`. `subject` and the Kafka key are the older
  plan's id; `site_id` is always present (`""` for a plan stored before migration `0008`).
- One PUBLISHED plan per key is a database fact: migration `0017` adds a partial unique index
  on `(site_id, location, process_path_id, window_start, window_end) WHERE status =
  'PUBLISHED'`. The publish locks the older plan (`FOR UPDATE`) and saves it SUPERSEDED before
  the newer plan; a publish that loses on the index reruns its unit once, then answers `409
  version-conflict`.
- Migration `0017` backfills before it builds the index: every PUBLISHED plan except the newest
  of its key (by `published_at`, then id) becomes SUPERSEDED by that newest plan. The backfill
  publishes NO event, so a consumer that kept those plans keeps them until a later publish of
  the key supersedes the newest one.
- Publishing a SUPERSEDED plan answers `409 capacity-plan-superseded`. The comparison read
  returns `status` as stored, SUPERSEDED included. The planning projector acknowledges
  `CapacityPlanSuperseded` without projecting it: the reports keep counting a superseded plan
  as published.

## Consequences

- A plan can be made before orders exist and remade from orders as they arrive; the stored
  `demand_source` says which.
- Planners can lay two or three options next to each other and publish one; the superseded
  chain says what replaced what.
- One new published type and one new status. Status filters on `GET /capacity-plans` gain
  the value; clients that switch on status must accept it.
- A plan from a forecast is only as good as the forecast. The variance read of ADR 0017 shows
  how far the floor ended up from it.

## Alternatives rejected

- **Sum forecast units into `assigned_demand` without `units_per_order`.** Units and orders
  are different units; this context never compares across them without the WorkloadProfile.
- **A separate scenario aggregate.** A scenario is a draft plan with a label; a second
  aggregate would duplicate the plan's whole computation.
- **Recompute scenarios on read.** Stored plans are reproducible; a recomputation would change
  under the reader as standards, constraints and forecasts change.
- **An optimiser that picks the scenario.** Out of scope: the forecast and the scenarios are
  the inputs any later optimisation would need, so they come first.
