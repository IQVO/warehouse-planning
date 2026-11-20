---
description: "ADR 0023: the workload forecast (wes-work-planning's ChargeForecast) and the shift labour plan (its PathPlan) move to this context, keyed by site, with their invariants unchanged; LabourPlanCommitted is published for workforce-management, and the drift between the plan and the committed shift moves with it. No importer: both facts are shift-scoped. Glossary: planned capacity vs remaining admission capacity."
---

# ADR 0023: The workload forecast and the shift labour plan move here from wes-work-planning

## Status

Accepted (2026-11-18). Second of the four requirement-side decisions (ADR 0022 to
ADR 0025). Companions: wes-work-planning ADR 0049 (the hand-over: its routes answer 410 and
its aggregates retire) and workforce-management ADR 0043 (it reads the published labour plan
as the requirement). Adds two aggregates, three published types and new REST routes; nothing
existing changes shape.

Implemented (2026-11-19): PRs #142-#148 (see Implementation notes below) and #149.

## Context

The planner's morning crosses three contexts today. The fleet's own warehouse-day simulation
(`e2e-tests`, `cmd/warehouse-day/sim.go`, `planTheDay`) shows it step by step: the charge
forecast goes to wes-work-planning (`POST /paths/{pathId}/charge`), a head proposal comes from
workforce-management (`POST /paths/{pathId}/plan-proposals`), the shift plan is committed in
workforce-management (`POST /shift-plans`) and a second, separate path plan is committed in
wes-work-planning (`POST /paths/{pathId}/plan`).

What wes-work-planning does with its two planning aggregates (read from its `develop`):

- `ChargeForecast` (volume due by CPT, one per path) is saved by `ReceiveChargeForecast` and
  read by nothing. No release, balancing or capacity rule consults it.
- `ShiftPlan` / `PathPlan` (rate x heads x hours, `plannedHeads <= installedStations`) is read
  only by the drift reconciliation of its ADR 0019 and by its own GET. Release, flow balancing
  and `PathCapacityChanged` never read it.
- Its ADR 0006 calls the path plan "this context's own throughput decision". Deciding how
  many heads a path needs for a shift is a planning decision; execution systems release and
  balance work against the floor's live state.

That split makes one question ("do we have the labour for today's forecast?") need two
reconciliation mechanisms (wes ADR 0006 and 0019) and a human in the middle. This context
already answers "can this warehouse process the demand assigned to it"; the forecast and the
required heads are its inputs and outputs.

## Decision

1. **`WorkloadForecast`** (was wes `ChargeForecast`) is owned here.
   - Identity `(site_id, path_id)`; `site_id` is a canonical facility-layout site code
     (ADR 0012), required. One forecast per identity; a new receipt replaces it as a whole
     (the wes semantics).
   - Invariant moved unchanged: at least one bucket. A bucket is `{cpt, quantity}` with an
     RFC 3339 `cpt` and a non-negative whole `quantity` in units.
   - REST: `PUT /workload-forecasts/{site_id}/{path_id}` (an idempotent PUT, so the
     Idempotency-Key middleware is not involved) and `GET` of the same resource.
   - Event `WorkloadForecastReceived`, type
     `com.warehouse.wes.warehouse-planning.workloadforecast.WorkloadForecastReceived`, on the
     integration and the analytics topic: `site_id`, `path_id`, `total_quantity`,
     `bucket_count`, `received_at`, `version`. A superset of wes's `ChargeForecastReceived`
     (which carried `path_id` only).
   - Not moved: the pick-load annotations of a wes bucket (`sku`, `pickUnit`, `handles`,
     `cubeMm3`, `weightG`). wes only echoed them back; no context reads them.
2. **`LabourPlan`** (was wes `PathPlan`) is owned here.
   - Identity `(site_id, shift_id, path_id)`. Fields: `planned_heads`,
     `rate_units_per_hour`, `hours`, `installed_stations`, optional `shift_start`,
     `heads_source` (`request` | `forecast`), `version`.
   - Invariants moved unchanged: `planned_heads <= installed_stations`, `hours > 0`, and a
     finite `rate x heads x hours`. `installed_stations` stays caller-supplied; when the
     facility tally (ADR 0002) knows fewer stations of the activity named like the path, the
     plan carries a warning (the number is reported, never silently replaced).
   - `planned_heads` may be omitted. It is then derived, like the order demand of ADR 0004:
     `ceil(forecast units with cpt in [shift_start, shift_start + hours) / (rate x hours))`,
     which needs `shift_start` and a stored forecast for `(site_id, path_id)`; otherwise the
     request is refused with `422 missing-planned-heads`. Zero forecast buckets in the shift
     is no data, never zero heads.
   - REST: `PUT /labour-plans/{site_id}/{shift_id}/{path_id}` and `GET` of it, plus
     `GET /labour-plans?site_id=&shift_id=` (keyset page). A PUT that changes nothing raises
     no event and keeps the version.
   - Event `LabourPlanCommitted`, type
     `com.warehouse.wes.warehouse-planning.labourplan.LabourPlanCommitted`, integration and
     analytics: every field above. Key and `subject`: `site_id/shift_id/path_id`.
3. **The drift moves with the plan.** The labour consumer already reads
   workforce-management's `ShiftPlanCommitted`; it also keeps `shift_id` and `planned_heads`
   per `(site, shift, path)` (additive columns). When either side changes and the committed
   heads differ from the labour plan's, the context raises `LabourPlanDriftDetected`
   (entity `labourplan`, integration and analytics): `site_id`, `shift_id`, `path_id`,
   `planned_heads`, `committed_heads`, `drift_heads`. It is raised only when the drift is
   non-zero and differs from the last one raised for that key, and it never corrects either
   side (the surfacing rule of wes ADR 0019, kept).
4. **Who reads what.** workforce-management keeps a copy of `LabourPlanCommitted` and
   proposes heads from it (its ADR 0043). wes-work-planning keeps no copy: nothing in its
   release, balancing or capacity rules reads a plan. Its `LaborPlanObserved` (the committed
   workforce plan, wes ADR 0006) and `PathCapacityChanged` (wes ADR 0018) stay.
5. **Hand-over without an importer.** Both facts are shift-scoped and the planner sends them
   again every shift, so there is no legacy importer and no backfill. The order is: this
   context ships the aggregates and routes; the warehouse-day simulation switches its
   planner's morning to them; wes-work-planning then answers `410` on its two POST routes and
   retires its three planning types (wes ADR 0049). The switch happens at a shift boundary.
6. **Language.** *Planned capacity* is this context's figure for a path and window (a
   published capacity plan, the labour plan). *Remaining admission capacity* is
   wes-work-planning's live figure per path and CPT (`PathCapacityChanged`). Both enter the
   ubiquitous-language page; no published type is renamed.
7. **MCP and A2A.** No tool or skill is added by this ADR: the MCP surface is capped by
   `governance_test.go` and a new tool is a charter decision of its own.

### Implementation notes (2026-11-19)

Recorded after PRs #142-#148, so the decision text and the code say the same thing.

- **Decision 2, station warning.** A facility tally that knows NO station of the activity is
  no data, not a shortfall: the warning is raised only when the tally knows at least one but
  fewer stations than `installed_stations`, the same rule the capacity plan applies to its
  station standards. It is computed on every write and read and never stored.
- **Decision 3, storage.** The committed heads are not columns on the LABOR constraint
  tables: they live in their own table `labour_plan_commitments` (migration `0015`:
  `committed_heads`, `observed_at`, `last_drift_heads`), so the LABOR registration stays
  byte-identical. The consumer writes it as a second step of the same unit of work as the
  processed-event claim and the LABOR registration.
- **Decision 3, raise rule.** "Differs from the last one" means the last drift RECORDED, and
  every computed drift is recorded, zero included. A drift that clears and comes back is
  therefore raised again; the same drift twice in a row is not. Only a changed labour plan
  (trigger B) or a new commitment (trigger A) computes a drift; an unchanged PUT and a
  redelivered event compute nothing.
- **Identity, path code.** `path_id` is a process-path code in one canonical form, trimmed
  and upper case, in the forecast, the labour plan, the committed heads and every published
  `path_id` (PR #150), the rule the LABOR constraint already applied. `site_id` and `shift_id`
  are matched as sent. Nothing had been released with the as-sent form, so no data migration.

## Consequences

- The planner's morning is two calls in this context (forecast, labour plan) plus the supply
  commitment in workforce-management, instead of four calls across three contexts.
- Required heads are computed where the forecast, the capacity constraints and the standards
  of ADR 0022 already live, and published once.
- wes-work-planning loses no rule it uses: the two aggregates it hands over were read by
  nothing but its drift check, which moves here.
- Three new published types (catalogue rows in ADR 0006, AsyncAPI on both channels) and new
  routes (OpenAPI, the generated REST reference).
- The pick-load annotations and wes's travel-distance hint on a committed plan (wes ADR 0017)
  are retired with the wes routes. Heavy handling is already a labour-standard concern
  (labor-performance's handling-unit and team-lift standards).

## Alternatives rejected

- **Leave planning in wes-work-planning.** The status quo: two planning aggregates nobody in
  that context reads, and two reconciliation ADRs to keep them aligned with workforce.
- **A new labour-planning context.** It would own exactly the requirement side this context
  exists for; extend before new.
- **Move the forecast and plan into workforce-management.** Workforce owns labour supply
  (associates, certifications, assignments, attendance); the requirement is derived from
  demand and capacity, which live here. Its ADR 0002 keeps it at the path boundary.
- **Import wes's stored rows.** One forecast and one plan per path, overwritten every shift:
  an importer would copy data that is stale by the next shift.
