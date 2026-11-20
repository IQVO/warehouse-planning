---
description: "ADR 0022: labour standards are read from labor-performance's integration topic into a local copy, and a capacity plan states, per path step, the labour its demand requires (tasks x standard seconds); unknown when no standard or no task factor exists, never guessed. The inbound labour standard and the station standards stay declarative."
---

# ADR 0022: Labour standards come from labor-performance; a plan states the labour each step requires

## Status

Accepted (2026-11-18). First of the four decisions that make this context the owner of the
requirement side of capacity planning (ADR 0022 to ADR 0025). Amends hard rule 5 of
`CLAUDE.md` (labour standards become an event-fed local copy; there is still no live call)
and the "no relationship" row for `labor-performance` on the context map page
(`docs/docs/ddd/context-map.md`). Producer side: labor-performance ADR 0038, which adds
the two standard events to its integration topic and an optional site scope. Additive for
every consumer of this context: no published type, payload or existence changes.

Implemented (2026-11-18): PRs #140 and #141 (migration `0013`).ing response field
changes.

## Context

Labour reaches a plan today in two ways only:

- as committed SUPPLY: workforce-management's `ShiftPlanCommitted` becomes a `LABOR`
  constraint (`labor_capacity_consumer.go`, rate per head x heads);
- as the operator-declared inbound allowance of [ADR 0017](0017-inbound-labour-demand-and-observed-capacity-feedback.md)
  (`minutes_per_appointment`, `minutes_per_asn`).

Nothing states how much labour the demand REQUIRES per step. A plan can say "capacity is
short by 400 orders, PACK binds on LABOR", but not "the window needs 31 PACK labour hours
and the shift committed 24". The engineered standards that answer that question already
exist in the fleet: labor-performance owns `LaborStandard` (task type, optional handling
unit, `expected_seconds`, optional travel component, effective ranges) and scores every
completed task against it. Commercial labour-management products drive the planned workload
from the same engineered standards their measurement uses; two independently declared sets
of numbers drift.

Two facts constrain the design (read from labor-performance's `apis/asyncapi.yaml` and
`internal/domain/standard` on `develop`):

1. `LaborStandardDefined` and `LaborStandardRevised` are published on the ANALYTICS topic
   only. The fleet rule is that only a context's own projector reads its analytics topic, so
   this context may not consume them there. labor-performance ADR 0038 adds both types to
   `warehouse.labor-performance.events` with the same payload.
2. The standard has no site and covers three task types only: `PICK`, `PACK`, `SLAM`.
   Receiving has no standard, and dock appointments carry no units (ADR 0017), so inbound
   labour cannot be derived from an engineered standard yet.

## Decision

1. **Local copy, Kafka only.** A new consumer, `LaborStandardConsumer`, reads
   `warehouse.labor-performance.events`, acts only on the full types
   `com.warehouse.wes.labor-performance.standard.LaborStandardDefined` and
   `...standard.LaborStandardRevised`, and ignores every other type on the topic. The
   processed-event claim (`processed_events`, consumer `labor-standard-consumer`, key = the
   CloudEvents `id`) and the upsert are ONE `ports.UnitOfWork.Do` (ADR 0007). The group id
   comes from the new REQUIRED env var `LABOR_STANDARD_CONSUMER_GROUP` (ADR 0016: no optional
   consumer; the boot error names the variable). `cmd/mcp` and `cmd/a2a` read the copy and
   never start the consumer.
2. **Wire shape (pinned from the producer).** `data`: `standard_id`, `task_type`,
   `expected_seconds` (> 0) and `effective_from` (RFC 3339) are required; optional
   `travel_component_seconds`, `handling_unit` (`EACH` | `CASE` | `PALLET`),
   `team_lift_threshold_g`, `team_lift_expected_seconds`, `previous_expected_seconds`
   (Revised only) and the additive `site_code` of labor-performance ADR 0038. A message
   missing a required field, or with `expected_seconds <= 0`, is dead-lettered
   (deterministic: a retry cannot fix it). Fixtures come from the producer's pinned AsyncAPI,
   and a contract test asserts both types are on the producer's INTEGRATION channel.
3. **The copy stores facts, not a current value.** Table `labor_standards`, one row per
   `standard_id`: `task_type`, `handling_unit` (`''` = task-type-only), `site_code`
   (`''` = every site), `expected_seconds`, `travel_component_seconds`, `effective_from`,
   `as_of` (the CloudEvents `time`). A redelivered or replayed id changes nothing. The
   standard that applies to a plan of site S, task type T and window W is resolved at READ
   time by one pure function shared by the memory and Postgres adapters, among rows with
   `task_type = T`, `handling_unit = ''` and `effective_from <= W.start`:
   - a row with `site_code = S` wins over a row with `site_code = ''`;
   - then the latest `effective_from`;
   - then the latest `as_of`.
   A revision in labor-performance closes the prior record and opens a new one, so the
   latest effective start before the window is the standard in force; arrival order does not
   matter (permutation test over arrival orders). Handling-unit standards are stored but not
   used in this ADR: plan demand has no handling-unit split.
4. **A step's process type names its task type when they are equal** (`PICK`, `PACK`,
   `SLAM`, the producer's closed set; the same convention as ADR 0017's `task_type`). A step
   of any other type (`REBIN`, ...) has no standard: its requirement is unknown with reason
   `no-labor-standard`. There is no mapping table and no default number.
5. **A capacity plan states the labour each step requires** (additive, informational, the
   same footing as the inbound labour of ADR 0017). Per step of the plan's path:
   `process_type`, `standard_seconds`, `standard_site_scoped`, `tasks`,
   `required_labour_hours` = `tasks x standard_seconds / 3600`, `required_average_heads` =
   `required_labour_hours / window hours`, `known` and, when not known, `reason`.
   - `tasks` for `PICK` is the sum of `released_lines` of the orders counted as the plan's
     demand when the demand came from the order read model (`demand_source = orders`;
     ADR 0017 records that a PICK task is one order line).
   - Otherwise, and for every other step, `tasks = assigned_demand x tasks_per_order[step]`,
     where `tasks_per_order` is a new OPTIONAL map on the request, next to
     `units_per_order` and `packages_per_order` (the WorkloadProfile factors already arrive
     on the request). Without a factor the step is unknown with reason `no-task-factor`.
   The requirement is computed when the plan is created and stored with it (migration
   `0013`), like the path capacity: a later revision of a standard never rewrites a stored
   plan. It is returned by `POST /capacity-plans`, `GET /capacity-plans/{id}` and the MCP
   `get_capacity_plan` tool. The ORDER shortage, the bottleneck and every published event are
   unchanged. Publishing labour to siblings is ADR 0023's labour plan, not this field.
6. **The inbound labour standard stays declarative.** `InboundLabourStandard` (ADR 0017) is
   kept: labor-performance has no receiving task type and the appointment facts carry no
   units. It is retired by a later ADR once both a receiving standard and unit-bearing
   inbound demand exist.
7. **Station standards stay.** A `StationStandard` (ADR 0002) is the rate of one piece of
   equipment, not the time a person needs for a task; it is not a labour standard.
8. **Measured actuals are not a planning input.** `TaskPerformanceRecorded` reports how fast
   work was done, including today's inefficiency; the plan uses the standard, and the
   variance read of ADR 0017 shows the gap between the two.

## Consequences

- One owner for engineered standards: planning and performance measurement use the same
  numbers, so "required hours" and "earned hours" are comparable.
- A plan can say how many labour hours each standard-backed step needs, which is the input
  the labour plan of ADR 0023 and the staffing proposal in workforce-management need.
- Steps outside `PICK`, `PACK`, `SLAM` stay unknown until labor-performance adds those task
  types. The response says so per step instead of hiding it.
- A new required env var: the chart (values, Deployment, ConfigMap, render test) and the
  warehouse-infra values entry must land together, infra values first, or the chart stops
  rendering the live application.
- Standards are fleet-wide until someone declares a site-scoped one in labor-performance.

## Alternatives rejected

- **Call labor-performance's `GET /standards/{taskType}` at plan time.** Hard rule 5 and
  labor-performance's own non-negotiable 1 forbid live cross-context calls.
- **Consume `warehouse.labor-performance.analytics`.** Analytics topics belong to their
  context's projector; a consumer outside it couples to a data product with no integration
  guarantees.
- **Keep declaring labour standards here.** Two owners of the same number drift; the
  inbound allowance is kept only because no standard exists for it (decision 6).
- **Plan with measured rates.** See decision 8.
