# ADR 0017: Inbound labour demand and observed capacity feedback

## Status

Accepted (2026-10-20). Additive: nothing that worked before changes, no
published CloudEvents type, payload, topic or existing REST field changes.
Closes the two "planned, not implemented" edges of the context map: the inbound
path (`inbound-receiving` -> this context) and, for the part that can be built
honestly from published events, the observed-capacity feedback that
[ADR 0001](./0001-warehouse-planning-bounded-context.md) listed as a "later
phase" (`fulfillment-execution` -> this context). ADR 0001 stays as written;
this ADR supersedes only that "later phase" note.

## Context

### What a capacity plan could not see

A `CapacityPlan` (ADR 0004, ADR 0012) evaluates ORDER demand against the
ORDER/hour capacity of a process path. Two facts were missing:

1. **Inbound work.** `inbound-receiving` publishes `DockAppointmentBooked` and
   `DockAppointmentCancelled` on `warehouse.inbound-receiving.events`. Nothing
   consumed them, so a window with six trucks at the dock looked identical to a
   window with none. The labour a receipt takes is a business rule nobody had
   specified.
2. **What the floor really does.** Capacity is planned (labour shifts, station
   standards) and never compared with what was completed.

### What the producers actually carry (read from their own
`apis/asyncapi.yaml` on `develop`, not guessed)

`inbound-receiving`, topic `warehouse.inbound-receiving.events`, key and
`subject` = the appointment id, `type`
`com.warehouse.wms.inbound-receiving.dockappointment.<EventName>`:

| event | `data` |
| --- | --- |
| `DockAppointmentBooked` | `appointment_id`, `door_code`, `carrier`, `window_start`, `window_end`, `asn_numbers` (at least one) |
| `DockAppointmentCancelled` | `appointment_id`, `door_code`, optional `reason` |

(`DockAppointmentCheckedIn` and `DockAppointmentCompleted` exist and are
ignored: a checked-in or completed appointment is still the demand it was.)
What is **not** there, and therefore not invented:

- **No site.** Neither event carries a site; `door_code` is a facility-layout
  location code (`WH1-DOCK-IN-01`) and prefix-parsing it would be exactly the
  inference ADR 0012 forbids.
- **No units, pallets or lines.** The appointment names its ASNs
  (`asn_numbers`) and no quantities. Expected units live on `ASNRegistered`,
  a different aggregate on the same topic; joining them would be a second read
  model and is out of scope here.
- **No revision number.** The only ordering fact is the CloudEvents `time`.
  The aggregate raises one `Booked` per id and `Cancel` is terminal from
  `Booked` (inbound-receiving's `appointment.DockAppointment`), so an id has at
  most one booking and one cancellation, on a topic keyed by appointment id, but
  consumers must still tolerate redelivery and reordering across a DLQ or a
  rebalance.

`fulfillment-execution`, topic `warehouse.fulfillment.events`, key and
`subject` = the task id, `type`
`com.warehouse.wes.fulfillment-execution.task.TaskCompleted`:
`task_id`, `station_id`, `work_unit_id`, and optional `associate_id`,
`duration_seconds`, **`task_type`** (`PICK`, `PACK`, `SLAM`, `REBIN`,
`DISPATCH`, `ARRIVAL`, `RELOCATE`, `REPLENISH`), `order_ref`, `line_no`, plus
RELOCATE/REPLENISH extras. What is **not** there:

- **No site and no process-path id.** The only attribution is `task_type`, the
  process STEP. Its values coincide with the `ProcessType` names a local
  `ProcessPath` is made of (`PICK`, `REBIN`, `PACK`).
- **The unit is a TASK, not an ORDER.** A PICK task is per order line; there is
  no `lines_per_order` on any event. Counting tasks and calling them orders
  would be an invented factor (the same reason ADR 0004 refuses
  `units = lines x something`).

The analytics stream of `fulfillment-execution` also enriches the path, but it
is that service's data product, not an integration contract (ADR 0004 rejected
the same shortcut for order-management).

## Decision

### Part A: inbound labour demand (gap K28)

1. **Local read model, Kafka only.** A new consumer, `InboundDemandConsumer`,
   reads `warehouse.inbound-receiving.events` and writes
   `inbound_appointments` (one row per appointment id). No REST/MCP call to
   `inbound-receiving` anywhere; `cmd/mcp` reads the same table.
2. **The labour minutes are a DECLARATIVE planning standard owned by this
   context**, in the style of the station standards (ADR 0002):
   `InboundLabourStandard`, keyed `(location, process_path_id)`:
   - `minutes_per_appointment` (REQUIRED, `> 0`): fixed labour per dock
     appointment;
   - `minutes_per_asn` (optional, default 0, `>= 0`): labour per ASN the
     appointment covers. This is the only per-"expected unit" figure the booked
     payload really carries. Units, pallets and lines are absent, so a
     per-unit minute is deliberately NOT offered (it would have nothing to
     multiply). An appointment's labour is therefore
     `minutes_per_appointment + minutes_per_asn x len(asn_numbers)`.

   Operators declare it with `PUT /inbound-labour-standards/{location}/{path_id}`
   and the MCP tool `declare_inbound_labour_standard`; `GET
   /inbound-labour-standards?location=` lists them. Like station capacity
   (ADR 0002 decision 3) the labour is composed at READ time from stored
   appointment FACTS and the current standard: nothing derived is stored on
   an appointment, so a late or revised declaration converges with no
   backfill.
3. **Site attribution is configured, not inferred.** Exactly as ADR 0004
   decision 4, all consumed appointments (and, in Part B, task completions) are
   attributed to ONE configured site: the existing env `DEMAND_SITE_ID`,
   stored as `location` at write time. No new site variable; the order,
   dock-appointment and task views of a site therefore agree by construction.
4. **Window semantics are ADR 0003/0004's.** An appointment is demand in the
   half-open window `[W.start, W.end)` when its door window STARTS inside it
   (`window_start >= W.start AND window_start < W.end`): a start exactly at
   `W.start` counts, exactly at `W.end` does not, so adjacent windows never
   count an appointment twice. No pro-rating of an appointment across windows
   (ADR 0003 rejected overlap pro-rating for the same reason).
5. **Cancel is idempotent and order-independent: last writer wins on the
   producer's event `time`, and `Cancelled` wins a tie.**
   `inbound.Resolve(prev, next)`:
   - no stored row: the event is stored (a `Cancelled` that arrives first is
     stored as a TOMBSTONE: status `CANCELLED`, no window, no ASN count);
   - `next.time > prev.time`: `next` wins (a cancel keeps the booking's window
     and ASN count on the row and flips its status);
   - `next.time < prev.time`: the event is stale and changes nothing, so a
     `Booked` that arrives after the `Cancelled` that supersedes it is ignored;
   - equal times: a `Cancelled` replaces a `Booked`, never the reverse.
   The processed-event claim (`processed_events`, consumer
   `inbound-demand-consumer`, key = the CloudEvents `id`) and the upsert are
   ONE `ports.UnitOfWork.Do` (the same atomicity as ADR 0004); the Postgres
   upsert reads the row `FOR UPDATE` and decides with the SAME `Resolve` the
   in-memory adapter uses. Only `BOOKED` rows are demand.
6. **No standard means no demand, said out loud.** An appointment is always
   recorded. When no standard is declared for `(location, path_id)` it
   contributes 0 minutes and the read says `standard_missing: true`. A plan
   adds the warning `inbound standard missing: ...` **when appointments exist
   in its window and the site has no inbound standard for ANY path**; a site
   that declared a standard for another path (the plan is an outbound path)
   gets no warning. No default number is ever applied.
7. **Plan integration.** `POST /capacity-plans` / `create_capacity_plan` read
   the inbound demand of `(location, path_id, window)` and the plan stores
   `inbound_appointments`, `inbound_labour_minutes` and `inbound_standard_missing`
   (migration `0009`), and exposes them additively with
   `inbound_average_heads` = minutes / window minutes (the average number of
   people the inbound work needs over the window, comparable by a human with
   planned heads). Inbound demand is a first-class demand source NEXT TO order
   demand, **not summed into `assigned_demand`**: orders and labour minutes are
   different units, and this context never compares across units without a
   conversion it can defend (domain-model.md, WorkloadProfile). The ORDER
   shortage, `demand_source` and every published event are unchanged. Reads:
   `GET /inbound-demand?location=&path_id=&window_start=&window_end=` and MCP
   `get_inbound_demand`.

### Part B: observed capacity feedback (gap X4)

8. **Observed throughput is a local read model.** A new consumer,
   `ObservedCapacityConsumer`, reads `warehouse.fulfillment.events`, acts only
   on `TaskCompleted`, and adds the completion to `observed_capacity` (migration
   `0010`): one row per `(location, process_type, minute)` with
   `tasks_completed`, `duration_seconds_total`, `tasks_with_duration` and
   `as_of`. `process_type` is the event's `task_type` upper-cased (the process
   STEP; the event carries no path), `location` is `DEMAND_SITE_ID` (decision
   3), and the minute is the event's `time` truncated to the minute (UTC).
   Resolution is therefore one minute: a completion is attributed to the minute
   it happened in and a window bound inside a minute follows `bucket_start >=
   W.start AND bucket_start < W.end`. The claim and the additive upsert are ONE
   unit of work, so a redelivered `id` adds nothing (exactly-once effect on
   at-least-once delivery). A `TaskCompleted` without a `task_type` cannot be
   attributed and is dead-lettered.
9. **Variance is read-only and advisory.** `GET /capacity-plans/{id}/variance
   [?tasks_per_order=]` and MCP `get_capacity_plan_variance` return, for the
   plan's window and its BOTTLENECK step: the planned ORDER/hour and
   ORDER-over-window, the observed `tasks`, `tasks_per_hour`, mean task
   duration and `as_of`, and the variance. **The variance exists only when the
   caller supplies `tasks_per_order`** (the same shape as the WorkloadProfile
   factors `units_per_order`/`packages_per_order`, which also arrive on the
   request): `observed_orders_equivalent = tasks / tasks_per_order`,
   `variance_orders = observed - planned_over_window`, `variance_percent` of
   the planned figure. Without it the answer is `comparable: false` with the
   reason, never a guess. No tasks observed is `has_data: false`, never zero
   throughput. A plan whose bottleneck is not a task step (no task type of
   that name completed) is simply `has_data: false`.
10. **It never feeds back automatically.** Observed capacity does NOT mutate a
    planned `ProcessCapacity`, does NOT change a stored plan and does NOT
    republish or re-create anything. It is information for a human to re-plan
    with. Automatic adjustment of planned capacity from observed throughput is
    explicitly a later ADR, which must also reason about convergence (a plan
    that lowers capacity from a bad shift would lower the next plan).
11. **Reports.** `cmd/planning-reports` reads only the analytical database
    (ADR 0005); observed completions and inbound appointments live in the
    operational database, and the variance depends on a stored plan plus a
    caller-supplied factor. The reports binary is therefore NOT extended.

### Wiring and failure handling

12. **Both consumers are always on in `cmd/api`** (ADR 0016: no optional or
    disabled-by-default consumer group). Two new required env vars, each with no
    default and a boot error that names it:
    `INBOUND_DEMAND_CONSUMER_GROUP` and `OBSERVED_CAPACITY_CONSUMER_GROUP`
    (chart values `inbound.consumerGroup`, `observed.consumerGroup`). The
    group ids stay out of the code (`TestKafkaConsumerGroupNeverHardcodedInline`).
13. **Dead letters for deterministic failures.** Both consumers ignore every
    other `type` on their shared topics. A message that is not CloudEvents 1.0,
    or a KNOWN type whose payload is unusable (malformed `data`, blank
    appointment id, `subject` different from the appointment id, a window whose
    end is not after its start, no ASN, a missing event `time`, a `TaskCompleted`
    with no `task_type`/`task_id`) is published at once to `<topic>.dlq` with the
    `x-dlq-*` headers and its offset is committed, instead of being skipped
    silently (the order-demand consumer's older behaviour, unchanged). Transient
    failures keep the existing bounded retry (5 attempts) and then the same
    DLQ.

## Consequences

- A planner sees, beside the ORDER shortage, how many dock appointments fall in
  the window and how many people they need on average, and is told when no
  standard exists. Operators own the number; it is visible and replaceable.
- The two views of one site (orders, dock appointments, task completions) share
  `DEMAND_SITE_ID`. A deployment whose upstreams span several sites needs a
  usable site on the events first; the consumers would then read it and
  `DEMAND_SITE_ID` becomes a fallback (as ADR 0004 said for orders).
- Inbound minutes are NOT netted against labour supply: the labour constraint
  is stored as `UNIT/hour` without heads or hours, so there is no labour-minutes
  supply to compare to. `inbound_average_heads` is the honest bridge.
- The variance is only as good as the supplied `tasks_per_order`, and at most
  as fine as one minute. Duplicate completions under DIFFERENT event ids would
  be double counted; the producer keeps the id stable across outbox redelivery.
- Two migrations (`0009`, `0010`), purely additive: new tables and defaulted
  columns, down migrations drop them. They run on boot of `cmd/api` and
  `cmd/mcp`.
- The MCP tool budget goes from 11 to 14 deliberately
  (`declare_inbound_labour_standard`, `get_inbound_demand`,
  `get_capacity_plan_variance`); `TestToolSurface` still pins the exact set.
- Deployment: the two group variables are required, so the new image does not
  boot (and the chart does not render) until warehouse-infra provides them. The
  infra values change is a separate, live change for the existing app.

## Explicit non-goals

- No change to `CapacityPlanCreated/Published/ShortageDetected/BottleneckDetected`
  or to `assigned_demand`'s meaning, no new CloudEvents type, no `.v2`.
- No consumption of `ASNRegistered`, `DockAppointmentCheckedIn/Completed`,
  `ReceiptLineReceived`, `TaskClaimed` or any analytics topic; no per-unit
  inbound minutes.
- No conversion of labour minutes into orders, and no per-step variance (the
  plan stores only its bottleneck step).
- No automatic adjustment of planned capacity from observed capacity; no change
  to `cmd/planning-reports`; no auth on any new endpoint or tool.
