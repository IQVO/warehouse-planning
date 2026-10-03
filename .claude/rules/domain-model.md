# Domain model: ubiquitous language, aggregates, events, use cases

## Ubiquitous Language (use these exact names)

- **ProcessCapacity** — the usable throughput of one warehouse process at
  one location for one time window; the minimum across its registered
  constraints (labor, location, equipment, station, conveyor, buffer,
  replenishment).
- **CapacityConstraint** — one named limiting factor (type + rate) attached
  to a ProcessCapacity.
- **CapacityRate** — a quantity + native unit + period, e.g. `4000 UNIT /
  HOUR`. Never compared across differing units without going through a
  WorkloadProfile first.
- **CapacityWindow** — the `[start, end)` period a capacity value is valid
  for. A capacity number with no window is incomplete by definition.
- **WorkloadProfile** — the per-warehouse conversion factors (e.g. units
  per order, packages per order) used to normalize different processes'
  native rates into one comparable flow unit.
- **ProcessPath** — an ordered sequence of ProcessTypes a given workload
  must flow through (e.g. Pick -> Rebin -> Pack). **Locally owned by this
  context** (operator-declared via `POST /process-paths`) — NOT a
  Conformist copy of `process-path-management`'s aggregate.
  `process-path-management`'s `ProcessPath` is routing/capability metadata
  (`path_id`, `required_capabilities`, `eligibility`...), it carries no
  physical step sequence; the two contexts share `path_id` only as a loose
  human cross-reference. See `docs/adr/0001-...` Addendum (2026-10-03).
- **WorkloadProfile** — see above; `NormalizeToOrderRate` converts a
  UNIT or PACKAGE `CapacityRate` into an ORDER rate for the same period.
- **ProcessPathCapacity** — the normalized, end-to-end throughput of a
  ProcessPath: the minimum of its steps' effective capacities after
  WorkloadProfile normalization, plus which step is the bottleneck.
  Computed by `ComputeProcessPathCapacity`
  (`internal/domain/processcapacity/process_path_capacity.go`), a domain
  SERVICE, not a stored aggregate.
- **CapacityPlan** — the aggregate that ties assigned demand for a
  warehouse location + planning window to the ProcessPathCapacity available to
  serve it, and the resulting shortage (if any). Implemented in Phase 4.
- **Bottleneck** — the constraint or process-path step currently limiting
  end-to-end flow.

## Aggregates

- **ProcessCapacity** (`internal/domain/processcapacity`): identity
  `(ProcessType, Location, CapacityWindow)`. Invariant: at least one
  constraint; all constraints on one instance share the same native unit
  (never silently force-compare UNIT against PACKAGE). `EffectiveRate()`
  is the minimum across constraints plus which constraint type is
  binding.
- **CapacityPlan** (`internal/domain/capacityplan`, Phase 4, implemented): id =
  a UUID string (natural key `(WarehouseID, PlanningWindow)`). Fields:
  `WarehouseID`, `Location` (the ProcessCapacity location evaluated),
  `PlanningWindow` (a `processcapacity.CapacityWindow`), `ProcessPathID`,
  `AssignedDemand` (orders, `>= 0`), and computed-at-creation `PathCapacity`
  (ORDER/HOUR, from `ComputeProcessPathCapacity`), `BottleneckStep`,
  `CapacityOverWindow` (= `PathCapacity` x window hours) and `Shortage`
  (= `max(0, demand - capacityOverWindow)`, never negative; demand exactly
  equal to the capacity is NOT a shortage), `Status` DRAFT | PUBLISHED.
  The aggregate does no I/O: `Create` takes the already-computed path rate and
  bottleneck plus an explicit id and time. `Publish` moves DRAFT -> PUBLISHED
  and returns `ErrAlreadyPublished` on a second call (never a silent double
  publish). Events are plain structs it accumulates; `PullEvents()` hands each
  over exactly once. `Rehydrate` rebuilds a stored plan for repositories
  without events.

## Domain events

Raised by CapacityPlan (published on `warehouse.warehouse-planning.events`,
see `integration-events.md`):

- `CapacityPlanCreated` -- recorded by `Create`.
- `CapacityPlanPublished` -- recorded by `Publish`, always.
- `CapacityShortageDetected` -- recorded by `Publish`, only when
  `Shortage > 0`.
- `BottleneckDetected` -- names the limiting process step; recorded by
  `Publish`, only when `Shortage > 0`.

Order on a shortage plan: Created (at creation), then Published,
ShortageDetected, BottleneckDetected (at publish).

Vocabulary only, NOT implemented or published yet (nothing raises them):
`ProcessCapacityRegistered` (a native constraint was registered) and
`ProcessCapacityChanged` (the effective rate changed).

## Key use cases (`internal/application/usecases`)

- `RegisterProcessCapacityConstraint` — upserts one constraint on a
  ProcessCapacity aggregate, recomputes and persists the effective rate.
- `GetEffectiveProcessCapacity` — query: effective rate + binding
  constraint for a process+location+window.
- `GetProcessPathCapacity` (Phase 2) — resolves a ProcessPath's steps'
  ProcessCapacity + the warehouse WorkloadProfile, returns normalized path
  capacity and bottleneck step.
- `CreateCapacityPlan` (Phase 4) — resolves the path capacity through
  `GetProcessPathCapacity` (the Phase 2 path; each step's ProcessCapacity must
  be registered for EXACTLY the plan's window), builds the aggregate, saves it
  and queues `CapacityPlanCreated` in the outbox -- one `ports.UnitOfWork.Do`.
  Assigned demand and the WorkloadProfile factors arrive in the request body
  (documented simplification: the final demand-ingestion shape from
  order-management/network-fulfillment is a later decision, and this context
  makes no live cross-context lookup).
- `PublishCapacityPlan` (Phase 4) — loads the plan, `Publish()`, saves it and
  queues every recorded event in the outbox, in one `UnitOfWork.Do`. The
  Postgres `FindByID` locks the row inside the unit of work, so concurrent
  publishes serialize.

Full phased delivery plan, worked examples (reproduced here as test
fixtures) and acceptance criteria: see the maintainer's
`warehouse-planning-bc-plan.md` design doc (referenced from this repo's
README).
