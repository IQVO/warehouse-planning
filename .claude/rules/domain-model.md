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
  must flow through (e.g. Pick -> Rebin -> Pack). Owned by
  `process-path-management`; this context holds a read-only, Conformist
  copy.
- **ProcessPathCapacity** — the normalized, end-to-end throughput of a
  ProcessPath: the minimum of its steps' effective capacities after
  WorkloadProfile normalization, plus which step is the bottleneck.
- **CapacityPlan** — the aggregate that ties assigned demand for a
  warehouse + planning window to the ProcessPathCapacity available to
  serve it, and the resulting shortage (if any).
- **Bottleneck** — the constraint or process-path step currently limiting
  end-to-end flow.

## Aggregates

- **ProcessCapacity** (`internal/domain/processcapacity`): identity
  `(ProcessType, Location, CapacityWindow)`. Invariant: at least one
  constraint; all constraints on one instance share the same native unit
  (never silently force-compare UNIT against PACKAGE). `EffectiveRate()`
  is the minimum across constraints plus which constraint type is
  binding.
- **CapacityPlan** (`internal/domain/capacityplan`, Phase 4): identity
  `(WarehouseID, PlanningWindow)`. Invariant: cannot be published twice;
  shortage is always `max(0, demand - capacity*duration)`, never negative.

## Domain events

- `ProcessCapacityRegistered` — a native constraint was registered for a
  process+location+window.
- `ProcessCapacityChanged` — the effective rate changed because a
  constraint changed.
- `CapacityPlanCreated` / `CapacityPlanPublished` (Phase 4).
- `CapacityShortageDetected` — shortage > 0 at publish time (Phase 4).
- `BottleneckDetected` — names the limiting process step (Phase 4).

## Key use cases (`internal/application/usecases`)

- `RegisterProcessCapacityConstraint` — upserts one constraint on a
  ProcessCapacity aggregate, recomputes and persists the effective rate.
- `GetEffectiveProcessCapacity` — query: effective rate + binding
  constraint for a process+location+window.
- `GetProcessPathCapacity` (Phase 2) — resolves a ProcessPath's steps'
  ProcessCapacity + the warehouse WorkloadProfile, returns normalized path
  capacity and bottleneck step.
- `CreateCapacityPlan` / `PublishCapacityPlan` (Phase 4) — demand in,
  shortage/bottleneck out, publishes the capacity-plan events.

Full phased delivery plan, worked examples (reproduced here as test
fixtures) and acceptance criteria: see the maintainer's
`warehouse-planning-bc-plan.md` design doc (referenced from this repo's
README).
