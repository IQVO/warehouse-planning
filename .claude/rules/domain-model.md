---
paths:
  - "internal/domain/**"
  - "internal/application/**"
  - "features/**"
  - "features_test.go"
---

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
  for. A capacity number with no window is incomplete by definition. A
  window `C` COVERS a planning window `W` when `C.start <= W.start AND
  C.end >= W.end` (`CapacityWindow.Covers`; a window covers itself): that is
  the rule by which a registered constraint applies to a requested window
  (docs/adr/0003). The window is also the ProcessCapacity's IDENTITY, which
  stays an exact key.
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
  UNIT or PACKAGE `CapacityRate` into an ORDER rate for the same period
  (an ORDER rate passes through unchanged; LINE is rejected with
  `ErrUnsupportedNormalizationUnit`).
- **StationStandard** — the OPERATOR-DECLARED throughput of ONE station of a
  process at a site: keyed `(location, process type)`, valued as a
  `CapacityRate` in the process's natural unit (e.g. `180 PACKAGE / hour` for
  PACK at SIM1). A planning parameter owned by this context -- no upstream
  publishes it (`internal/domain/processcapacity/station_standard.go`).
  Quantity must be positive; the unit must be UNIT, PACKAGE or ORDER.
- **Station count** — how many work-center stations facility-layout tallied
  for an activity across the zones of a site. A COUNT has no throughput of
  its own: only `count x StationStandard` does (design doc section 19:
  10 stations x 180 packages/hour/station = 1,800 packages/hour). Counts live
  in the facility tally (`location_slot_tally`), never as a ProcessCapacity.
- **Site / location** — a planning `location` is a site (building) code, e.g.
  `SIM1`: the labor consumer's `building_id`, and the first dash-separated
  segment of the facility zone ids of that site (`SIM1-OPS-WC`,
  `SIM1-STOR-AMB`). The zones of a site are the tally zones whose id starts
  with `<location>-`; a zone with no matching site contributes nothing.
- **Expected demand (order)** — the local read model of orders
  order-management has promised (`internal/domain/demand`, docs/adr/0004): one
  `demand.Order` per order id (`location` = the ONE configured site
  `DEMAND_SITE_ID`, `promise_at` = the order's `promise_date`, `released_lines`,
  `as_of` = the CloudEvents `time`), fed only by Kafka. An order is demand in
  `[start, end)` when `promise_at >= start AND promise_at < end`
  (`Order.CountsIn`: start counts, end does not); last writer wins per order id
  on `as_of`, later-or-equal replaces (`Order.Supersedes`). `Summary.Orders` is
  what a plan defaults its `assigned_demand` to when the caller omits it; zero
  orders is NO DATA (`Summary.HasData`), never zero demand. There are no units
  and no cancellation netting: order-management does not publish them.
- **DemandSource** — where a plan's `assigned_demand` came from: `request`
  (stated) or `orders` (defaulted from the read model); stored on the plan.
- **ProcessPathCapacity** — the normalized, end-to-end throughput of a
  ProcessPath: the minimum of its steps' effective capacities after
  WorkloadProfile normalization, plus which step is the bottleneck and which
  constraint type binds it. Computed by `ComposeProcessPathCapacity`
  (`internal/domain/processcapacity/process_path_capacity.go`), a domain
  SERVICE, not a stored aggregate; `ComputeProcessPathCapacity` is the same
  computation over registered constraints alone (the section-31 worked
  example).
- **Step composition** — `ComposeStepCapacity`
  (`internal/domain/processcapacity/step_capacity.go`): for a path step with
  process P at location L and window W, the candidate constraints are
  (a) the constraints of the ProcessCapacity aggregates of `(P, L)` whose
  window COVERS W (`StepInput.Covering`, from
  `ProcessCapacityRepository.FindCovering`; ADR 0003): per constraint TYPE the
  one from the aggregate with the LATEST window start (tie: the narrower
  window, i.e. the earlier end) -- an older/wider covering aggregate's
  constraint of the SAME type is shadowed, other types still apply -- and
  (b) a DERIVED STATION constraint = `stationCount(L, activity=P) x
  StationStandard(L, P)`, present only when BOTH a station count > 0 and a
  standard exist. Every candidate is normalized to ORDER with the request's
  WorkloadProfile BEFORE comparing (units/hour and packages/hour are not
  comparable raw -- design doc rule 8); the step's effective rate is the
  minimum and its binding constraint type (LABOR, STATION, ...) is reported.
  Ties go to the earliest candidate (registered constraints in registration
  order, then STATION). Stations tallied but no standard declared: the step
  uses (a) only and a WARNING is carried -- a throughput is never invented. No
  candidate at all: `ErrMissingStepCapacity`. Composition happens on
  transient values at READ time (nothing derived is stored: it converges, has
  no stale rows and handles late declarations); the stored ProcessCapacity
  aggregate and its single-native-unit invariant are untouched.
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
  `WarehouseID`, `SiteID` (canonical site, a facility-layout Site
  `site_code`; REQUIRED at `Create` since ADR 0012 — never inferred from
  `WarehouseID` or `Location`, and empty only for rows stored before
  migration 0008), `Location` (the ProcessCapacity location evaluated),
  `PlanningWindow` (a `processcapacity.CapacityWindow`), `ProcessPathID`,
  `AssignedDemand` (orders, `>= 0`), and computed-at-creation `PathCapacity`
  (ORDER/HOUR, from `ComputeProcessPathCapacity`), `BottleneckStep`,
  `CapacityOverWindow` (= `PathCapacity` x window hours) and `Shortage`
  (= `max(0, demand - capacityOverWindow)`, never negative; demand exactly
  equal to the capacity is NOT a shortage), `Status` DRAFT | PUBLISHED, plus
  the informational composition outcome `BottleneckConstraint` (the
  constraint type binding the bottleneck step, e.g. STATION) and `Warnings`
  (no published event carries them).
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

`CapacityPlanPublished` also carries `BottleneckConstraint` (the plan's binding
constraint at the bottleneck step; empty for a plan created before it was
recorded). The INTEGRATION payload does not serialize it: only the analytics
stream does (ADR 0005, `integration-events.md`). Since ADR 0012 it also
carries `SiteID`, serialized ADDITIVELY as `site_id` on the v1 integration
payload (`omitempty`: a pre-migration-0008 plan publishes without it, and a
legacy payload without it still decodes). The same four events are written
to both the integration and the analytics topic by the outbox; the analytical
side (`internal/analytics/report`, `analyticsstore`) is a projection built from
them and the domain never imports it.

Vocabulary only, NOT implemented or published yet (nothing raises them):
`ProcessCapacityRegistered` (a native constraint was registered) and
`ProcessCapacityChanged` (the effective rate changed).

## Key use cases (`internal/application/usecases`)

- `RegisterProcessCapacityConstraint` — upserts one constraint on a
  ProcessCapacity aggregate, recomputes and persists the effective rate.
- `GetEffectiveProcessCapacity` — query: effective rate + binding
  constraint for a process+location+window.
- `GetProcessPathCapacity` (Phase 2, composition since ADR 0002, coverage since
  ADR 0003) — resolves a
  ProcessPath's steps' covering ProcessCapacity aggregates
  (`ports.ProcessCapacityRepository.FindCovering`), the site's tallied station
  counts (`ports.StorageTallyReader`) and the declared StationStandards, plus
  the warehouse WorkloadProfile, and returns normalized path capacity, the
  bottleneck step and its binding constraint, every step's composed result
  (`step_breakdown`) and the warnings.
- `DeclareStationStandard` — validates and upserts a StationStandard
  (created vs replaced is reported).
- `GetStorageCapacity` — query: the facility tally of a site as a READ MODEL
  (storage positions per zone + locationType, stations per zone + activity).
  Positions are NOT process throughput (they stay the StorageCapacityPool
  idea of design doc sections 14-17) and there is no "consumed" figure --
  stock must never be read from inventory-storage (design doc rule 1).
- `CreateCapacityPlan` (Phase 4; demand defaulting since ADR 0004) — when the
  request OMITS `assigned_demand` (`DemandFromOrders`) it first resolves the
  demand from `GetExpectedDemand` (orders expected at the plan's location in
  its window; none -> `ErrMissingAssignedDemand`, today's 422, never zero) and
  records `DemandSource`; an explicit figure is used untouched. Then it resolves the path capacity through
  `GetProcessPathCapacity` (the same read-time composition; each step's
  registered ProcessCapacity must COVER the plan's window (ADR 0003; the
  plan keeps the REQUESTED window, and `capacity_over_window` / shortage use
  its length), and
  stations can bind it), builds the aggregate, saves it
  and queues `CapacityPlanCreated` in the outbox -- one `ports.UnitOfWork.Do`.
  The WorkloadProfile factors arrive in the request body. Assigned demand
  arrives in the request body OR defaults to the expected-demand read model
  (docs/adr/0004, order-management `OrderAllocated` events); this context makes
  no live cross-context lookup either way.
- `PublishCapacityPlan` (Phase 4) — loads the plan, `Publish()`, saves it and
  queues every recorded event in the outbox, in one `UnitOfWork.Do`. The
  Postgres `FindByID` locks the row inside the unit of work, so concurrent
  publishes serialize.

Full phased delivery plan, worked examples (reproduced here as test
fixtures) and acceptance criteria: the maintainer's out-of-repo
`warehouse-planning-bc-plan.md` design doc (cited in ADR 0001; it is not
checked in here).
