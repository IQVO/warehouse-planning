---
id: use-cases
title: Use cases
sidebar_position: 2
---

# Use cases

Application services in `internal/application/usecases`. REST and MCP call the
same use cases.

| Use case | Kind | What it does |
| --- | --- | --- |
| `RegisterProcessCapacityConstraint` | command | Upserts one constraint on a ProcessCapacity aggregate, recomputes and persists the effective rate. |
| `RegisterProcessPath` | command | Declares (or wholesale replaces) a ProcessPath: id, name and an ordered, non-empty step list. |
| `GetProcessPathCapacity` | query | Resolves each step's covering ProcessCapacity aggregates, the site's tallied station counts and declared StationStandards, plus the WorkloadProfile; returns the normalized path capacity, bottleneck step and binding constraint, per-step breakdown and warnings. |
| `DeclareStationStandard` | command | Validates and upserts a StationStandard; reports created versus replaced. |
| `GetStorageCapacity` | query | The facility tally of a site as a read model (storage positions per zone and location type, stations per zone and activity). |
| `CreateCapacityPlan` | command | Resolves the path capacity through the same read-time composition, builds the aggregate, saves it and queues `CapacityPlanCreated` in the outbox, in one unit of work. |
| `PublishCapacityPlan` | command | Loads the plan, publishes it, saves it and queues every recorded event in the outbox, in one unit of work. The Postgres lookup locks the row inside the unit of work, so concurrent publishes serialize. |

Plain reads without a use case (a repository port is called directly):
`GetEffectiveProcessCapacity` (`GET /process-capacities`),
`GET /capacity-plans/{id}` and `GET /station-standards`.

:::note Demand ingestion (docs/adr/0004)
`assigned_demand` is optional on the path-capacity and capacity-plan
endpoints: an explicit value in the request body always wins, and when
omitted it is resolved from the expected-demand read model fed by
order-management's published `OrderAllocated`/`OrderPartiallyAllocated`
events (`GetExpectedDemand`). There is still no live cross-context lookup
-- the read model is populated by `OrderDemandConsumer`, never by a
synchronous call. WorkloadProfile factors (units/packages per order)
still arrive in the request body; no WorkloadProfile persistence exists
yet.
:::
