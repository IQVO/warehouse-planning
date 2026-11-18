---
description: "ADR 0025: process-path-management's ProcessFlow (an ordered sequence of process-path stages) replaces the process paths declared in this context; a version-guarded local copy feeds the path capacity, local declaration retires through a strangler, and the Separate Ways edge of ADR 0001 becomes a customer/supplier edge with an anti-corruption layer."
---

# ADR 0025: Process flows come from process-path-management

## Status

Accepted (2026-11-18). Last of the four requirement-side decisions (ADR 0022 to ADR 0025).
Producer side: process-path-management ADR 0042 (the `ProcessFlow` aggregate). Supersedes the
"Separate Ways" relationship with process-path-management recorded in the Addendum of
[ADR 0001](0001-warehouse-planning-bounded-context.md) (2026-10-03) and amends hard rule 5 of
`CLAUDE.md`. Implemented after ADR 0022 to ADR 0024.

## Context

This context's `ProcessPath` is an ordered list of process types (`PICK -> REBIN -> PACK`)
declared here with `POST /process-paths`. The ADR 0001 Addendum chose "Separate Ways" because
process-path-management published no step sequence: its `ProcessPath` is ONE stage of work
(`PICK`, `PACK`, `REBIN`, `SLAM`), with routing metadata (`required_capabilities`,
`eligibility`, `intake`), and the two contexts share only the `path_id` string.

That leaves the topology of the floor (which stage feeds which) declared in a planning
context, while the catalogue of stages lives in the context extracted so that the fleet has
one definition of a path. process-path-management's `intake` (its ADR 0034) already says that
PACK, REBIN and SLAM are fed by PICK's output, but not in which order a flow passes through
them. The sequence is topology, not a planning decision.

## Decision

1. **Local copy of process flows.** A consumer of `warehouse.process-path-management.events`
   acts on `ProcessFlowDefined`, `ProcessFlowRevised` and `ProcessFlowRetired`
   (`com.warehouse.wes.process-path-management.processflow.<Event>`, process-path-management
   ADR 0042) and keeps `process_flows`, one row per flow id with its ordered stages. Each
   event is full state and carries the aggregate `version`; a message is applied only when
   its version is greater than the stored one, so replay and reordering are harmless. Group
   id from a new required env var `PROCESS_FLOW_CONSUMER_GROUP`; claim and apply in one unit
   of work.
2. **The path capacity reads flows.** A flow's id is the `path_id` plans and path-capacity
   reads use; its steps are the stages' `process_type` values in order. Composition
   (ADR 0002, ADR 0003) is unchanged.
3. **Local declaration retires through a strangler.**
   - Stage 1: both sources are read. A flow wins over a locally declared path with the same
     id; local paths without a flow keep working and are reported with `source: local`.
   - Stage 2, once every local path in the cluster has a flow (checked live, not assumed):
     `POST /process-paths` answers `410 process-path-moved` naming process-path-management's
     route. `GET /process-paths` stays: the console remote lists paths with it.
   - Stage 3: the local table and declaration code are removed by a later change.
4. **Relationship.** process-path-management becomes an upstream Open Host Service with a
   Published Language; this context is a downstream customer with an anti-corruption layer
   (the consumer translates stages into steps). Still no live call.

## Consequences

- One owner for the floor's topology; planning, and any later consumer, read the same flows.
- Flows reference stages by `path_id`, so a stage rename or deactivation is visible to every
  flow that uses it (process-path-management refuses to deactivate a stage an active flow
  uses, its ADR 0042).
- A third consumer group on this context and a new required env var (infra values first).
- Until stage 2, two sources coexist; the `source` field makes it visible which one a plan
  used.

## Alternatives rejected

- **Keep Separate Ways.** The topology stays in a planning context and drifts from the
  catalogue it names.
- **Derive the sequence from `intake`.** `intake` says which demand may enter a stage, not
  the order stages are visited; deriving an order from it would be an invented rule.
- **Copy process-path-management's stage catalogue and keep declaring flows here.** The
  sequence would still be owned by the wrong context.
