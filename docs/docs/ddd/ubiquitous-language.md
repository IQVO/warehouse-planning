---
id: ubiquitous-language
title: Ubiquitous language
sidebar_position: 1
---

# Ubiquitous language

Use these exact names in code, API and conversation. The source of truth is
`.claude/rules/domain-model.md` in the repository.

| Term | Meaning |
| --- | --- |
| **ProcessCapacity** | The usable throughput of one warehouse process at one location for one time window; the minimum across its registered constraints. |
| **CapacityConstraint** | One named limiting factor (type + rate) attached to a ProcessCapacity: `LABOR`, `LOCATION`, `EQUIPMENT`, `STATION`, `CONVEYOR`, `BUFFER`, `REPLENISHMENT`. |
| **CapacityRate** | A quantity + native unit + period, for example `4000 UNIT / HOUR`. |
| **CapacityWindow** | The `[start, end)` period a capacity is valid for. A capacity with no window is incomplete by definition. A window *covers* another when it starts no later and ends no earlier. |
| **WorkloadProfile** | Per-warehouse conversion factors used to normalize different processes' native rates into one comparable flow unit (ORDER). |
| **ProcessPath** | An ordered sequence of process types a workload flows through (for example Pick, Rebin, Pack). Locally owned by this context. |
| **ProcessPathCapacity** | The normalized end-to-end throughput of a ProcessPath: the minimum of its steps, plus the bottleneck step and its binding constraint type. |
| **StationStandard** | Operator-declared throughput of ONE station of a process at a site. |
| **Station count** | How many work-center stations `facility-layout` tallied for an activity across a site's zones. Has no throughput of its own. |
| **Site / location** | A planning `location` is a site (building) code such as `SIM1`; also the first dash-separated segment of that site's facility zone ids. |
| **CapacityPlan** | Assigned demand for a location and window, compared with the ProcessPathCapacity; yields shortage and bottleneck. |
| **Shortage** | `max(0, assigned demand - capacity over window)`, in orders. |
| **Bottleneck** | The constraint or process-path step currently limiting end-to-end flow. |

## Vocabulary only (not implemented)

`ProcessCapacityRegistered` (a native constraint was registered) and
`ProcessCapacityChanged` (the effective rate changed) exist as domain-model
vocabulary. Nothing raises or publishes them yet.
