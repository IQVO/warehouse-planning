---
id: planners-morning
title: "How-to: a planner's morning"
sidebar_label: "How-to: a planner's morning"
---

# How-to: a planner's morning

The requirement side of a shift, end to end: send the workload forecast, commit
the labour plan with derived heads, read it back, see what workforce-management
does with the published plan, and recognise a drift. Decisions:
[ADR 0023](/docs/adr/0023-workload-forecast-and-labour-plan-move-here) (forecast,
labour plan, drift) and, for the per-step labour hours of a capacity plan,
[ADR 0022](/docs/adr/0022-labour-standards-from-labor-performance).

The request and response bodies below are the ones the BDD suite sends and
checks over the real HTTP surface (`features/workload_forecast.feature`,
`features/labour_plan.feature`, `features/labour_plan_drift.feature`), shaped
by the schemas in `apis/openapi.yaml`. Site `SIM1`, shift `S1`, path `PICK`.
The fleet's warehouse-day simulation (`e2e-tests`, `cmd/warehouse-day/sim.go`)
makes the same two calls every simulated morning.

Before you start:

- `cmd/api` is running (`go run ./cmd/api`, see the
  [Quickstart](/docs/overview/quickstart)); the routes are REST only, there is
  no MCP tool and no A2A skill for them (ADR 0023 decision 7).
- The `PUT`s take **no** `Idempotency-Key`: they are naturally idempotent.
- Ids are matched **exactly as sent**. `PICK` and `pick` are two keys, here and
  in the drift against workforce-management's commitments.

## 1. Send the workload forecast

The volume the path is expected to receive, as CPT buckets. One forecast per
`(site_id, path_id)`; a new receipt replaces the buckets as a whole.

```http
PUT /workload-forecasts/SIM1/PICK
Content-Type: application/json

{
  "buckets": [
    {"cpt": "2026-11-19T07:00:00Z", "quantity": 500},
    {"cpt": "2026-11-19T13:59:00Z", "quantity": 300},
    {"cpt": "2026-11-19T14:00:00Z", "quantity": 999}
  ]
}
```

`201 Created` (a first receipt, version 1):

```json
{
  "site_id": "SIM1",
  "path_id": "PICK",
  "buckets": [
    {"cpt": "2026-11-19T07:00:00Z", "quantity": 500},
    {"cpt": "2026-11-19T13:59:00Z", "quantity": 300},
    {"cpt": "2026-11-19T14:00:00Z", "quantity": 999}
  ],
  "total_quantity": 1799,
  "received_at": "2026-11-19T05:00:00Z",
  "version": 1
}
```

`received_at` is the server's receipt time (the value shown is the OpenAPI
example). In the same transaction the outbox gets one
`WorkloadForecastReceived` per topic (integration and analytics), with the
totals, not the buckets:

```json
{
  "site_id": "SIM1",
  "path_id": "PICK",
  "total_quantity": 1799,
  "bucket_count": 3,
  "received_at": "2026-11-19T05:00:00Z",
  "version": 1
}
```

What the next `PUT` does:

| You send | Answer | Event |
| --- | --- | --- |
| the same buckets in the same order (a `cpt` with another offset for the same instant counts as the same, for example `2026-11-19T04:00:00-03:00`) | `200`, version and `received_at` unchanged, nothing written | none |
| other buckets | `200`, version + 1, buckets replaced as a whole | one `WorkloadForecastReceived` |
| a negative quantity | `422 negative-bucket-quantity` | none |
| no bucket | `422 no-forecast-buckets` | none |

Read it back with `GET /workload-forecasts/SIM1/PICK` (`404
workload-forecast-not-found` if nothing was received under those ids).

## 2. Commit the labour plan, heads derived

Omit `planned_heads` and send `shift_start`: the heads are derived from the
forecast due in the shift window `[shift_start, shift_start + hours)`.

```http
PUT /labour-plans/SIM1/S1/PICK
Content-Type: application/json

{
  "rate_units_per_hour": 10,
  "hours": 8,
  "installed_stations": 12,
  "shift_start": "2026-11-19T06:00:00Z"
}
```

The window is `[06:00Z, 14:00Z)`. The 07:00Z and 13:59Z buckets are inside
(800 units); the 14:00Z bucket is at the end, so it belongs to the next shift.
One head covers `10 x 8 = 80` units, so `planned_heads = ceil(800 / 80) = 10`.

`201 Created`:

```json
{
  "site_id": "SIM1",
  "shift_id": "S1",
  "path_id": "PICK",
  "planned_heads": 10,
  "rate_units_per_hour": 10,
  "hours": 8,
  "installed_stations": 12,
  "shift_start": "2026-11-19T06:00:00Z",
  "heads_source": "forecast",
  "version": 1,
  "warnings": []
}
```

`warnings` is empty because the facility-layout tally of `SIM1` knows 12 `PICK`
stations (the feature's background registers 12 work-center slots) and
`installed_stations` is 12. The outbox gets one `LabourPlanCommitted` per
topic, carrying every field of the plan (`subject` and Kafka key
`SIM1/S1/PICK`):

```json
{
  "site_id": "SIM1",
  "shift_id": "S1",
  "path_id": "PICK",
  "planned_heads": 10,
  "rate_units_per_hour": 10,
  "hours": 8,
  "installed_stations": 12,
  "shift_start": "2026-11-19T06:00:00Z",
  "heads_source": "forecast",
  "version": 1
}
```

When the heads cannot be derived the plan is refused, never planned at zero
heads. With a forecast whose only bucket is `2026-11-20T07:00:00Z` (outside the
shift), the same `PUT` answers:

```json
{
  "type": "https://errors.warehouse-planning.warehouse-systems.dev/missing-planned-heads",
  "title": "planned_heads is required when it cannot be derived from the workload forecast",
  "status": 422,
  "detail": "usecases: planned_heads was omitted and cannot be derived: it needs shift_start and a stored workload forecast with at least one bucket in [shift_start, shift_start + hours)",
  "instance": "/labour-plans/SIM1/S1/PICK"
}
```

The same `422` comes back without `shift_start` or without a stored forecast
for `(SIM1, PICK)`. Other refusals: `422 heads-exceed-stations` (13 heads on
12 stations), `422 missing-installed-stations`, `422 non-positive-rate`,
`422 non-positive-hours`. The full list is on
[Troubleshooting](/docs/operations/troubleshooting).

### Sending the heads yourself

Send `planned_heads` and the plan keeps them (`heads_source` `request`):

```json
{"planned_heads": 7, "rate_units_per_hour": 10, "hours": 8, "installed_stations": 12}
```

Committed over the plan above this is a revision: `200`, version 2,
`heads_source` `request`, and no `shift_start` in the answer or the event (it
is omitted, never `null`, when the plan has none). The same body again is
`200` with version 2 and no event.

### The station warning

`installed_stations` is stored as sent. When the tally knows at least one but
fewer stations of the activity named like the path (the path id upper-cased),
the answer carries a warning. With 7 heads and `installed_stations: 15` on a
site that tallies 12 `PICK` stations:

```json
"warnings": [
  "installed stations above the facility tally: installed_stations is 15, the facility-layout tally knows 12 PICK station(s) at SIM1; the plan keeps 15 as sent"
]
```

The warning is never stored: every read recomputes it against the current
tally, and a site with no tallied station gives none.

## 3. Read the plans back

```http
GET /labour-plans/SIM1/S1/PICK
```

answers `200` with the body of step 2 (`404 labour-plan-not-found` for an
unknown key). All the paths of the shift, ordered by path id, one keyset page
at a time:

```http
GET /labour-plans?site_id=SIM1&shift_id=S1
```

```json
{
  "items": [
    {"site_id": "SIM1", "shift_id": "S1", "path_id": "PACK", "...": "..."},
    {"site_id": "SIM1", "shift_id": "S1", "path_id": "PICK", "...": "..."},
    {"site_id": "SIM1", "shift_id": "S1", "path_id": "SORT", "...": "..."}
  ]
}
```

(`"...": "..."` stands for the rest of each plan.) Follow `nextCursor` until it
is absent and repeat `site_id` and `shift_id` on every page; both are required
(`400 missing-site-id`, `400 missing-shift-id`). A plan of another shift (`S2`)
is not listed.

## 4. What workforce-management does with it

workforce-management owns the labour **supply** and keeps a copy of the
requirement (its ADR 0043):

- Its consumer (group `workforce-management-labour-plans`, env
  `LABOUR_PLAN_CONSUMER_GROUP` there) acts only on `LabourPlanCommitted` and
  stores one `labour_requirements` row per `(site_code, shift_id, path_id)`,
  applied only when the message's `version` is greater than the stored one.
- A plan proposal that omits `charge` and names a `shiftId` proposes the copy's
  `planned_heads` (`rateSource` `labour-plan`); no row is `404
  labour-plan-not-found` there, never zero heads.
- Its staffing-gap and committed shift-plan lines show the requirement as
  `requiredHeads`.
- It does **not** refuse a commitment below the requirement: committing fewer
  heads is a supply decision. The commitment comes back here as
  `ShiftPlanCommitted`, one message per path line, for example:

```json
{
  "site_code": "SIM1",
  "shift_id": "S1",
  "path_id": "PICK",
  "planned_heads": 10,
  "planned_rate": 10,
  "planned_hours": 8
}
```

This context registers the LABOR constraint from it (heads x rate, the
capacity side) and, because the line carries a `shift_id`, records the
**committed heads** for `SIM1/S1/PICK` and compares them with the labour plan.

## 5. What a drift looks like

`drift_heads = committed_heads - planned_heads`, positive when
workforce-management committed more heads than the plan needs. It is raised as
`LabourPlanDriftDetected` only when it is non-zero **and** differs from the
last drift recorded for the key; every computed drift is recorded, zero
included. Neither side is ever corrected.

The scenarios of `features/labour_plan_drift.feature`, laid end to end on one
key, with a plan of 7 heads sent by the planner:

| Step | Committed heads | Drift | Raised? |
| --- | --- | --- | --- |
| workforce-management commits (event `wfm-1`) | 10 | +3 | yes |
| the same event `wfm-1` is redelivered | | | no: the CloudEvents id is already claimed |
| it commits again (event `wfm-2`) | 10 | +3 | no: the same drift as last time |
| it commits 7 (event `wfm-3`) | 7 | 0 | no: recorded as 0 |
| it commits 10 again (event `wfm-4`) | 10 | +3 | yes: a drift that cleared and returned |

The event (`subject` and key `SIM1/S1/PICK`):

```json
{
  "site_id": "SIM1",
  "shift_id": "S1",
  "path_id": "PICK",
  "planned_heads": 7,
  "committed_heads": 10,
  "drift_heads": 3
}
```

The other trigger is the plan itself. When the commitment arrived first (10
heads, no plan yet) nothing is compared; the first `PUT` of the plan (7 heads)
then raises the drift of 3. A later revision to 9 heads raises a drift of 1;
the same 9-head `PUT` again raises nothing, because an unchanged plan runs no
comparison. `GET /labour-plans/SIM1/S1/PICK` keeps showing the planner's
heads throughout: the drift is information for the planner, who either revises
the plan or asks workforce-management to commit again.

Why a drift you expected was not raised (a line without `shift_id`, keys that
differ in case, the same drift twice, and the accepted limits) is answered row
by row in
[Troubleshooting: labour plan drift](/docs/operations/troubleshooting#labour-plan-drift-why-was-nothing-raised).

## Where to go next

- [Ubiquitous language: requirement side](/docs/ddd/ubiquitous-language#requirement-side-adr-0022-adr-0023)
- [Sequence diagrams 10 to 12](/docs/ddd/sequence-diagrams) for the units of
  work behind each call
- [Capacity composition: requirement versus capacity](/docs/overview/capacity-composition#requirement-versus-capacity)
- The generated REST reference for `PUT /workload-forecasts/...` and
  `PUT /labour-plans/...` in the [API reference](/docs/api-reference)
