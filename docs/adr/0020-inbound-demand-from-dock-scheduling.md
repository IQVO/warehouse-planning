# ADR 0020: Inbound demand is read from dock-scheduling's appointment events

## Status

Accepted (2026-11-05). Companion of the fleet decision YD-0002 (the
`DockAppointment` aggregate moves from `inbound-receiving` to the new
`dock-scheduling` context, which owns door time for both directions; strangler
stage D in this repo). Supersedes the **source** of the inbound labour demand
decided in [ADR 0017](./0017-inbound-labour-demand-and-observed-capacity-feedback.md)
Part A: its decisions 1 (the consumer and its topic), 3 (site attribution of
appointments), 5 (the ordering rule) and the appointment half of 12 and 13 (the
env var and the payload checks). Everything else in ADR 0017 stands: the labour
standard (decision 2), the window semantics (4), "no standard means no demand"
(6), the plan integration (7), and all of Part B (observed capacity). The labour
a dock appointment costs is unchanged. No CloudEvents type this context
publishes, no topic it produces to and no request shape changes; one REST
response value changes (see Consequences).

## Context

### What ADR 0017 had to live with

ADR 0017 read `DockAppointmentBooked` and `DockAppointmentCancelled` from
`inbound-receiving` and recorded three limitations of that source:

- **No site.** Neither event carried one, and parsing a door code prefix is the
  inference ADR 0012 forbids. Every appointment was therefore attributed to the
  ONE configured site, `DEMAND_SITE_ID`.
- **No revision.** The only ordering fact was the CloudEvents `time`, so the
  guard was "last writer wins on `time`, and `Cancelled` wins a tie".
- **Inbound only.** The source context knew only inbound appointments.

### What the new producer carries

`dock-scheduling` publishes on `warehouse.dock-scheduling.events` (key and
`subject` = the appointment id, `type`
`com.warehouse.wms.dock-scheduling.dockappointment.<EventName>`, snake_case
`data`). The contract is pinned by the yard-management plan
(`contracts-delta.md`, which mirrors the model) and documented in this repo's
`apis/asyncapi.yaml`; the producer's own asyncapi was not yet on its `develop`
when this was written, so the consumer is exercised against fixtures built from
the pinned contract and must be re-checked against the producer's published
asyncapi when it lands. The four events this context uses:

| event | `data` fields relied on |
| --- | --- |
| `DockAppointmentBooked` | `appointment_id`, `site_id`, `direction` (`INBOUND` or `OUTBOUND`), `window_start`, `window_end`, `asn_numbers` (at least one when `INBOUND`), `version` |
| `DockAppointmentRescheduled` | `appointment_id`, `site_id`, `window_start`, `window_end`, `version` |
| `DockAppointmentCancelled` | `appointment_id`, `site_id`, `version` |
| `DockAppointmentNoShow` | `appointment_id`, `site_id`, `direction`, `version` |

`version` is the aggregate version: it starts at 1 and grows by 1 per accepted
change. Every event carries `site_id`, a facility-layout `siteCode`. The other
appointment events (checked in, door reassigned, completed, at risk) are
analytics-only on the producer and never reach this topic; its door calendar
events do, and are ignored. `Cancelled` and `NoShow` are terminal on the
producer (a late carrier needs a new booking).

Two facts are NOT in every event and are not invented: `Rescheduled` carries no
direction and no ASNs; `Cancelled` carries neither; `NoShow` carries the
direction but no window and no ASNs.

## Decision

1. **The source switches.** A consumer (`DockAppointmentConsumer`, replacing
   `InboundDemandConsumer`) reads `warehouse.dock-scheduling.events` and acts on
   exactly four FULL type strings:
   `com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentBooked`,
   `...DockAppointmentRescheduled`, `...DockAppointmentCancelled` and
   `...DockAppointmentNoShow`. Every other type on the topic is ignored,
   including `com.warehouse.wms.inbound-receiving.dockappointment.*`: the old
   types are NOT consumed any more, and `warehouse.inbound-receiving.events` is
   no longer read for appointments. Still Kafka only, no REST/MCP call to a
   sibling, local read model `inbound_appointments`.
2. **Only INBOUND appointments are demand.** `direction` is stored with the row
   and `inbound.Appointment.CountsIn` and the SQL both require `INBOUND`.
   `OUTBOUND` appointments are stored, not counted: storing them lets a later
   event of the same id be told apart from an inbound one.
3. **Demand is attributed by the event's `site_id`.** This closes ADR 0017's
   no-site limitation. The consumer no longer takes a location; the
   `Location` field of the old consumer, which existed only because the events
   had no site, is gone. `GET /inbound-demand?location=` and a plan's
   `location` read the appointments of that site code. `DEMAND_SITE_ID` is
   unchanged for what still needs it, the order-demand and observed-capacity
   consumers (their events still carry no site), and is no longer used for
   appointments.
4. **The window follows the reschedule; cancel and no-show remove the demand.**
   A `Rescheduled` replaces the row's window (so the demand moves to the window
   the new start falls in, by ADR 0017 decision 4's half-open rule). A
   `Cancelled` flips the row to `CANCELLED` and a `NoShow` to `NO_SHOW`; neither
   is demand, and both keep the window already known so the row still says what
   was removed.
5. **The ordering rule is the aggregate `version`, and it replaces ADR 0017
   decision 5.** `inbound.Resolve(prev, next)`:
   - the STATE (status, window, version, site, event time) is written only when
     `next.version > prev.version`: a redelivery, a duplicate or an older event
     changes nothing, and nothing depends on the CloudEvents `time` any more;
   - the booking FACTS (direction, ASN count) are filled in from whichever
     event carries them while the row lacks them, and a winning `Booked`
     restates them. So a `Booked` (version 1) that arrives after a `Rescheduled`
     (version 2) completes the row and does NOT move the window back; one that
     arrives after a `Cancelled` or `NoShow` does not resurrect it;
   - an event that arrives before its `Booked` is stored as a PARTIAL row (a
     `Rescheduled`: the new window, direction unknown; a `Cancelled`: a
     tombstone). A partial row is not demand until the direction is known to be
     `INBOUND`, which only a `Booked` or a `NoShow` supplies.
   The decision is one pure function shared by the in-memory and the Postgres
   adapters (the Postgres one under a per-id advisory lock, so two first writes
   serialise), and the events converge to the same demand in every arrival order
   (a permutation test proves it for the domain, the Postgres integration test
   for the adapter). The window of a removed appointment is informational and
   may differ with the arrival order; the demand never does.
6. **Idempotency, atomicity and DLQ are ADR 0017's.** The processed-event claim
   (CloudEvents `id`, consumer name `dock-appointment-consumer`: new, so a claim
   made under the retired source can never swallow an event of the same id) and
   the write are ONE `UnitOfWork.Do`. The consumer applies a sibling's events and
   raises no event of its own, so there is no loop. A message that is not
   CloudEvents 1.0, or a known type with an unusable payload (malformed `data`,
   blank appointment id or `site_id`, `subject` different from the appointment
   id, an unknown or missing direction, a window whose end is not after its
   start, no ASN on an `INBOUND` booking, no `version`, no event time) is
   dead-lettered at once to `warehouse.dock-scheduling.events.dlq`; transient
   failures keep the bounded retry.
7. **Migration `0012_dock_scheduling_appointments`.** `inbound_appointments`
   gains `direction` (`INBOUND`/`OUTBOUND`, nullable until known) and `version`
   (default 0), `status` gains `NO_SHOW`, and the checks that demanded a window
   and an ASN on every `BOOKED` row are relaxed to what a partial row needs.
   Rows written by the retired source become `INBOUND` at version 0, so any
   dock-scheduling event for the same appointment id supersedes them (the
   producer's legacy importer keeps appointment ids, strangler stage A); they
   keep the site they were stored under until then. The down migration restores
   the old shape (OUTBOUND and unknown-direction booked rows are dropped, NO_SHOW
   becomes CANCELLED).
8. **The old source is rejected at boot, loudly (fleet rule).** The variable that
   selected it, `INBOUND_DEMAND_CONSUMER_GROUP`, is REFUSED by `cmd/api` when
   set, with an error that names it and its replacement, whether or not the new
   variable is also set. The chart refuses `inbound.consumerGroup` and an
   `extraEnv` entry of that name at render time. A stale deployment therefore
   fails at rollout instead of running with an unread topic and an empty model.
9. **New configuration.** `DOCK_APPOINTMENT_CONSUMER_GROUP` is required, has no
   default and is always on (ADR 0016); chart value
   `dockAppointments.consumerGroup`, in `values.yaml`, `deployment.yaml` and the
   render test's everything-set. `warehouse-infra` pins
   `warehouse-planning-dock-appointments`. A new group starts from the earliest
   retained message, so a fresh deployment rebuilds the model from history.

## Consequences

- The inbound demand is attributed to the site each appointment really belongs
  to; one deployment can serve several sites without a configured one for
  appointments. The labour per appointment, the window rule, the standard, the
  plan integration and every number a caller already got are unchanged for the
  same set of INBOUND appointments.
- **One REST/MCP value changes:** `GET /inbound-demand` and `get_inbound_demand`
  report `source: "dock-scheduling"` (was `"inbound-receiving"`), because the
  label would otherwise lie. The OpenAPI enum and the generated reference change
  with it. No client in this repo (console `web/`, MCP, A2A) reads the field.
- **Deployment ordering.** The new image does not boot (and the chart does not
  render) with the old variable, and does not boot without the new one, so
  `warehouse-infra`'s values change (rename the key, new group name) is a
  separate, live change that must go out with it. Until `dock-scheduling`
  produces to its topic the model is empty; rows from the retired source keep
  counting (at version 0) until the producer's importer republishes them.
 
- A partial row (an event that arrived before its `Booked`) is invisible to the
  demand until the `Booked` arrives. A deployment whose `Booked` was lost would
  never count that appointment; the dead-letter topic is where it would show.
- The consumer-group name, the consumer type and the `processed_events` consumer
  name change; `INBOUND_DEMAND_CONSUMER_GROUP`'s committed offsets on the retired
  topic are simply abandoned.
- The producer's own asyncapi is the contract of record; the fixtures in the
  unit, BDD and integration tests mirror the pinned contract and should be
  re-checked against it when it is published on its `develop`.

## Explicit non-goals

- No change to the labour standard, the window rule, `inbound_average_heads`, the
  plan's stored `inbound_*` columns, the warning text or any event this context
  publishes.
- No consumption of the analytics-only appointment events (`CheckedIn`,
  `DoorReassigned`, `Completed`, `AtRisk`), of door closures or operating hours,
  or of `yard-management`'s facts: door time is the only input.
- No demand for OUTBOUND appointments (outbound loading labour is a separate
  question), and no per-direction or per-door breakdown.
- No dual-read period with `inbound-receiving` and no fallback to it.
- ADR 0017's Part B (observed capacity) and its `OBSERVED_CAPACITY_CONSUMER_GROUP`
  are untouched.
