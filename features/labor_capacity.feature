# Derived from: docs/adr/0001-warehouse-planning-bounded-context.md (Addendum:
# workforce-management ShiftPlanCommitted), docs/adr/0007-outbox-and-resilient-consumers.md,
# .claude/rules/fleet/idempotency-and-outbox.md,
# internal/adapters/inbound/kafka/labor_capacity_consumer.go (consumer contract).
@bdd
Feature: Labor capacity from committed shift plans
  As a warehouse planner
  I want the shift plans workforce-management commits to become LABOR capacity
  So that plans are evaluated against the labor that is actually staffed

  # workforce-management publishes one ShiftPlanCommitted per path line of a
  # committed shift plan. The CloudEvent time is the start of the shift; the
  # window is [time, time + planned_hours); the LABOR rate is
  # planned_heads x planned_rate UNIT per HOUR for the upper-cased path_id at
  # the building. The events go through the REAL consumer's HandleMessage; the
  # results are read through the REAL HTTP surface.

  Scenario: A committed shift plan registers a LABOR constraint for its building, path and shift window
    Given workforce-management committed shift plan "shift-1" for building "SIM1" on path "pick" with 10 heads at 400 per head per hour for 8 hours starting "2026-10-05T08:00:00Z"
    When I look up the effective capacity for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the response status is 200
    And the effective capacity response reports 4000 UNIT per HOUR bound by LABOR
    And the effective capacity response lists 1 constraint

  Scenario: Redelivering a shift plan event does not apply it twice
    Given workforce-management committed shift plan "shift-1" for building "SIM1" on path "pick" with 10 heads at 400 per head per hour for 8 hours starting "2026-10-05T08:00:00Z"
    And workforce-management committed shift plan "shift-1" for building "SIM1" on path "pick" with 20 heads at 400 per head per hour for 8 hours starting "2026-10-05T08:00:00Z"
    When I look up the effective capacity for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the effective capacity response reports 4000 UNIT per HOUR bound by LABOR
    And the effective capacity response lists 1 constraint

  Scenario: A later shift plan for the same building and window replaces the LABOR rate
    Given workforce-management committed shift plan "shift-1" for building "SIM1" on path "pick" with 10 heads at 400 per head per hour for 8 hours starting "2026-10-05T08:00:00Z"
    And workforce-management committed shift plan "shift-2" for building "SIM1" on path "pick" with 5 heads at 400 per head per hour for 8 hours starting "2026-10-05T08:00:00Z"
    When I look up the effective capacity for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the effective capacity response reports 2000 UNIT per HOUR bound by LABOR
    And the effective capacity response lists 1 constraint

  Scenario: Committed labor drives the path capacity and the shortage of a plan
    Given workforce-management committed shift plan "shift-1" for building "SIM1" on path "pick" with 10 heads at 400 per head per hour for 8 hours starting "2026-10-05T08:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I look up the capacity of process path "pick-only" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 2000 ORDER per HOUR bound by PICK
    And the step breakdown is "PICK:2000:LABOR"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 20000, units_per_order 2 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 2000 ORDER per HOUR, capacity over window 16000, shortage 4000 and bottleneck PICK
    And the capacity plan is bound by LABOR

  Scenario: Shift plan events the consumer cannot use are ignored and register nothing
    Given workforce-management cancelled shift plan "shift-x" for building "SIM1" on path "pick" starting "2026-10-05T08:00:00Z"
    And workforce-management committed shift plan "shift-y" without a building on path "pick" starting "2026-10-05T08:00:00Z"
    And workforce-management committed shift plan "shift-z" for building "SIM1" on path "pick" with 10 heads at 400 per head per hour for 0 hours starting "2026-10-05T08:00:00Z"
    And the workforce topic carries a message that is not a CloudEvent
    When I look up the effective capacity for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the response status is 404
    And the problem detail type is "process-capacity-not-found"
