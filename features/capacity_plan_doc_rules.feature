# Derived from the warehouse-planning documentation (warehouse-docs
# docs/contexts/warehouse-planning): aggregate-design-canvas (CapacityPlan:
# Enforced Invariants, Corrective Policies, Created Events), domain-events
# (payloads, attributes, order), troubleshooting (REST problem types and the
# "capacity answers that look wrong" rows) and the repo's rest-api.md.
@bdd
Feature: CapacityPlan lifecycle, arithmetic and published events, as the aggregate design canvas documents them
  As a warehouse planner
  I want a plan's arithmetic, lifecycle and announcements to follow the documented rules exactly
  So that the shortage, the events downstream contexts consume and the retry behaviour are never a surprise

  Background:
    Given I register a LABOR constraint of 1000 ORDER per HOUR for PICK at PLAN-DOC for the window "2026-10-05T08:00:00Z" to "2026-10-06T08:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK

  # ------------------------------------------------------------- arithmetic

  Scenario Outline: A window ending at <end> gives a capacity over the window of <capacity> and a shortage of <shortage> for 2000 orders
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-DOC" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "<end>" with assigned demand 2000, units_per_order 1 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window <capacity>, shortage <shortage> and bottleneck PICK

    Examples:
      | end                  | capacity | shortage |
      | 2026-10-05T08:30:00Z | 500      | 1500     |
      | 2026-10-05T09:30:00Z | 1500     | 500      |
      | 2026-10-05T10:00:00Z | 2000     | 0        |
      | 2026-10-06T08:00:00Z | 24000    | 0        |

  Scenario: The capacity over the window follows the rate's period, not just its quantity
    Given I register a LABOR constraint of 900 UNIT per 1800 seconds for PICK at PLAN-HALF for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-HALF" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 4000, units_per_order 1 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1800 ORDER per HOUR, capacity over window 3600, shortage 400 and bottleneck PICK

  # --------------------------------------------------------------- lifecycle

  # Publishing the second supersedes the first (docs/adr/0024 decision 3): both
  # have the same site, location, path and window.
  Scenario: Creating the same plan twice makes two plans, and publishing one leaves the other a draft
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-DOC" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 9000, units_per_order 1 and packages_per_order 1
    And I note the capacity plan as "first"
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-DOC" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 9000, units_per_order 1 and packages_per_order 1
    And I note the capacity plan as "second"
    Then the noted capacity plans "first" and "second" are different plans
    When I list the capacity plans for location "PLAN-DOC"
    Then the capacity plan list has 2 plans
    When I publish the noted capacity plan "first"
    Then the response status is 200
    And the noted capacity plan "first" is PUBLISHED
    And the noted capacity plan "second" is DRAFT
    When I publish the noted capacity plan "first"
    Then the response status is 409
    And the problem detail type is "capacity-plan-already-published"
    When I publish the noted capacity plan "second"
    Then the response status is 200
    And the noted capacity plan "first" is SUPERSEDED
    And the noted capacity plan "second" is PUBLISHED
    And the outbox event types are ProcessCapacityRegistered, CapacityPlanCreated, CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected, CapacityPlanSuperseded, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected

  Scenario: Retrying a plan creation with the same Idempotency-Key answers the first answer and creates no second plan
    When I send POST "/capacity-plans" with the Idempotency-Key "plan-retry-1" and body:
      """
      {"warehouse_id": "WH-1", "site_id": "SIM1", "location": "PLAN-DOC", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 9000, "units_per_order": 1}
      """
    Then the response status is 201
    And the response is not an idempotent replay
    When I send POST "/capacity-plans" with the Idempotency-Key "plan-retry-1" and body:
      """
      {"warehouse_id": "WH-1", "site_id": "SIM1", "location": "PLAN-DOC", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 9000, "units_per_order": 1}
      """
    Then the response status is 201
    And the response is an idempotent replay
    And the response is the first answer given for the Idempotency-Key "plan-retry-1"
    When I list the capacity plans for location "PLAN-DOC"
    Then the capacity plan list has 1 plans
    And the outbox event types are ProcessCapacityRegistered, CapacityPlanCreated

  Scenario: An Idempotency-Key reused for another request is a conflict and a missing key is refused, both before anything is created
    When I send POST "/capacity-plans" with the Idempotency-Key "plan-retry-2" and body:
      """
      {"warehouse_id": "WH-1", "site_id": "SIM1", "location": "PLAN-DOC", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 9000, "units_per_order": 1}
      """
    Then the response status is 201
    When I send POST "/capacity-plans" with the Idempotency-Key "plan-retry-2" and body:
      """
      {"warehouse_id": "WH-1", "site_id": "SIM1", "location": "PLAN-DOC", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 9001, "units_per_order": 1}
      """
    Then the response status is 409
    And the problem detail type is "idempotency-key-reused"
    When I send POST "/capacity-plans" without an Idempotency-Key and body:
      """
      {"warehouse_id": "WH-1", "site_id": "SIM1", "location": "PLAN-DOC", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 9000, "units_per_order": 1}
      """
    Then the response status is 400
    And the problem detail type is "idempotency-key-required"
    When I list the capacity plans for location "PLAN-DOC"
    Then the capacity plan list has 1 plans

  # ---------------------------------------------------------- demand source

  Scenario: An explicit assigned_demand of zero is stated demand, not an omitted one, even when orders are expected
    Given order-management published 12 orders promised at "2026-10-05T09:00:00Z"
    When I send POST "/capacity-plans" with body:
      """
      {"warehouse_id": "WH-1", "site_id": "SIM1", "location": "PLAN-DOC", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T10:00:00Z", "path_id": "pick-only", "assigned_demand": 0, "units_per_order": 1}
      """
    Then the response status is 201
    And the capacity plan uses demand 0 from request with shortage 0
    And the response field "demand_source" is "request"

  Scenario: Expected demand is read for the plan's own location, so another location has no data and is refused
    Given I register a LABOR constraint of 1000 ORDER per HOUR for PICK at SIM2 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And order-management published 12 orders promised at "2026-10-05T09:00:00Z"
    When I create a capacity plan for warehouse "WH-1" at site "SIM2" in location "SIM2" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" without assigned demand, units_per_order 1 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "missing-assigned-demand"
    And the outbox holds no capacity plan events

  # ----------------------------------------------------------------- events

  Scenario Outline: The <event> event is a CloudEvents 1.0 structured event of the documented type, source and data schema
    Given I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-DOC" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 1 and packages_per_order 1
    And I publish the capacity plan
    Then the <event> event has the CloudEvents attribute "specversion" "1.0"
    And the <event> event has the CloudEvents attribute "type" "com.warehouse.wes.warehouse-planning.capacityplan.<event>"
    And the <event> event has the CloudEvents attribute "source" "/warehouse/warehouse-planning"
    And the <event> event has the CloudEvents attribute "dataschema" "urn:warehouse:warehouse-planning:events:<event>:v1"
    And every queued event has its own CloudEvents id

    Examples:
      | event                    |
      | CapacityPlanCreated      |
      | CapacityPlanPublished    |
      | CapacityShortageDetected |
      | BottleneckDetected       |

  Scenario: CapacityPlanCreated carries the status and no publication time, CapacityPlanPublished the opposite
    Given I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-DOC" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 1 and packages_per_order 1
    And I publish the capacity plan
    Then the CapacityPlanCreated event of the plan has status "DRAFT"
    And the CapacityPlanCreated event of the plan has no field "published_at"
    And the CapacityPlanPublished event of the plan has no field "status"
    And the CapacityPlanPublished event of the plan has capacity_over_window 8000
    And the CapacityShortageDetected event of the plan has no field "path_capacity"
    And the BottleneckDetected event of the plan has no field "shortage"

  # ------------------------------------------- blank identifying fields

  # troubleshooting.md, rest-api.md, ADR 0012 and the openapi: a blank
  # warehouse_id, site_id, location or path_id is 422 missing-required-field.
  # The use case checks those fields before it resolves the demand or the path
  # capacity, so a blank location is never a missing-step-capacity and a blank
  # path_id is never a process-path-not-found.
  @bdd
  Scenario: A plan with a blank location is a missing-required-field problem
    When I send POST "/capacity-plans" with body:
      """
      {"warehouse_id": "WH-1", "site_id": "SIM1", "location": "", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 100, "units_per_order": 1}
      """
    Then the response status is 422
    And the problem detail type is "missing-required-field"

  @bdd
  Scenario: A plan with a blank path_id is a missing-required-field problem
    When I send POST "/capacity-plans" with body:
      """
      {"warehouse_id": "WH-1", "site_id": "SIM1", "location": "PLAN-DOC", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "", "assigned_demand": 100, "units_per_order": 1}
      """
    Then the response status is 422
    And the problem detail type is "missing-required-field"
