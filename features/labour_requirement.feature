Feature: The labour each capacity-plan step requires, from labor-performance's standards
  As a warehouse planner
  I want a capacity plan to state, per step of its path, the labour its demand requires
  So that "short by 4000 orders, bound by REBIN" comes with "PACK needs 300 labour hours"

  # labor-performance publishes LaborStandardDefined / LaborStandardRevised on its
  # INTEGRATION topic (its ADR 0038); warehouse-planning keeps a LOCAL copy of
  # those facts (docs/adr/0022). The standard that prices a step of task type T
  # for a plan of site S and window W is, among task-type-only standards in force
  # at W's start, S's own over the fleet-wide one, then the latest effective_from,
  # then the latest event time. A step's process type names its task type; a
  # step without a standard, or without a task count, is unknown and says why --
  # never a guessed number. PICK tasks are the released order lines when the
  # demand came from the order read model; every other step uses the request's
  # tasks_per_order factor. The requirement is computed at creation and stored
  # with the plan; the ORDER shortage, the bottleneck and every published event
  # are unchanged by it. The standards go through the REAL consumer's
  # HandleMessage; the requests go through the REAL HTTP surface.

  Background:
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at LABOUR-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at LABOUR-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at LABOUR-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK

  Scenario: Legacy unchanged: a plan with no standard-backed step behaves exactly as before
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "LABOUR-ZONE" on path "pick-rebin-pack" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 2.5 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000 and bottleneck REBIN
    And every step's labour requirement is unknown because "no-labor-standard"
    When I publish the capacity plan
    Then the response status is 200
    And the capacity plan is PUBLISHED with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000 and bottleneck REBIN
    And the outbox event types are ProcessCapacityRegistered, ProcessCapacityRegistered, ProcessCapacityRegistered, CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected

  Scenario: Each standard-backed step is priced with the standard in force for the plan's site and window
    Given labor-performance published LaborStandardDefined "std-pack" for task type "PACK" of 90 seconds effective from "2026-10-01T00:00:00Z"
    And labor-performance published LaborStandardDefined "std-pick" for task type "PICK" of 20 seconds effective from "2026-10-01T00:00:00Z"
    And labor-performance published LaborStandardDefined "std-pick-sim1" for task type "PICK" of 12 seconds at site "SIM1" effective from "2026-10-01T00:00:00Z"
    And labor-performance published LaborStandardRevised "std-pick-sim2" for task type "PICK" of 5 seconds at site "SIM2" effective from "2026-10-02T00:00:00Z"
    And labor-performance published LaborStandardRevised "std-pack-later" for task type "PACK" of 60 seconds effective from "2026-10-05T08:00:01Z"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "LABOUR-ZONE" on path "pick-rebin-pack" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 2.5, packages_per_order 1 and tasks_per_order "PICK=3,PACK=1"
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000 and bottleneck REBIN
    And the labour requirement of PICK is 36000 tasks at 12 seconds, 120 labour hours and 15 average heads from the site's own standard
    And the labour requirement of REBIN is unknown because "no-labor-standard"
    And the labour requirement of PACK is 12000 tasks at 90 seconds, 300 labour hours and 37.5 average heads
    And the stored capacity plan states the same labour requirement

  Scenario: A step whose tasks cannot be counted is unknown, not guessed
    Given labor-performance published LaborStandardDefined "std-pack" for task type "PACK" of 90 seconds effective from "2026-10-01T00:00:00Z"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "LABOUR-ZONE" on path "pick-rebin-pack" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 2.5 and packages_per_order 1
    Then the response status is 201
    And the labour requirement of PACK is unknown because "no-task-factor"
    And the labour requirement of PICK is unknown because "no-labor-standard"

  Scenario: PICK tasks are the released order lines when the demand came from orders
    Given labor-performance published LaborStandardDefined "std-pick-sim1" for task type "PICK" of 450 seconds at site "SIM1" effective from "2026-10-01T00:00:00Z"
    And I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And order-management published OrderAllocated for order "ord-1" promised at "2026-10-05T09:00:00Z" with 3 released lines
    And order-management published OrderAllocated for order "ord-2" promised at "2026-10-05T12:00:00Z" with 5 released lines
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" without assigned demand, units_per_order 1 and packages_per_order 1
    Then the response status is 201
    And the capacity plan uses demand 2 from orders with shortage 0
    And the labour requirement of PICK is 8 tasks at 450 seconds, 1 labour hours and 0.125 average heads from the site's own standard

  Scenario: A redelivered or replayed standard changes nothing, and a later revision never rewrites a stored plan
    Given labor-performance published LaborStandardDefined "std-pack" for task type "PACK" of 90 seconds effective from "2026-10-01T00:00:00Z"
    And labor-performance redelivers its last standard
    And labor-performance published LaborStandardRevised "std-pack" for task type "PACK" of 30 seconds effective from "2026-10-02T00:00:00Z"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "LABOUR-ZONE" on path "pick-rebin-pack" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 2.5, packages_per_order 1 and tasks_per_order "PACK=1"
    Then the response status is 201
    And the labour requirement of PACK is 12000 tasks at 90 seconds, 300 labour hours and 37.5 average heads
    Given labor-performance published LaborStandardRevised "std-pack-v2" for task type "PACK" of 60 seconds effective from "2026-10-03T00:00:00Z"
    Then the stored capacity plan states the same labour requirement
    And the labour requirement of PACK is 12000 tasks at 90 seconds, 300 labour hours and 37.5 average heads

  Scenario: A non-positive task factor is rejected with an RFC 7807 problem
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "LABOUR-ZONE" on path "pick-rebin-pack" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 2.5, packages_per_order 1 and tasks_per_order "PACK=0"
    Then the response status is 422
    And the problem detail type is "non-positive-tasks-per-order"
    And the outbox holds no capacity plan events
