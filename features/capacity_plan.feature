Feature: CapacityPlan
  As a warehouse planner
  I want to evaluate the demand assigned to a warehouse window against the
  capacity of a process path
  So that I know the shortage, which step is the bottleneck, and can publish
  the plan (and its shortage/bottleneck events) exactly once

  # The design doc's section-43 worked example, end to end through the real
  # HTTP surface (chi router, in-memory repositories + outbox) -- kept
  # forever as a regression fixture. Path capacity is 1000 ORDER/HOUR (Pick
  # 4000 UNIT/h / 2.5 = 1600, Rebin 2500 UNIT/h / 2.5 = 1000, Pack 1800
  # PACKAGE/h / 1 = 1800); over the 8h window that is 8000 orders, so a
  # demand of 12000 is short by 4000.
  Scenario: 12000 orders over 8h on Pick -> Rebin -> Pack is short by 4000, bound by Rebin
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PLAN-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at PLAN-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at PLAN-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    Then the response status is 201
    When I create a capacity plan for warehouse "WH-1" at "PLAN-ZONE-A" on path "pick-rebin-pack" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 2.5 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000 and bottleneck REBIN
    When I publish the capacity plan
    Then the response status is 200
    And the capacity plan is PUBLISHED with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 4000 and bottleneck REBIN
    And the outbox event types are CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected
    When I publish the capacity plan
    Then the response status is 409
    And the problem detail type is "capacity-plan-already-published"
    And the outbox event types are CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected

  Scenario: 6000 orders over 8h is within capacity, so there is no shortage and no shortage event
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PLAN-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at PLAN-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at PLAN-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    And I create a capacity plan for warehouse "WH-1" at "PLAN-ZONE-B" on path "pick-rebin-pack" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 6000, units_per_order 2.5 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 0 and bottleneck REBIN
    When I publish the capacity plan
    Then the response status is 200
    And the outbox event types are CapacityPlanCreated, CapacityPlanPublished

  Scenario: Creating a plan for a process path that was never registered
    When I create a capacity plan for warehouse "WH-1" at "PLAN-ZONE-C" on path "nowhere-path" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 100, units_per_order 2.5 and packages_per_order 1
    Then the response status is 404
    And the problem detail type is "process-path-not-found"

  Scenario: Creating a plan with negative demand
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PLAN-ZONE-D for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at "PLAN-ZONE-D" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand -5, units_per_order 2.5 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "negative-assigned-demand"

  Scenario: Publishing a capacity plan that does not exist
    When I publish the capacity plan "no-such-plan"
    Then the response status is 404
    And the problem detail type is "capacity-plan-not-found"
