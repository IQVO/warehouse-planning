# Derived from: apis/openapi.yaml (POST /capacity-plans, POST /capacity-plans/{id}/publish,
# GET /capacity-plans/{id}), docs/adr/0012-canonical-site-id-on-capacity-plans.md,
# docs/adr/0007-outbox-and-resilient-consumers.md, .claude/rules/domain-model.md
# (CapacityPlan, Domain events), .claude/rules/integration-events.md.
@bdd
Feature: CapacityPlan rules, events and validation
  As a warehouse planner
  I want plan creation to validate everything it is given and to publish exactly what happened
  So that a rejected request leaves nothing behind and an accepted plan tells its consumers the truth

  Background:
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PLAN-RULES for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK

  Scenario: Demand exactly equal to the capacity is not a shortage, one order more is, and an explicit zero is used as stated
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 8000, units_per_order 4 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 0 and bottleneck PICK
    When I publish the capacity plan
    Then the outbox event types are ProcessCapacityRegistered, CapacityPlanCreated, CapacityPlanPublished
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 8001, units_per_order 4 and packages_per_order 1
    Then the capacity plan is DRAFT with path capacity 1000 ORDER per HOUR, capacity over window 8000, shortage 1 and bottleneck PICK
    When I publish the capacity plan
    Then the outbox event types are ProcessCapacityRegistered, CapacityPlanCreated, CapacityPlanPublished, CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 0, units_per_order 4 and packages_per_order 1
    Then the response status is 201
    And the capacity plan uses demand 0 from request with shortage 0

  Scenario: The plan is scoped to the stated site and its published events name the plan and the shortage
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 12000, units_per_order 4 and packages_per_order 1
    Then the response status is 201
    And the response field "site_id" is "SIM1"
    And the response field "location" is "PLAN-RULES"
    When I publish the capacity plan
    Then the outbox event types are ProcessCapacityRegistered, CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected
    And the CapacityPlanCreated event of the plan has status "DRAFT"
    And the CapacityPlanPublished event of the plan has site_id "SIM1"
    And the CapacityPlanPublished event of the plan has shortage 4000
    And the CapacityShortageDetected event of the plan has assigned_demand 12000
    And the CapacityShortageDetected event of the plan has capacity_over_window 8000
    And the BottleneckDetected event of the plan has bottleneck_step "PICK"
    And the BottleneckDetected event of the plan has path_capacity 1000

  Scenario: Rejected plan requests leave neither a plan nor an event behind
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "no-such-path" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 100, units_per_order 4 and packages_per_order 1
    Then the response status is 404
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "NOWHERE" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 100, units_per_order 4 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "missing-step-capacity"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand -1, units_per_order 4 and packages_per_order 1
    Then the response status is 422
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pick-only" for the window "yesterday" to "2026-10-05T16:00:00Z" with assigned demand 100, units_per_order 4 and packages_per_order 1
    Then the response status is 400
    And the problem detail type is "malformed-window-start"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pick-only" for the window "2026-10-05T16:00:00Z" to "2026-10-05T08:00:00Z" with assigned demand 100, units_per_order 4 and packages_per_order 1
    Then the response status is 400
    And the problem detail type is "invalid-capacity-window"
    When I send POST "/capacity-plans" with body:
      """
      {"warehouse_id": "WH-1", "site_id":
      """
    Then the response status is 400
    And the problem detail type is "malformed-json"
    When I send POST "/capacity-plans" with body:
      """
      {"warehouse_id": "WH-1", "location": "PLAN-RULES", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 100, "units_per_order": 4}
      """
    Then the response status is 422
    And the problem detail type is "missing-required-field"
    When I send POST "/capacity-plans" with body:
      """
      {"warehouse_id": "WH-1", "site_id": "", "location": "PLAN-RULES", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 100, "units_per_order": 4}
      """
    Then the response status is 422
    And the problem detail type is "missing-required-field"
    When I send POST "/capacity-plans" with body:
      """
      {"warehouse_id": "", "site_id": "SIM1", "location": "PLAN-RULES", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "path_id": "pick-only", "assigned_demand": 100, "units_per_order": 4}
      """
    Then the response status is 422
    And the problem detail type is "missing-required-field"
    And the outbox holds no capacity plan events
    When I list the capacity plans of every location
    Then the capacity plan list is empty

  Scenario: A stored plan carries the composition warnings raised when it was created
    Given facility-layout registered 10 PACK work-center slots in zone "PLAN-RULES-OPS"
    And I register a LABOR constraint of 2500 PACKAGE per HOUR for PACK at PLAN-RULES for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pack-only" named "Pack-Only" with steps PACK
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pack-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 100, units_per_order 4 and packages_per_order 1
    Then the response status is 201
    When I get the current capacity plan
    Then the response status is 200
    And the response has 1 warning
    And a warning mentions "no station standard declared for PACK at PLAN-RULES"
    And the capacity plan is bound by LABOR

  Scenario: Reading a plan shows it as DRAFT until it is published, and an unknown id is a 404
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "PLAN-RULES" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 100, units_per_order 4 and packages_per_order 1
    And I get the current capacity plan
    Then the response field "status" is "DRAFT"
    And the response field "published_at" is absent
    When I publish the capacity plan
    And I get the current capacity plan
    Then the response field "status" is "PUBLISHED"
    And the response field "published_at" is present
    When I get the capacity plan "no-such-plan"
    Then the response status is 404
    And the problem detail type is "capacity-plan-not-found"
