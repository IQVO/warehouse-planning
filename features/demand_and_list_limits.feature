# Derived from: apis/openapi.yaml (GET /demand, GET /capacity-plans),
# docs/adr/0004-demand-ingestion-from-order-management.md,
# .claude/rules/domain-model.md (Expected demand (order), CapacityPlan).
@bdd
Feature: Demand request validation and capacity plan list limits
  As a console user and REST client
  I want malformed demand requests and oversized lists to be answered precisely
  So that bad input is never mistaken for "no demand" or an unbounded result

  Scenario: The demand request needs a location and two well-formed, ordered window bounds
    When I send GET "/demand?window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z"
    Then the response status is 400
    And the problem detail type is "missing-location"
    When I send GET "/demand?location=SIM1&window_start=yesterday&window_end=2026-10-05T16:00:00Z"
    Then the response status is 400
    And the problem detail type is "malformed-window-start"
    When I send GET "/demand?location=SIM1&window_start=2026-10-05T08:00:00Z"
    Then the response status is 400
    And the problem detail type is "malformed-window-end"
    When I send GET "/demand?location=SIM1&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T08:00:00Z"
    Then the response status is 400
    And the problem detail type is "invalid-capacity-window"

  Scenario: Plan lists default to 20 plans, cap a stated limit at 100 and echo the location filter
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at LIM-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at LIM-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at LIM-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    And I create 105 capacity plans with assigned demand 1000 at "LIM-ZONE" on path "pick-rebin-pack"
    When I list the capacity plans of every location
    Then the response status is 200
    And the capacity plan list has 20 plans
    And the response field "location" is absent
    When I list the capacity plans for location "LIM-ZONE" with limit "1000"
    Then the capacity plan list has 100 plans
    And the response field "location" is "LIM-ZONE"
    When I list the capacity plans for location "LIM-ZONE" with limit "7"
    Then the capacity plan list has 7 plans
    When I list the capacity plans for location "LIM-OTHER"
    Then the capacity plan list is empty
