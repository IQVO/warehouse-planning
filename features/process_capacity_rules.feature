# Derived from: apis/openapi.yaml (POST/GET /process-capacities),
# .claude/rules/domain-model.md (ProcessCapacity, CapacityWindow),
# internal/adapters/inbound/http/errors.go (problem-type slugs).
@bdd
Feature: ProcessCapacity registration rules
  As a warehouse planner
  I want constraint registration to be an upsert with strict input validation
  So that the effective capacity can never be corrupted by a bad or repeated registration

  Scenario: Re-registering a constraint type replaces its rate instead of adding a second constraint
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at RULES-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 3000 UNIT per HOUR for PICK at RULES-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 201
    And the effective capacity response reports 3000 UNIT per HOUR bound by LABOR
    When I look up the effective capacity for PICK at RULES-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 200
    And the effective capacity response reports 3000 UNIT per HOUR bound by LABOR
    And the effective capacity response lists 1 constraint

  Scenario: A negative quantity is rejected and nothing is registered
    When I register a LABOR constraint of -5 UNIT per HOUR for PICK at RULES-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 422
    And the problem detail type is "negative-quantity"
    When I look up the effective capacity for PICK at RULES-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 404
    And the problem detail type is "process-capacity-not-found"

  Scenario: A period that is not positive is rejected
    When I register a LABOR constraint of 4000 UNIT per 0 seconds for PICK at RULES-ZONE-C for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 422
    And the problem detail type is "non-positive-period"
    When I register a LABOR constraint of 4000 UNIT per -3600 seconds for PICK at RULES-ZONE-C for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 422
    And the problem detail type is "non-positive-period"

  Scenario: A registration with an unusable window is rejected with a 400 problem
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at RULES-ZONE-D for the window "yesterday" to "2026-10-05T09:00:00Z"
    Then the response status is 400
    And the problem detail type is "malformed-window-start"
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at RULES-ZONE-D for the window "2026-10-05T08:00:00Z" to "tomorrow"
    Then the response status is 400
    And the problem detail type is "malformed-window-end"
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at RULES-ZONE-D for the window "2026-10-05T09:00:00Z" to "2026-10-05T08:00:00Z"
    Then the response status is 400
    And the problem detail type is "invalid-capacity-window"
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at RULES-ZONE-D for the window "2026-10-05T08:00:00Z" to "2026-10-05T08:00:00Z"
    Then the response status is 400
    And the problem detail type is "invalid-capacity-window"

  Scenario: The effective capacity lookup is an exact-key read, not a coverage match
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at RULES-ZONE-E for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I look up the effective capacity for PICK at RULES-ZONE-E for the window "2026-10-05T09:00:00Z" to "2026-10-05T10:00:00Z"
    Then the response status is 404
    And the problem detail type is "process-capacity-not-found"
    When I look up the effective capacity for PICK at RULES-ZONE-E for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the response status is 200
    And the effective capacity response reports 4000 UNIT per HOUR bound by LABOR
    When I look up the effective capacity for PICK at RULES-ZONE-E for the window "not-a-time" to "2026-10-05T16:00:00Z"
    Then the response status is 400
    And the problem detail type is "malformed-window-start"
