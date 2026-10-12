# Derived from: apis/openapi.yaml (POST /process-paths, GET /process-paths/{id}/capacity),
# docs/adr/0003-window-coverage-semantics.md, .claude/rules/domain-model.md
# (ProcessPath, WorkloadProfile, Step composition), errors.go (problem-type slugs).
@bdd
Feature: ProcessPath registration and path capacity rules
  As a warehouse planner
  I want path capacity to follow the registration, normalization and window-coverage rules
  So that a path's bottleneck is never computed from a wrong, shadowed or un-normalizable figure

  Scenario: A process path without steps is rejected, a malformed body is a 400, and re-registering an id replaces the path wholesale
    When I register a process path "empty" named "Empty" with no steps
    Then the response status is 422
    And the problem detail type is "empty-process-path-steps"
    When I send POST "/process-paths" with body:
      """
      {"id": "broken", "name":
      """
    Then the response status is 400
    And the problem detail type is "malformed-json"
    When I list the process paths
    Then the process path list is empty
    When I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    And I register a process path "pick-rebin-pack" named "Pack only" with steps PACK
    Then the response status is 201
    When I list the process paths
    Then the process path list is "pick-rebin-pack=PACK"

  Scenario: A step's native unit needs its WorkloadProfile factor, and every given factor must be positive
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at FACT-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at FACT-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 100 ORDER per HOUR for GATE at FACT-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I register a process path "gate-only" named "Gate-Only" with steps GATE
    When I send GET "/process-paths/pick-only/capacity?location=FACT-ZONE&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z"
    Then the response status is 422
    And the problem detail type is "missing-conversion-factor"
    When I send GET "/process-paths/pack-only/capacity?location=FACT-ZONE&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z&units_per_order=2.5"
    Then the response status is 422
    And the problem detail type is "missing-conversion-factor"
    When I send GET "/process-paths/pick-only/capacity?location=FACT-ZONE&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z&units_per_order=0"
    Then the response status is 422
    And the problem detail type is "non-positive-conversion-factor"
    When I send GET "/process-paths/pack-only/capacity?location=FACT-ZONE&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z&packages_per_order=-1"
    Then the response status is 422
    And the problem detail type is "non-positive-conversion-factor"
    When I send GET "/process-paths/gate-only/capacity?location=FACT-ZONE&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z"
    Then the response status is 200
    And the process path capacity response reports 100 ORDER per HOUR bound by GATE

  Scenario: A malformed path capacity request is a 400 problem
    Given I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I send GET "/process-paths/pick-only/capacity?location=BAD-ZONE&window_start=yesterday&window_end=2026-10-05T16:00:00Z"
    Then the response status is 400
    And the problem detail type is "malformed-window-start"
    When I send GET "/process-paths/pick-only/capacity?location=BAD-ZONE&window_start=2026-10-05T08:00:00Z&window_end=tomorrow"
    Then the response status is 400
    And the problem detail type is "malformed-window-end"
    When I send GET "/process-paths/pick-only/capacity?location=BAD-ZONE&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z&units_per_order=lots"
    Then the response status is 400
    And the problem detail type is "malformed-units-per-order"
    When I send GET "/process-paths/pick-only/capacity?location=BAD-ZONE&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z&packages_per_order=lots"
    Then the response status is 400
    And the problem detail type is "malformed-packages-per-order"
    When I send GET "/process-paths/pick-only/capacity?location=BAD-ZONE&window_start=2026-10-05T16:00:00Z&window_end=2026-10-05T08:00:00Z"
    Then the response status is 400
    And the problem detail type is "invalid-capacity-window"

  Scenario: Per constraint type the covering window with the latest start wins, and other types of older windows still apply
    Given I register a LABOR constraint of 1000 UNIT per HOUR for PICK at SHAD-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LOCATION constraint of 5000 UNIT per HOUR for PICK at SHAD-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 6000 UNIT per HOUR for PICK at SHAD-ZONE for the window "2026-10-05T10:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I look up the capacity of process path "pick-only" at "SHAD-ZONE" for the window "2026-10-05T10:00:00Z" to "2026-10-05T14:00:00Z" with units_per_order 1 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 5000 ORDER per HOUR bound by PICK
    And the step breakdown is "PICK:5000:LOCATION"
    When I look up the capacity of process path "pick-only" at "SHAD-ZONE" for the window "2026-10-05T08:00:00Z" to "2026-10-05T12:00:00Z" with units_per_order 1 and packages_per_order 1
    Then the step breakdown is "PICK:1000:LABOR"
