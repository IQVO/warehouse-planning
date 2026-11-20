# Derived from: apis/openapi.yaml (POST /process-paths, GET /process-paths/{id}/capacity),
# docs/adr/0025-process-flows-from-process-path-management.md (stage 2: the local
# declaration retired; POST answers 410), docs/adr/0003-window-coverage-semantics.md,
# .claude/rules/domain-model.md, errors.go (problem-type slugs).
@bdd
Feature: ProcessPath registration and path capacity rules
  As a warehouse planner
  I want path capacity to follow the registration, normalization and window-coverage rules
  So that a path's bottleneck is never computed from a wrong, shadowed or un-normalizable figure

  Scenario: The retired local declaration answers 410 for any body
    When I send POST "/process-paths" with body:
      """
      {"id": "broken", "name":
      """
    Then the response status is 410
    And the problem detail type is "process-path-moved"
    When I send POST "/process-paths" with body:
      """
      {"id": "any", "name": "Any", "steps": ["PICK"]}
      """
    Then the response status is 410
    And the problem detail type is "process-path-moved"
    When I list the process paths
    Then the process path list is empty
