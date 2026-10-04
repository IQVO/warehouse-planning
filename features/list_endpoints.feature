Feature: Listing process paths and capacity plans
  As the console remote (and any REST client)
  I want to discover the registered process paths and the most recent capacity plans
  So that an operator can pick a path, review plans and publish one without knowing ids up front

  # Read-only, REST only (no MCP tool). Both lists answer 200 with an empty
  # array when there is nothing, never null and never an error.

  Scenario: No process paths registered yet
    When I list the process paths
    Then the response status is 200
    And the process path list is empty

  Scenario: Registered process paths are listed by id, steps in declared order
    When I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    And I register a process path "a-pack-only" named "Pack only" with steps PACK
    And I list the process paths
    Then the response status is 200
    And the process path list is "a-pack-only=PACK, pick-rebin-pack=PICK>REBIN>PACK"

  Scenario: No capacity plans for a location
    When I list the capacity plans for location "LIST-NOWHERE"
    Then the response status is 200
    And the capacity plan list is empty

  Scenario: Plans are listed newest first, per location, and a published plan shows as PUBLISHED
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at LIST-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at LIST-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at LIST-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 4000 UNIT per HOUR for PICK at LIST-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at LIST-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at LIST-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    When I create a capacity plan with assigned demand 1000 at "LIST-ZONE-A" on path "pick-rebin-pack"
    And I publish the capacity plan
    And I create a capacity plan with assigned demand 2000 at "LIST-ZONE-B" on path "pick-rebin-pack"
    And I create a capacity plan with assigned demand 12000 at "LIST-ZONE-A" on path "pick-rebin-pack"
    And I list the capacity plans for location "LIST-ZONE-A"
    Then the response status is 200
    And the capacity plan list has assigned demands 12000, 1000
    And listed plan 1 is DRAFT
    And listed plan 2 is PUBLISHED
    When I list the capacity plans of every location
    Then the capacity plan list has assigned demands 12000, 2000, 1000
    When I list the capacity plans for location "LIST-ZONE-A" with limit "1"
    Then the capacity plan list has assigned demands 12000

  Scenario: A limit that is not a positive integer is rejected
    When I list the capacity plans for location "LIST-ZONE-A" with limit "0"
    Then the response status is 400
    And the problem detail type is "malformed-limit"
    When I list the capacity plans for location "LIST-ZONE-A" with limit "many"
    Then the response status is 400
    And the problem detail type is "malformed-limit"
