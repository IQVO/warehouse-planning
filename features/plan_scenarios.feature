Feature: What-if capacity plan scenarios
  As a warehouse planner
  I want to label several draft plans of one window and list them together
  So that I can lay options next to each other before I publish one

  # docs/adr/0024 decision 2: a plan may carry an optional scenario label (1 to 64
  # characters, stored and returned). Several DRAFT plans may exist for the same
  # (site_id, path_id, window); each is a scenario. GET /capacity-plans filters by
  # site_id, path_id, window_start and window_end additively (a plan matches every
  # filter sent), newest first, next to the location filter it always had. Every
  # request goes through the REAL HTTP surface.

  Background:
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK

  Scenario: Labelled drafts of one window coexist and list by site, path and window
    Given I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8 labelled "baseline"
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with assigned demand 4 labelled "first hour only"
    And I create a capacity plan for warehouse "WH-1" at site "SIM2" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8 labelled "other site"
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 12 labelled "volume 15% higher"
    When I send GET "/capacity-plans?site_id=SIM1&path_id=pick-only&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T10:00:00Z"
    Then the response status is 200
    And the capacity plan list holds the scenarios "volume 15% higher, baseline"
    When I send GET "/capacity-plans?location=SIM1&site_id=SIM1"
    Then the capacity plan list holds the scenarios "volume 15% higher, first hour only, baseline"

  Scenario: A blank scenario label is refused
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8 labelled ""
    Then the response status is 422
    And the problem detail type is "invalid-scenario"
    And the outbox holds no capacity plan events

  Scenario: A malformed window filter is a problem, never an empty list
    When I send GET "/capacity-plans?window_start=yesterday"
    Then the response status is 400
    And the problem detail type is "malformed-window-start"
