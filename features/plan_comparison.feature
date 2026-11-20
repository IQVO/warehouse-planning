Feature: Comparing capacity plan scenarios side by side
  As a warehouse planner
  I want to read two to five plans of one site and window next to each other
  So that I can choose the option to publish from their stored figures

  # docs/adr/0024 decision 4: GET /capacity-plans/comparisons?ids=a,b[,c...] takes 2 to
  # 5 distinct ids of plans of one site and one window and returns, per plan and in
  # the order asked, its scenario, status, demand and source, path capacity,
  # capacity over the window, shortage, bottleneck and labour requirement exactly
  # as stored: nothing is recomputed. PICK runs 5 UNIT per HOUR here (10 orders over
  # the window with units_per_order 1). Every request goes through the REAL HTTP
  # surface.

  Background:
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8 labelled "baseline"
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 12 labelled "volume 15% higher"

  Scenario: Two scenarios of one window are laid side by side, as stored, in the order asked
    When I compare the capacity plans "volume 15% higher, baseline"
    Then the response status is 200
    And the comparison holds the scenarios "volume 15% higher, baseline" with shortages "2, 0"

  Scenario: Plans of different windows are not compared
    Given I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with assigned demand 4 labelled "first hour only"
    When I compare the capacity plans "baseline, first hour only"
    Then the response status is 422
    And the problem detail type is "comparison-window-mismatch"

  Scenario: Plans of different sites are not compared
    Given I create a capacity plan for warehouse "WH-1" at site "SIM2" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8 labelled "other site"
    When I compare the capacity plans "baseline, other site"
    Then the response status is 422
    And the problem detail type is "comparison-site-mismatch"

  Scenario: A comparison takes two to five distinct plans
    When I compare the capacity plans "baseline"
    Then the response status is 422
    And the problem detail type is "invalid-comparison-ids"
    When I compare the capacity plans "baseline, baseline"
    Then the response status is 422
    And the problem detail type is "invalid-comparison-ids"

  Scenario: An unknown plan id is not found
    When I compare the capacity plans "baseline, no-such-plan"
    Then the response status is 404
    And the problem detail type is "capacity-plan-not-found"
