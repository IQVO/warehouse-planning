Feature: Publishing a capacity plan supersedes the plan published before it
  As a warehouse planner
  I want publishing a scenario to replace the plan published before it for the same window
  So that exactly one published plan states the capacity of a site, location, path and window

  # docs/adr/0024 decision 3: publishing a plan when another PUBLISHED plan exists for
  # the same (site_id, location, path_id, window) moves that older plan to the terminal
  # status SUPERSEDED and records CapacityPlanSuperseded (subject and key: the older
  # plan) in the same unit of work, before the newer plan's own events. A superseded
  # plan is never published again. Every request goes through the REAL HTTP surface.
  # The path delivers 5 orders per hour: 10 over the two-hour window.

  Background:
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8 labelled "baseline"

  Scenario: Two scenarios of one window published in turn: the second supersedes the first
    Given I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 12 labelled "volume 15% higher"
    When I publish the capacity plan labelled "baseline"
    Then the response status is 200
    And the capacity plan labelled "baseline" is PUBLISHED
    When I publish the capacity plan labelled "volume 15% higher"
    Then the response status is 200
    And the capacity plan labelled "volume 15% higher" is PUBLISHED
    And the capacity plan labelled "baseline" is SUPERSEDED by the plan labelled "volume 15% higher"
    And the outbox event types are ProcessCapacityRegistered, CapacityPlanCreated, CapacityPlanCreated, CapacityPlanPublished, CapacityPlanSuperseded, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected
    And the CapacityPlanSuperseded event names the plan labelled "baseline" superseded by the plan labelled "volume 15% higher" of warehouse "WH-1" at site "SIM1" in location "SIM1"
    When I send GET "/capacity-plans?site_id=SIM1&path_id=pick-only&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T10:00:00Z"
    Then the response status is 200
    And the capacity plan list holds the scenarios "volume 15% higher, baseline"

  Scenario: A superseded plan cannot be published again
    Given I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 9 labelled "second pack line open"
    And I publish the capacity plan labelled "baseline"
    And I publish the capacity plan labelled "second pack line open"
    When I publish the capacity plan labelled "baseline"
    Then the response status is 409
    And the problem detail type is "capacity-plan-superseded"
    And the outbox holds 1 CapacityPlanSuperseded event
    And the outbox holds 2 CapacityPlanPublished events
    And the capacity plan labelled "second pack line open" is PUBLISHED

  Scenario: A plan of another window or another path supersedes nothing
    Given I register a process path "pick-again" named "Pick-Again" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with assigned demand 4 labelled "first hour only"
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-again" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8 labelled "other path"
    When I publish the capacity plan labelled "baseline"
    And I publish the capacity plan labelled "first hour only"
    And I publish the capacity plan labelled "other path"
    Then the response status is 200
    And the capacity plan labelled "baseline" is PUBLISHED
    And the capacity plan labelled "first hour only" is PUBLISHED
    And the capacity plan labelled "other path" is PUBLISHED
    And the outbox holds 0 CapacityPlanSuperseded events
