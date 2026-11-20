Feature: Capacity plans from the workload forecast
  As a warehouse planner
  I want a capacity plan to take its demand from the workload forecast
  So that I can plan a window before order-management has allocated its orders

  # docs/adr/0024 decision 1: demand_source forecast. The assigned demand in ORDERS is
  # the units of the WorkloadForecast of (site_id, forecast_path_id) whose bucket cpt is
  # in [window_start, window_end), divided by units_per_order and rounded UP.
  # forecast_path_id defaults to the plan's path_id; both are read in their canonical
  # form (trimmed, upper case). No bucket in the window is no data, never zero demand.
  # PICK runs 5 UNIT per HOUR here: with units_per_order 2 that is 5 orders over the
  # two-hour window, with units_per_order 1 it is 10. Every request goes through the
  # REAL HTTP surface.

  Background:
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK

  Scenario: The buckets in the window are the demand; window_start is in, window_end is out, orders round up
    Given I receive a workload forecast for path "pick-only" at site "SIM1" with buckets "2026-10-05T08:00:00Z=7, 2026-10-05T09:30:00Z=4, 2026-10-05T10:00:00Z=50, 2026-10-05T07:59:59Z=9"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" from the forecast with units_per_order 2
    Then the response status is 201
    And the capacity plan uses demand 6 from forecast with shortage 1
    And the capacity plan read the forecast of path "PICK-ONLY"
    And the stored capacity plan remembers its demand source is forecast

  Scenario: forecast_path_id names the forecast of another path than the plan's
    Given I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-10-05T09:00:00Z=30"
    And I receive a workload forecast for path "pick-only" at site "SIM1" with buckets "2026-10-05T09:00:00Z=2"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" from the forecast of path "pick" with units_per_order 1
    Then the response status is 201
    And the capacity plan uses demand 30 from forecast with shortage 20
    And the capacity plan read the forecast of path "PICK"

  Scenario: No bucket in the window is missing demand, never zero demand
    Given I receive a workload forecast for path "pick-only" at site "SIM1" with buckets "2026-10-05T10:00:00Z=50"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" from the forecast with units_per_order 2
    Then the response status is 422
    And the problem detail type is "missing-assigned-demand"
    And the outbox holds no capacity plan events

  Scenario: Forecast units are never read as orders without units_per_order
    Given I receive a workload forecast for path "pick-only" at site "SIM1" with buckets "2026-10-05T09:00:00Z=8"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" from the forecast without units_per_order
    Then the response status is 422
    And the problem detail type is "missing-units-per-order"
    And the outbox holds no capacity plan events
