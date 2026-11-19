Feature: The shift labour plan of a process path at a site
  As a warehouse planner
  I want to commit the heads a process path needs for a shift, or have them derived from the forecast
  So that workforce-management receives one published requirement per site, shift and path

  # docs/adr/0023 decision 2: one plan per (site_id, shift_id, path_id), moved here from
  # wes-work-planning's path plan with its invariants (planned_heads <= installed_stations,
  # hours > 0, a finite rate x heads x hours). Omitted planned_heads are derived as
  # ceil(forecast units with a cpt in [shift_start, shift_start + hours) / (rate x hours)),
  # which needs shift_start and a stored forecast with at least one bucket in the shift:
  # zero buckets is no data, never zero heads (422 missing-planned-heads). A PUT that
  # changes nothing raises no event and keeps the version. When the facility tally knows
  # fewer stations of the activity named like the path than installed_stations, the plan
  # is stored with the number as sent and carries a warning.

  Background:
    Given facility-layout registered 12 PICK work-center slots in zone "SIM1-OPS-WC"

  Scenario: Sent heads are stored as the request's and raise LabourPlanCommitted
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    Then the response status is 201
    And the labour plan has 7 planned heads from "request" at version 1
    And the response has 0 warnings
    And the outbox holds 1 LabourPlanCommitted event

  Scenario: Omitted heads are derived from the forecast due in the shift
    Given I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-11-19T07:00:00Z=500, 2026-11-19T13:59:00Z=300, 2026-11-19T14:00:00Z=999"
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" without planned heads, rate 10 units per hour, 8 hours, 12 installed stations and shift start "2026-11-19T06:00:00Z"
    Then the response status is 201
    And the labour plan has 10 planned heads from "forecast" at version 1
    And the outbox holds 1 LabourPlanCommitted event

  Scenario: Omitted heads with no forecast bucket in the shift are refused, never zero
    Given I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-11-20T07:00:00Z=500"
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" without planned heads, rate 10 units per hour, 8 hours, 12 installed stations and shift start "2026-11-19T06:00:00Z"
    Then the response status is 422
    And the problem detail type is "missing-planned-heads"
    And the outbox holds 0 LabourPlanCommitted events

  Scenario: Heads above the installed stations are refused
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 13 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    Then the response status is 422
    And the problem detail type is "heads-exceed-stations"
    And the outbox holds 0 LabourPlanCommitted events

  Scenario: An identical PUT keeps the version and raises no event
    Given I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    Then the response status is 200
    And the labour plan has 7 planned heads from "request" at version 1
    And the outbox holds 1 LabourPlanCommitted event

  Scenario: More installed stations than the facility tally knows is a warning, not a correction
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 15 installed stations
    Then the response status is 201
    And the response has 1 warning
    And a warning mentions "installed_stations is 15, the facility-layout tally knows 12 PICK station(s) at SIM1"

  Scenario: The plans of a shift are listed by path id
    Given I commit a labour plan for path "SORT" in shift "S1" at site "SIM1" with 1 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 1 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And I commit a labour plan for path "PACK" in shift "S1" at site "SIM1" with 1 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And I commit a labour plan for path "PICK" in shift "S2" at site "SIM1" with 1 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    When I list the labour plans of shift "S1" at site "SIM1"
    Then the response status is 200
    And the labour plan list holds the paths "PACK, PICK, SORT"

  # One canonical process-path code (trimmed, upper case) keys the plan and its forecast
  # read: a path in any case names one plan, stored and published in upper case.
  Scenario: A plan PUT as "pick" is read as "PICK" and revised as "Pick"
    Given I commit a labour plan for path "pick" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And I commit a labour plan for path "Pick" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    When I send GET "/labour-plans/SIM1/S1/PICK"
    Then the response status is 200
    And the response path id is "PICK"
    And the labour plan has 7 planned heads from "request" at version 1
    And the outbox holds 1 LabourPlanCommitted event

  Scenario: A forecast received as "Pick" derives the heads of a plan PUT as "PICK"
    Given I receive a workload forecast for path "Pick" at site "SIM1" with buckets "2026-11-19T07:00:00Z=500, 2026-11-19T13:59:00Z=300"
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" without planned heads, rate 10 units per hour, 8 hours, 12 installed stations and shift start "2026-11-19T06:00:00Z"
    Then the response status is 201
    And the response path id is "PICK"
    And the labour plan has 10 planned heads from "forecast" at version 1
