Feature: The workload forecast of a process path at a site
  As a warehouse planner
  I want to send the volume a process path is expected to receive, as CPT buckets
  So that the shift labour plan can derive the heads it needs from it

  # docs/adr/0023 decision 1: one forecast per (site_id, path_id), moved here from
  # wes-work-planning's charge forecast. A receipt replaces the buckets as a whole.
  # A receipt whose buckets equal the stored ones (same order) changes nothing: no
  # event, same version. The PUT is naturally idempotent and takes no
  # Idempotency-Key. Every request goes through the REAL HTTP surface.

  Scenario: A first receipt is stored at version 1 and raises WorkloadForecastReceived
    When I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-11-19T07:00:00Z=500, 2026-11-19T12:00:00Z=300"
    Then the response status is 201
    And the workload forecast is at version 1 with total quantity 800
    And the outbox holds 1 WorkloadForecastReceived event

  Scenario: A different receipt replaces the buckets as a whole
    Given I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-11-19T07:00:00Z=500, 2026-11-19T12:00:00Z=300"
    When I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-11-19T09:00:00Z=40"
    Then the response status is 200
    And the workload forecast is at version 2 with total quantity 40
    And the outbox holds 2 WorkloadForecastReceived events

  Scenario: An identical receipt changes nothing and raises no event
    Given I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-11-19T07:00:00Z=500, 2026-11-19T12:00:00Z=300"
    When I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-11-19T04:00:00-03:00=500, 2026-11-19T12:00:00Z=300"
    Then the response status is 200
    And the workload forecast is at version 1 with total quantity 800
    And the outbox holds 1 WorkloadForecastReceived event

  Scenario: A negative bucket quantity is refused
    When I receive a workload forecast for path "PICK" at site "SIM1" with buckets "2026-11-19T07:00:00Z=-1"
    Then the response status is 422
    And the problem detail type is "negative-bucket-quantity"
    And the outbox holds 0 WorkloadForecastReceived events
