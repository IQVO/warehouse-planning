# Derived from: apis/openapi.yaml (PUT/GET /station-standards, GET /storage-capacity),
# docs/adr/0002-station-capacity-composition.md,
# .claude/rules/domain-model.md (StationStandard, Station count, Site / location),
# internal/adapters/inbound/kafka/storage_capacity_consumer.go (consumer contract).
@bdd
Feature: Station standards and the facility tally lifecycle
  As a warehouse planner
  I want station standards to be listable and validated, and the station tally to follow facility-layout
  So that the derived STATION capacity is always count x standard of the stations that really exist

  Scenario: Declared station standards are listed by location then process type, optionally for one site
    When I list the station standards
    Then the response status is 200
    And the station standards list is ""
    When I declare a station standard of 200 PACKAGE per HOUR for PACK at SIM2
    And I declare a station standard of 90 UNIT per HOUR for PICK at SIM1
    And I declare a station standard of 180 PACKAGE per HOUR for PACK at SIM1
    And I list the station standards
    Then the response status is 200
    And the station standards list is "SIM1/PACK=180 PACKAGE, SIM1/PICK=90 UNIT, SIM2/PACK=200 PACKAGE"
    When I list the station standards of "SIM1"
    Then the response field "location" is "SIM1"
    And the station standards list is "SIM1/PACK=180 PACKAGE, SIM1/PICK=90 UNIT"

  Scenario: Invalid station standard declarations are rejected and nothing is stored
    When I declare a station standard of 180 PACKAGE per 0 seconds for PACK at SIM1
    Then the response status is 422
    And the problem detail type is "non-positive-period"
    When I send PUT "/station-standards/SIM1/PACK" with body:
      """
      {"quantity": -180, "unit": "PACKAGE", "period_seconds": 3600}
      """
    Then the response status is 422
    And the problem detail type is "negative-quantity"
    When I send PUT "/station-standards/SIM1/PACK" with body:
      """
      {"quantity": 180, "unit": "PACKAGE", "period_seconds": 3600
      """
    Then the response status is 400
    And the problem detail type is "malformed-json"
    When I list the station standards
    Then the station standards list is ""

  Scenario: Decommissioned stations stop counting at the next read, and an unknown decommission changes nothing
    Given facility-layout registered 10 PACK work-center slots in zone "SIM1-OPS-WC"
    And I register a LABOR constraint of 2500 PACKAGE per HOUR for PACK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I declare a station standard of 180 PACKAGE per HOUR for PACK at SIM1
    When I look up the capacity of process path "pack-only" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the step breakdown is "PACK:1800:STATION"
    When facility-layout decommissioned 4 slots in zone "SIM1-OPS-WC"
    And facility-layout decommissioned the slot "NEVER-SEEN-001"
    And I look up the capacity of process path "pack-only" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 1080 ORDER per HOUR bound by PACK
    And the step breakdown is "PACK:1080:STATION"
    When I look up the storage capacity of "SIM1"
    Then the storage capacity lists 6 PACK stations in zone "SIM1-OPS-WC"

  Scenario: A slot announced twice is tallied once, whether the event is redelivered or re-announced under a new id
    Given facility-layout announced storage slot "SIM1-STOR-AMB-A1" of type "SimShelf" in zone "SIM1-STOR-AMB" as event "slot-evt-1"
    And facility-layout announced storage slot "SIM1-STOR-AMB-A1" of type "SimShelf" in zone "SIM1-STOR-AMB" as event "slot-evt-1"
    And facility-layout announced storage slot "SIM1-STOR-AMB-A1" of type "SimShelf" in zone "SIM1-STOR-AMB" as event "slot-evt-2"
    And facility-layout announced storage slot "SIM1-STOR-AMB-A2" of type "SimShelf" in zone "SIM1-STOR-AMB" as event "slot-evt-3"
    When I look up the storage capacity of "SIM1"
    Then the response status is 200
    And the storage capacity lists 2 SimShelf storage positions in zone "SIM1-STOR-AMB"

  Scenario: A site only sees the zones whose id starts with its own code and a dash
    Given facility-layout registered 2 PACK work-center slots in zone "SIM1-OPS-WC"
    And facility-layout registered 3 PACK work-center slots in zone "SIM10-OPS-WC"
    And I register a LABOR constraint of 2500 PACKAGE per HOUR for PACK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I declare a station standard of 100 PACKAGE per HOUR for PACK at SIM1
    When I look up the storage capacity of "SIM1"
    Then the storage capacity lists 2 PACK stations in zone "SIM1-OPS-WC"
    And the storage capacity lists no zone "SIM10-OPS-WC"
    When I look up the capacity of process path "pack-only" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the step breakdown is "PACK:200:STATION"

  Scenario: The storage capacity read model requires a location
    When I look up the storage capacity of ""
    Then the response status is 400
    And the problem detail type is "missing-location"
