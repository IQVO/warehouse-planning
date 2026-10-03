Feature: ProcessPathCapacity
  As a warehouse planner
  I want the normalized, end-to-end capacity of a process path to be the
  minimum of its steps' effective capacities after WorkloadProfile
  normalization
  So that I know which step is the bottleneck across the whole path

  # Byte-for-byte reproduction of the design doc's Pick -> Rebin -> Pack
  # worked example, end to end through the real HTTP surface (chi router,
  # in-memory repositories) -- kept forever as a regression fixture.
  Scenario: Pick -> Rebin -> Pack path capacity is bound by Rebin
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    Then the response status is 201
    When I look up the capacity of process path "pick-rebin-pack" at "PATH-ZONE-A" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 1000 ORDER per HOUR bound by REBIN

  Scenario: Looking up the capacity of a process path that was never registered
    When I look up the capacity of process path "nowhere-path" at "PATH-ZONE-A" for the window "2026-01-01T00:00:00Z" to "2026-01-01T01:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 404

  Scenario: Looking up the capacity of a process path with a step missing capacity data
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PATH-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK, REBIN
    Then the response status is 201
    When I look up the capacity of process path "pick-only" at "PATH-ZONE-B" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 422
