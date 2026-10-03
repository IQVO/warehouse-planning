Feature: Effective ProcessCapacity
  As a warehouse planner
  I want the effective capacity of a process at a location and window to be
  the minimum across every registered constraint
  So that I know which constraint is the current bottleneck

  # Byte-for-byte reproduction of the design doc's PICK/PICK-ZONE-A worked
  # example, end to end through the real HTTP surface (chi router, in-memory
  # repository) -- kept forever as a regression fixture.
  Scenario: PICK capacity at PICK-ZONE-A is bound by LOCATION
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PICK-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LOCATION constraint of 3500 UNIT per HOUR for PICK at PICK-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register an EQUIPMENT constraint of 5000 UNIT per HOUR for PICK at PICK-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a CONVEYOR constraint of 3800 UNIT per HOUR for PICK at PICK-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 201
    And the effective capacity response reports 3500 UNIT per HOUR bound by LOCATION
    When I look up the effective capacity for PICK at PICK-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 200
    And the effective capacity response reports 3500 UNIT per HOUR bound by LOCATION
    And the effective capacity response lists 4 constraints

  Scenario: Looking up a process capacity that was never registered
    When I look up the effective capacity for PACK at NOWHERE for the window "2026-01-01T00:00:00Z" to "2026-01-01T01:00:00Z"
    Then the response status is 404
    And the problem detail type is "process-capacity-not-found"

  Scenario: Registering a mismatched unit is rejected
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PICK-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register an EQUIPMENT constraint of 50 PACKAGE per HOUR for PICK at PICK-ZONE-B for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 409
    And the problem detail type is "unit-mismatch"
