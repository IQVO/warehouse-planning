Feature: Station capacity composed at read time
  As a warehouse planner
  I want the stations facility-layout tallied to limit a process step together
  with its labor, through an operator-declared throughput per station
  So that "10 stations x 180 packages/hour/station = 1,800 packages/hour"
  (design doc section 19) can be the binding constraint of a path or a plan,
  while storage positions stay a read model (they are not process throughput)

  # The facility-layout stream is fed through the REAL consumer
  # (HandleMessage over the in-memory tally), then everything else goes through
  # the real HTTP surface. Site SIM1 owns the zones SIM1-OPS-WC (work center,
  # PACK) and SIM1-STOR-AMB (storage, SimShelf): the zone id's first
  # dash-separated segment is the site code, which is the planning location.
  Background:
    Given facility-layout registered 10 PACK work-center slots in zone "SIM1-OPS-WC"
    And facility-layout registered 24 storage slots of type "SimShelf" in zone "SIM1-STOR-AMB"

  # Fixtures A + B. Pick 8000 UNIT/h -> 3200, Rebin 2500 UNIT/h -> 1000,
  # Pack LABOR 2500 PACKAGE/h but 10 stations x 180 = 1800 PACKAGE/h -> 1800
  # (units_per_order 2.5, packages_per_order 1). Rebin bottlenecks the path at
  # 1000 (section 31 unchanged); raised to 6000 UNIT/h (2400) the PACK stations
  # bind it at 1800, and a plan's shortage is bound by STATION.
  Scenario: The pack stations bind the path once rebin is raised
    When I register a LABOR constraint of 8000 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2500 PACKAGE per HOUR for PACK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    And I declare a station standard of 180 PACKAGE per HOUR for PACK at SIM1
    Then the response status is 201
    When I look up the capacity of process path "pick-rebin-pack" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 1000 ORDER per HOUR bound by REBIN
    And the step breakdown is "PICK:3200:LABOR,REBIN:1000:LABOR,PACK:1800:STATION"
    And the response has 0 warnings
    When I register a LABOR constraint of 6000 UNIT per HOUR for REBIN at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I look up the capacity of process path "pick-rebin-pack" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 1800 ORDER per HOUR bound by PACK
    And the step breakdown is "PICK:3200:LABOR,REBIN:2400:LABOR,PACK:1800:STATION"
    When I create a capacity plan for warehouse "WH-1" at "SIM1" on path "pick-rebin-pack" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 20000, units_per_order 2.5 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 1800 ORDER per HOUR, capacity over window 14400, shortage 5600 and bottleneck PACK
    And the capacity plan is bound by STATION
    And the response has 0 warnings

  # Fixture C: stations are tallied but nobody declared what a station can do.
  # No throughput is invented: the step uses labor only and says so; declaring
  # the standard later takes effect on the very next read (nothing is stored).
  Scenario: Stations without a declared standard warn and use labor only, until the standard is declared
    When I register a LABOR constraint of 2500 PACKAGE per HOUR for PACK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I look up the capacity of process path "pack-only" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 2500 ORDER per HOUR bound by PACK
    And the step breakdown is "PACK:2500:LABOR"
    And the response has 1 warning
    And a warning mentions "no station standard declared for PACK at SIM1"
    When I declare a station standard of 180 PACKAGE per HOUR for PACK at SIM1
    And I look up the capacity of process path "pack-only" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the process path capacity response reports 1800 ORDER per HOUR bound by PACK
    And the step breakdown is "PACK:1800:STATION"
    And the response has 0 warnings

  Scenario: A step with stations but neither a standard nor labor has no capacity data
    When I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I look up the capacity of process path "pack-only" at "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "missing-step-capacity"

  Scenario: Declaring a station standard twice replaces it; invalid ones are rejected
    When I declare a station standard of 180 PACKAGE per HOUR for PACK at SIM1
    Then the response status is 201
    When I declare a station standard of 200 PACKAGE per HOUR for PACK at SIM1
    Then the response status is 200
    When I declare a station standard of 0 PACKAGE per HOUR for PACK at SIM1
    Then the response status is 422
    And the problem detail type is "non-positive-station-standard"
    When I declare a station standard of 180 LINE per HOUR for PACK at SIM1
    Then the response status is 422
    And the problem detail type is "unsupported-normalization-unit"

  # Storage positions are a read model, not a throughput: counts per zone and
  # locationType, stations per zone and activity, for the zones of one site.
  Scenario: The storage capacity read model lists the positions and stations of the site
    Given facility-layout registered 3 PACK work-center slots in zone "SIM2-OPS-WC"
    When I look up the storage capacity of "SIM1"
    Then the response status is 200
    And the storage capacity lists 10 PACK stations in zone "SIM1-OPS-WC"
    And the storage capacity lists 24 SimShelf storage positions in zone "SIM1-STOR-AMB"

  Scenario: A site with nothing tallied has an empty storage capacity, not an error
    When I look up the storage capacity of "NOWHERE"
    Then the response status is 200
    And the storage capacity lists nothing
