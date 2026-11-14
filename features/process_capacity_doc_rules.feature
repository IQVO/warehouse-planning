# Derived from the warehouse-planning documentation (warehouse-docs
# docs/contexts/warehouse-planning): aggregate-design-canvas (ProcessCapacity:
# State Transitions, Enforced Invariants, Corrective Policies, Created Events),
# ubiquitous-language (Covers, Step composition, Binding constraint),
# domain-events, troubleshooting, plus the repo's .claude/rules/domain-model.md.
# Every scenario names ONE documented rule that no earlier scenario pinned.
@bdd
Feature: ProcessCapacity and step composition, as the aggregate design canvas documents them
  As a warehouse planner
  I want the ProcessCapacity invariants and the read-time step composition to hold at their edges
  So that an effective rate, its binding constraint and its events can be trusted

  # ------------------------------------------------ ProcessCapacity aggregate

  Scenario Outline: A tie in the effective rate goes to the constraint registered first (<first> before <second>)
    When I register a <first> constraint of 4000 UNIT per HOUR for PICK at TIE-<first> for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a <second> constraint of 4000 UNIT per HOUR for PICK at TIE-<first> for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 201
    And the effective capacity response reports 4000 UNIT per HOUR bound by <first>
    And the outbox event types are ProcessCapacityRegistered

    Examples:
      | first     | second    |
      | LABOR     | EQUIPMENT |
      | EQUIPMENT | LABOR     |
      | CONVEYOR  | BUFFER    |

  Scenario: Re-registering a constraint replaces its rate in place and keeps its place in the tie-break order
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at ORDER-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register an EQUIPMENT constraint of 4000 UNIT per HOUR for PICK at ORDER-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the effective capacity response reports 4000 UNIT per HOUR bound by LABOR
    When I register a LABOR constraint of 5000 UNIT per HOUR for PICK at ORDER-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the effective capacity response reports 4000 UNIT per HOUR bound by EQUIPMENT
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at ORDER-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the effective capacity response reports 4000 UNIT per HOUR bound by LABOR
    When I look up the effective capacity for PICK at ORDER-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the effective capacity response reports 4000 UNIT per HOUR bound by LABOR
    And the effective capacity response lists 2 constraints

  Scenario: A rate of exactly zero is a valid constraint, only a negative quantity is refused
    When I register a LABOR constraint of 0 UNIT per HOUR for PICK at ZERO-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 201
    And the effective capacity response reports 0 UNIT per HOUR bound by LABOR
    And the outbox event types are ProcessCapacityRegistered
    And the last ProcessCapacityRegistered event has quantity 0

  Scenario: A step registered at zero throughput makes the path capacity zero and the whole demand a shortage
    Given I register a LABOR constraint of 0 UNIT per HOUR for PICK at ZERO-PLAN for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "ZERO-PLAN" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with assigned demand 500, units_per_order 2.5 and packages_per_order 1
    Then the response status is 201
    And the capacity plan is DRAFT with path capacity 0 ORDER per HOUR, capacity over window 0, shortage 500 and bottleneck PICK
    When I publish the capacity plan
    Then the outbox event types are ProcessCapacityRegistered, CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected

  Scenario: Another unit for an existing constraint type is refused and leaves the aggregate untouched
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at MIX-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 50 PACKAGE per HOUR for PICK at MIX-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 409
    And the problem detail type is "unit-mismatch"
    When I look up the effective capacity for PICK at MIX-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the effective capacity response reports 4000 UNIT per HOUR bound by LABOR
    And the effective capacity response lists 1 constraint
    And the outbox event types are ProcessCapacityRegistered

  Scenario: The native unit belongs to one (process, location, window) aggregate, not to the process
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at WIN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 50 PACKAGE per HOUR for PICK at WIN-ZONE for the window "2026-10-05T09:00:00Z" to "2026-10-05T10:00:00Z"
    Then the response status is 201
    When I register a LABOR constraint of 70 ORDER per HOUR for PICK at WIN-OTHER for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 201
    When I look up the effective capacity for PICK at WIN-ZONE for the window "2026-10-05T09:00:00Z" to "2026-10-05T10:00:00Z"
    Then the effective capacity response reports 50 PACKAGE per HOUR bound by LABOR

  Scenario: All seven constraint types register on one aggregate and only a registration that moves the effective rate raises ProcessCapacityChanged
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at SEVEN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LOCATION constraint of 3500 UNIT per HOUR for PICK at SEVEN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register an EQUIPMENT constraint of 5000 UNIT per HOUR for PICK at SEVEN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a STATION constraint of 3000 UNIT per HOUR for PICK at SEVEN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a CONVEYOR constraint of 3800 UNIT per HOUR for PICK at SEVEN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a BUFFER constraint of 2800 UNIT per HOUR for PICK at SEVEN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a REPLENISHMENT constraint of 900 UNIT per HOUR for PICK at SEVEN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the response status is 201
    And the effective capacity response reports 900 UNIT per HOUR bound by REPLENISHMENT
    When I look up the effective capacity for PICK at SEVEN-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the effective capacity response reports 900 UNIT per HOUR bound by REPLENISHMENT
    And the effective capacity response lists 7 constraints
    And the outbox event types are ProcessCapacityRegistered, ProcessCapacityChanged, ProcessCapacityChanged, ProcessCapacityChanged, ProcessCapacityChanged

  Scenario: A registration that moves only the binding constraint, not the value, still raises ProcessCapacityChanged
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at BIND-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LOCATION constraint of 4000 UNIT per HOUR for PICK at BIND-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the outbox event types are ProcessCapacityRegistered
    When I register a LABOR constraint of 5000 UNIT per HOUR for PICK at BIND-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the effective capacity response reports 4000 UNIT per HOUR bound by LOCATION
    And the outbox event types are ProcessCapacityRegistered, ProcessCapacityChanged
    And the last ProcessCapacityChanged event has constraint_type "LABOR"
    And the last ProcessCapacityChanged event has previous_binding_constraint "LABOR"
    And the last ProcessCapacityChanged event has previous_quantity 4000
    And the last ProcessCapacityChanged event has effective_binding_constraint "LOCATION"
    And the last ProcessCapacityChanged event has effective_quantity 4000
    And the last ProcessCapacityChanged event has effective_unit "UNIT"
    And the last ProcessCapacityChanged event has effective_period_seconds 3600

  Scenario: ProcessCapacity events are keyed by the aggregate's canonical identity and each carries its own CloudEvents id
    When I register a LABOR constraint of 4000 UNIT per HOUR for PICK at KEY-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LOCATION constraint of 3500 UNIT per HOUR for PICK at KEY-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then every ProcessCapacity event is keyed by "PICK/KEY-ZONE/2026-10-05T08:00:00Z/2026-10-05T09:00:00Z"
    And the ProcessCapacityRegistered event has the CloudEvents attribute "type" "com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityRegistered"
    And the ProcessCapacityChanged event has the CloudEvents attribute "dataschema" "urn:warehouse:warehouse-planning:events:ProcessCapacityChanged:v1"
    And the last ProcessCapacityRegistered event has process_type "PICK"
    And the last ProcessCapacityRegistered event has window_end "2026-10-05T09:00:00Z"
    And every queued event has its own CloudEvents id

  Scenario: A unit string is not checked when a constraint is registered, only when it is normalized
    Given I register a LABOR constraint of 40 LINE per HOUR for PICK at LINE-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    Then the response status is 201
    When I look up the effective capacity for PICK at LINE-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the effective capacity response reports 40 LINE per HOUR bound by LABOR
    When I look up the capacity of process path "pick-only" at "LINE-ZONE" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "unsupported-normalization-unit"

  Scenario: Rates are compared per second, so a shorter period is not mistaken for a smaller rate
    When I register a LABOR constraint of 1000 UNIT per 1800 seconds for PICK at PERIOD-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register an EQUIPMENT constraint of 1500 UNIT per HOUR for PICK at PERIOD-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    Then the effective capacity response reports 1500 UNIT per HOUR bound by EQUIPMENT

  # ----------------------------------------------- Window coverage (ADR 0003)

  Scenario Outline: A registered window of 08:00 to 16:00 covers a planning window from <start> to <end>
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at COVER-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I look up the capacity of process path "pick-only" at "COVER-ZONE" for the window "<start>" to "<end>" with units_per_order 2 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 2000 ORDER per HOUR bound by PICK

    Examples:
      | start                | end                  |
      | 2026-10-05T08:00:00Z | 2026-10-05T16:00:00Z |
      | 2026-10-05T08:00:01Z | 2026-10-05T15:59:59Z |

  Scenario Outline: A registered window of 08:00 to 16:00 does not cover a planning window from <start> to <end>
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at COVER-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I look up the capacity of process path "pick-only" at "COVER-ZONE" for the window "<start>" to "<end>" with units_per_order 2 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "missing-step-capacity"

    Examples:
      | start                | end                  |
      | 2026-10-05T07:59:59Z | 2026-10-05T16:00:00Z |
      | 2026-10-05T08:00:00Z | 2026-10-05T16:00:01Z |

  Scenario: Of two covering windows that start together, the narrower one wins a constraint type
    Given I register a LABOR constraint of 1000 UNIT per HOUR for PICK at NARROW-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 2000 UNIT per HOUR for PICK at NARROW-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T12:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I look up the capacity of process path "pick-only" at "NARROW-ZONE" for the window "2026-10-05T09:00:00Z" to "2026-10-05T11:00:00Z" with units_per_order 1 and packages_per_order 1
    Then the step breakdown is "PICK:2000:LABOR"
    When I look up the capacity of process path "pick-only" at "NARROW-ZONE" for the window "2026-10-05T09:00:00Z" to "2026-10-05T14:00:00Z" with units_per_order 1 and packages_per_order 1
    Then the step breakdown is "PICK:1000:LABOR"

  # ------------------------------------------------- Step and path composition

  Scenario Outline: The path bottleneck of two equal steps is the earlier step of the path (<first>, <second>)
    Given I register a LABOR constraint of 1000 ORDER per HOUR for PICK at EQ-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a LABOR constraint of 1000 ORDER per HOUR for PACK at EQ-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "two-steps" named "Two steps" with steps <first>, <second>
    When I look up the capacity of process path "two-steps" at "EQ-ZONE" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 1000 ORDER per HOUR bound by <first>

    Examples:
      | first | second |
      | PICK  | PACK   |
      | PACK  | PICK   |

  Scenario Outline: A step bound by labor <labor> PACKAGE per hour against 10 stations of 180 each is bound by <binding> (ties go to the registered constraint)
    Given facility-layout registered 10 PACK work-center slots in zone "TIE1-OPS-WC"
    And I register a LABOR constraint of <labor> PACKAGE per HOUR for PACK at TIE1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I declare a station standard of 180 PACKAGE per HOUR for PACK at TIE1
    When I look up the capacity of process path "pack-only" at "TIE1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the step breakdown is "PACK:<rate>:<binding>"

    Examples:
      | labor | rate | binding |
      | 1799  | 1799 | LABOR   |
      | 1800  | 1800 | LABOR   |
      | 1801  | 1800 | STATION |

  Scenario: Stations and their standard are enough capacity for a step without any labor registered
    Given facility-layout registered 10 PACK work-center slots in zone "ALONE-OPS-WC"
    And I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I declare a station standard of 180 PACKAGE per HOUR for PACK at ALONE
    When I look up the capacity of process path "pack-only" at "ALONE" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 1800 ORDER per HOUR bound by PACK
    And the step breakdown is "PACK:1800:STATION"
    And the response has 0 warnings

  Scenario: A station standard with no station tallied is not capacity, and stops being once the stations are decommissioned
    Given I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I declare a station standard of 180 PACKAGE per HOUR for PACK at NOSTA
    When I look up the capacity of process path "pack-only" at "NOSTA" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "missing-step-capacity"
    When facility-layout registered 2 PACK work-center slots in zone "NOSTA-OPS-WC"
    And I look up the capacity of process path "pack-only" at "NOSTA" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the step breakdown is "PACK:360:STATION"
    When facility-layout decommissioned 2 slots in zone "NOSTA-OPS-WC"
    And I look up the capacity of process path "pack-only" at "NOSTA" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "missing-step-capacity"

  Scenario Outline: A station standard of <standard> is normalized to orders per hour before it is multiplied by 10 stations
    Given facility-layout registered 10 PACK work-center slots in zone "STD1-OPS-WC"
    And I register a process path "pack-only" named "Pack-Only" with steps PACK
    And I declare a station standard of <standard> for PACK at STD1
    When I look up the capacity of process path "pack-only" at "STD1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the step breakdown is "PACK:<orders>:STATION"

    Examples:
      | standard                    | orders |
      | 3 PACKAGE per 60 seconds    | 1800   |
      | 180 ORDER per 3600 seconds  | 1800   |
      | 90 UNIT per 3600 seconds    | 360    |
