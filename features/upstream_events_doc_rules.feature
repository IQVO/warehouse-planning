# Derived from the warehouse-planning documentation (warehouse-docs
# docs/contexts/warehouse-planning): domain-events "Consumed" (fields used,
# effect, ignored on purpose), aggregate-design-canvas "Corrective Policies"
# (a ShiftPlanCommitted that fails a rule is a deterministic rejection, logged,
# claimed and never retried), eventstorming (policies), troubleshooting
# (Kafka ingestion) and .claude/rules/domain-model.md (Expected demand).
# Every event is handed to the REAL consumer's HandleMessage; a consumer that
# returns an error would be retried, so each step also proves the event was
# acknowledged.
@bdd
Feature: Policies driven by upstream events, as the documentation lists them
  As a warehouse planner
  I want upstream facts to be applied exactly once, and a fact that can never be applied to be dropped for good
  So that a poison event never blocks a partition and a redelivery never double counts

  # ------------------------------------------------ ShiftPlanCommitted policy

  Scenario Outline: A shift plan with <flaw> is dropped for good: acknowledged, nothing registered, nothing published
    Given workforce-management delivered a ShiftPlanCommitted as event "shift-bad" starting "2026-10-05T08:00:00Z" with data <data>
    When I look up the effective capacity for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the response status is 404
    And the problem detail type is "process-capacity-not-found"
    And the outbox is empty

    Examples:
      | flaw                    | data                                                                                                 |
      | a negative planned rate | {"building_id":"SIM1","path_id":"pick","planned_heads":10,"planned_rate":-400,"planned_hours":8}     |
      | negative planned heads  | {"building_id":"SIM1","path_id":"pick","planned_heads":-3,"planned_rate":400,"planned_hours":8}      |
      | negative planned hours  | {"building_id":"SIM1","path_id":"pick","planned_heads":10,"planned_rate":400,"planned_hours":-8}     |
      | heads that are no number | {"building_id":"SIM1","path_id":"pick","planned_heads":"ten","planned_rate":400,"planned_hours":8}  |
      | no building             | {"path_id":"pick","planned_heads":10,"planned_rate":400,"planned_hours":8}                           |
      | no path                 | {"building_id":"SIM1","planned_heads":10,"planned_rate":400,"planned_hours":8}                       |

  Scenario: A shift plan whose unit contradicts what the aggregate already holds is acknowledged and changes nothing
    Given I register an EQUIPMENT constraint of 50 PACKAGE per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    When workforce-management committed shift plan "shift-unit" for building "SIM1" on path "pick" with 10 heads at 400 per head per hour for 8 hours starting "2026-10-05T08:00:00Z"
    And I look up the effective capacity for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the response status is 200
    And the effective capacity response reports 50 PACKAGE per HOUR bound by EQUIPMENT
    And the effective capacity response lists 1 constraint
    And the outbox event types are ProcessCapacityRegistered

  Scenario Outline: A shift of 6.5 hours from 08:00 covers a planning window ending at <end> with status <status>
    Given workforce-management committed shift plan "shift-half" for building "SIM1" on path "pick" with 10 heads at 400 per head per hour for 6.5 hours starting "2026-10-05T08:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I look up the capacity of process path "pick-only" at "SIM1" for the window "2026-10-05T08:00:00Z" to "<end>" with units_per_order 1 and packages_per_order 1
    Then the response status is <status>

    Examples:
      | end                  | status |
      | 2026-10-05T14:30:00Z | 200    |
      | 2026-10-05T14:31:00Z | 422    |

  Scenario: Each shift is its own window, so two shifts never merge into one longer one
    Given workforce-management committed shift plan "shift-early" for building "SIM1" on path "pick" with 10 heads at 400 per head per hour for 8 hours starting "2026-10-05T08:00:00Z"
    And workforce-management committed shift plan "shift-late" for building "SIM1" on path "pick" with 5 heads at 400 per head per hour for 8 hours starting "2026-10-05T16:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    When I look up the effective capacity for PICK at SIM1 for the window "2026-10-05T16:00:00Z" to "2026-10-06T00:00:00Z"
    Then the effective capacity response reports 2000 UNIT per HOUR bound by LABOR
    When I look up the capacity of process path "pick-only" at "SIM1" for the window "2026-10-05T12:00:00Z" to "2026-10-05T20:00:00Z" with units_per_order 1 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "missing-step-capacity"

  # ---------------------------------------------- LocationSlot facility policy

  Scenario Outline: A slot announcement the consumer cannot use (<why>) is acknowledged and tallies nothing
    Given facility-layout announced a LocationSlotRegistered as event "slot-bad" with data <data>
    When I look up the storage capacity of "SIM1"
    Then the response status is 200
    And the storage capacity lists nothing

    Examples:
      | why                              | data                                                                                           |
      | a work center with no activities | {"locationCode":"SIM1-A1","zoneId":"SIM1-OPS-WC","role":"WorkCenter","activities":[]}          |
      | storage without a location type  | {"locationCode":"SIM1-A2","zoneId":"SIM1-STOR-AMB","role":"Storage"}                           |
      | a role the tally does not know   | {"locationCode":"SIM1-A3","zoneId":"SIM1-DOCK","role":"Dock","locationType":"Door"}            |
      | no zone id                       | {"locationCode":"SIM1-A4","role":"Storage","locationType":"SimShelf"}                          |
      | no location code                 | {"zoneId":"SIM1-STOR-AMB","role":"Storage","locationType":"SimShelf"}                          |

  Scenario Outline: A slot announced incompletely (<why>) is not remembered, so its complete announcement is still tallied
    Given facility-layout announced a LocationSlotRegistered as event "slot-1" with data <incomplete>
    And facility-layout announced a LocationSlotRegistered as event "slot-2" with data <complete>
    When I look up the storage capacity of "SIM1"
    Then the storage capacity lists 1 PACK stations in zone "SIM1-OPS-WC"

    Examples:
      | why                              | incomplete                                                                       | complete                                                                                         |
      | a work center with no activities | {"locationCode":"SIM1-WC-7","zoneId":"SIM1-OPS-WC","role":"WorkCenter","activities":[]} | {"locationCode":"SIM1-WC-7","zoneId":"SIM1-OPS-WC","role":"WorkCenter","activities":["PACK"]} |
      | no zone id                       | {"locationCode":"SIM1-WC-7","role":"WorkCenter","activities":["PACK"]}          | {"locationCode":"SIM1-WC-7","zoneId":"SIM1-OPS-WC","role":"WorkCenter","activities":["PACK"]} |

  Scenario: Other message types and non-CloudEvents on the facility topic are ignored
    Given facility-layout published an unrelated event of type "com.warehouse.wms.facility-layout.zone.ZoneCreated"
    And the facility topic carries a message that is not a CloudEvent
    When I look up the storage capacity of "SIM1"
    Then the storage capacity lists nothing

  Scenario: A work center that supports several activities counts as a station for each of them, whatever the case
    Given facility-layout announced a LocationSlotRegistered as event "wc-1" with data {"locationCode":"SIM1-WC-1","zoneId":"SIM1-OPS-WC","role":"WorkCenter","activities":["pick","PACK"]}
    When I look up the storage capacity of "SIM1"
    Then the storage capacity lists 1 PICK stations in zone "SIM1-OPS-WC"
    And the storage capacity lists 1 PACK stations in zone "SIM1-OPS-WC"

  Scenario Outline: A slot decommissioned a second time (<how>) is counted off once
    Given facility-layout registered 3 PACK work-center slots in zone "SIM1-OPS-WC"
    When facility-layout decommissioned the slot "SIM1-OPS-WC-001" as event "dec-1"
    And facility-layout decommissioned the slot "SIM1-OPS-WC-001" as event "<second event>"
    And I look up the storage capacity of "SIM1"
    Then the storage capacity lists 2 PACK stations in zone "SIM1-OPS-WC"

    Examples:
      | how                       | second event |
      | the event is redelivered  | dec-1        |
      | under a new event id      | dec-2        |

  Scenario: A zone whose last slot is decommissioned drops out of the read model
    Given facility-layout registered 2 storage slots of type "SimShelf" in zone "SIM1-STOR-AMB"
    When I look up the storage capacity of "SIM1"
    Then the storage capacity lists 2 SimShelf storage positions in zone "SIM1-STOR-AMB"
    When facility-layout decommissioned 2 slots in zone "SIM1-STOR-AMB"
    And I look up the storage capacity of "SIM1"
    Then the storage capacity lists nothing

  # --------------------------------------------- OrderAllocated demand policy

  Scenario: An order event stamped with the stored event's own time replaces it
    Given order-management published OrderPartiallyAllocated for order "ord-a" promised at "2026-10-05T09:00:00Z" with 1 released line at event time "2026-10-04T09:00:00Z"
    And order-management published OrderAllocated for order "ord-a" promised at "2026-10-06T09:00:00Z" with 3 released lines at event time "2026-10-04T09:00:00Z"
    When I request the expected demand for "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the expected demand is 0 orders and 0 released lines
    When I request the expected demand for "SIM1" for the window "2026-10-06T08:00:00Z" to "2026-10-06T16:00:00Z"
    Then the expected demand is 1 order and 3 released lines

  Scenario: A redelivered order event is applied once, whatever the second copy says
    Given order-management delivered an OrderAllocated event "ord-evt-1" about subject "ord-a" at "2026-10-04T09:00:00Z" with data {"order_id":"ord-a","promise_date":"2026-10-05T09:00:00Z","lines":[{"line_no":1},{"line_no":2}]}
    And order-management delivered an OrderAllocated event "ord-evt-1" about subject "ord-a" at "2026-10-04T10:00:00Z" with data {"order_id":"ord-a","promise_date":"2026-10-06T09:00:00Z","lines":[{"line_no":1}]}
    When I request the expected demand for "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the expected demand is 1 order and 2 released lines
    And the expected demand as of is "2026-10-04T09:00:00Z"
    When I request the expected demand for "SIM1" for the window "2026-10-06T08:00:00Z" to "2026-10-06T16:00:00Z"
    Then the expected demand is 0 orders and 0 released lines

  Scenario Outline: An order event with <flaw> is acknowledged and never becomes demand
    Given order-management delivered an OrderAllocated event "ord-evt-bad" about subject "<subject>" at "2026-10-04T09:00:00Z" with data <data>
    When I request the expected demand for "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the expected demand is 0 orders and 0 released lines
    And the expected demand as of is null

    Examples:
      | flaw                        | subject | data                                                                                   |
      | a subject that is not its id | ord-b   | {"order_id":"ord-a","promise_date":"2026-10-05T09:00:00Z","lines":[{"line_no":1}]}     |
      | no promise date             | ord-a   | {"order_id":"ord-a","lines":[{"line_no":1}]}                                           |
      | a payload that is no object | ord-a   | [1,2,3]                                                                                |
