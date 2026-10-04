Feature: Expected demand from order-management
  As a warehouse planner
  I want capacity plans to default their demand from the orders order-management
  has already promised
  So that nobody has to type a number that the fleet already knows

  # order-management publishes OrderAllocated / OrderPartiallyAllocated with the
  # order's promise cutoff (promise_date); warehouse-planning keeps a LOCAL read
  # model of them (docs/adr/0004). An order is demand in the half-open window
  # [start, end) when its cutoff is at or after start and strictly before end.
  # order-management orders carry no site: every order belongs to the one
  # configured site, SIM1 here. The events go through the REAL consumer's
  # HandleMessage; the requests go through the REAL HTTP surface.

  Scenario: Orders promised inside a window are expected demand; the cutoff at the window end is not
    Given order-management published OrderAllocated for order "ord-a" promised at "2026-10-05T08:00:00Z" with 2 released lines
    And order-management published OrderAllocated for order "ord-b" promised at "2026-10-05T11:30:00Z" with 3 released lines
    And order-management published OrderPartiallyAllocated for order "ord-c" promised at "2026-10-05T15:59:59Z" with 1 released line
    And order-management published OrderAllocated for order "ord-d" promised at "2026-10-05T16:00:00Z" with 5 released lines
    And order-management published OrderAllocated for order "ord-e" promised at "2026-10-05T07:59:59Z" with 7 released lines
    When I request the expected demand for "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the response status is 200
    And the expected demand is 3 orders and 6 released lines
    And the expected demand as of is "2026-10-04T09:04:00Z"

  Scenario: An order is counted once, at the promise of its latest event
    Given order-management published OrderPartiallyAllocated for order "ord-a" promised at "2026-10-05T09:00:00Z" with 1 released line at event time "2026-10-04T09:00:00Z"
    And order-management published OrderAllocated for order "ord-a" promised at "2026-10-06T09:00:00Z" with 4 released lines at event time "2026-10-04T10:00:00Z"
    And order-management published OrderAllocated for order "ord-a" promised at "2026-10-05T09:00:00Z" with 9 released lines at event time "2026-10-04T09:30:00Z"
    When I request the expected demand for "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the expected demand is 0 orders and 0 released lines
    When I request the expected demand for "SIM1" for the window "2026-10-06T08:00:00Z" to "2026-10-06T16:00:00Z"
    Then the expected demand is 1 order and 4 released lines
    And the expected demand as of is "2026-10-04T10:00:00Z"

  Scenario: What the events cannot support is ignored without error
    Given order-management published an OrderRepromised for order "ord-a"
    And order-management topic carries a message that is not a CloudEvent
    When I request the expected demand for "SIM1" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the response status is 200
    And the expected demand is 0 orders and 0 released lines
    And the expected demand as of is null

  Scenario: Another site has no demand from the configured site's orders
    Given order-management published OrderAllocated for order "ord-a" promised at "2026-10-05T09:00:00Z" with 2 released lines
    When I request the expected demand for "SIM2" for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    Then the expected demand is 0 orders and 0 released lines

  Scenario: A plan created without assigned_demand uses the expected orders and is labelled as such
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And order-management published 12 orders promised at "2026-10-05T09:00:00Z"
    And order-management published OrderAllocated for order "ord-outside" promised at "2026-10-05T10:00:00Z" with 1 released line
    When I create a capacity plan for warehouse "WH-1" at "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" without assigned demand, units_per_order 1 and packages_per_order 1
    Then the response status is 201
    And the capacity plan uses demand 12 from orders with shortage 2
    And the stored capacity plan remembers its demand source is orders
    When I publish the capacity plan
    Then the response status is 200
    And the outbox event types are CapacityPlanCreated, CapacityPlanPublished, CapacityShortageDetected, BottleneckDetected

  Scenario: An explicit assigned_demand always wins over the orders
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And order-management published 12 orders promised at "2026-10-05T09:00:00Z"
    When I create a capacity plan for warehouse "WH-1" at "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 5, units_per_order 1 and packages_per_order 1
    Then the response status is 201
    And the capacity plan uses demand 5 from request with shortage 0

  Scenario: Without order data a plan with no assigned_demand is rejected, never silently zero
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And order-management published OrderAllocated for order "ord-outside" promised at "2026-10-05T10:00:00Z" with 1 released line
    When I create a capacity plan for warehouse "WH-1" at "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" without assigned demand, units_per_order 1 and packages_per_order 1
    Then the response status is 422
    And the problem detail type is "missing-assigned-demand"
    And the outbox is empty

  Scenario: The demand request validates its inputs with RFC 7807 problems
    When I request the expected demand for "SIM1" for the window "2026-10-05T16:00:00Z" to "2026-10-05T08:00:00Z"
    Then the response status is 400
    And the problem detail type is "invalid-capacity-window"
