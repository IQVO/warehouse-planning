Feature: Observed capacity feedback from task completions
  As a warehouse planner
  I want to see how the floor actually performed against a capacity plan
  So that I can re-plan with evidence, without the plan changing under me

  # fulfillment-execution publishes TaskCompleted; warehouse-planning keeps a
  # LOCAL observed-capacity read model of them at minute resolution
  # (docs/adr/0017). Observed throughput is in TASKS and a plan is in ORDERS, so
  # the variance exists only when the caller supplies tasks_per_order. The read
  # is ADVISORY: it never changes a plan, a planned capacity or the outbox.
  # Completions carry no site: every one belongs to the one configured site,
  # SIM1 here. The events go through the REAL consumer's HandleMessage; the
  # requests go through the REAL HTTP surface.

  Background:
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8, units_per_order 1 and packages_per_order 1

  Scenario: Observed tasks over the plan window are compared with the planned capacity
    Given fulfillment-execution published 14 TaskCompleted events for task type "PICK" at "2026-10-05T08:30:00Z"
    And fulfillment-execution published 10 TaskCompleted events for task type "PICK" at "2026-10-05T09:59:00Z"
    And fulfillment-execution published 7 TaskCompleted events for task type "PICK" at "2026-10-05T10:00:00Z"
    And fulfillment-execution published 5 TaskCompleted events for task type "PICK" at "2026-10-05T07:59:00Z"
    When I request the variance of the capacity plan with tasks_per_order 2
    Then the response status is 200
    And the planned capacity is 5 orders per hour and 10 over the window
    And the observed throughput is 24 tasks
    And the variance is 12 observed orders, 2 orders and 20 percent

  Scenario: Without tasks_per_order the observed side is reported but no variance is invented
    Given fulfillment-execution published 24 TaskCompleted events for task type "PICK" at "2026-10-05T09:00:00Z"
    When I request the variance of the capacity plan
    Then the response status is 200
    And the observed throughput is 24 tasks
    And the variance is not comparable because "tasks_per_order was not provided"

  Scenario: Nothing observed is "no data", never zero throughput
    Given fulfillment-execution published 6 TaskCompleted events for task type "PACK" at "2026-10-05T09:00:00Z"
    When I request the variance of the capacity plan with tasks_per_order 2
    Then the response status is 200
    And the observed side has no data
    And the variance is not comparable because "no task completions observed"

  Scenario: A redelivered completion is counted once
    Given fulfillment-execution published 4 TaskCompleted events for task type "PICK" at "2026-10-05T09:00:00Z"
    And fulfillment-execution redelivers its last task event
    When I request the variance of the capacity plan with tasks_per_order 1
    Then the observed throughput is 4 tasks
    And the variance is 4 observed orders, -6 orders and -60 percent

  Scenario: A lower-case task type is attributed to the same step
    Given fulfillment-execution published 3 TaskCompleted events for task type "pick" at "2026-10-05T09:00:00Z"
    When I request the variance of the capacity plan with tasks_per_order 1
    Then the observed throughput is 3 tasks

  Scenario: A completion that cannot be attributed to a step is a poison message, never counted
    Given fulfillment-execution published a TaskCompleted without a task type that is rejected as a poison message
    When I request the variance of the capacity plan with tasks_per_order 1
    Then the observed side has no data

  Scenario: The variance read never changes the plan and never republishes anything
    Given fulfillment-execution published 24 TaskCompleted events for task type "PICK" at "2026-10-05T09:00:00Z"
    And I remember the capacity plan
    When I request the variance of the capacity plan with tasks_per_order 2
    Then the response status is 200
    And the capacity plan and the outbox are unchanged

  Scenario: A non-positive tasks_per_order is rejected with an RFC 7807 problem
    When I request the variance of the capacity plan with tasks_per_order 0
    Then the response status is 422
    And the problem detail type is "non-positive-tasks-per-order"
