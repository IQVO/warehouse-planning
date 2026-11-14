# Derived from the warehouse-planning documentation (warehouse-docs
# docs/contexts/warehouse-planning: domain-events "Consumed" for the dock
# appointment and task completion rows, troubleshooting "GET /inbound-demand"
# and "variance" rows, use-cases) and from apis/openapi.yaml for the list
# paging contract. Edges are Examples rows so each one is its own definition.
@bdd
Feature: Inbound demand, observed capacity and list paging at their documented edges
  As a warehouse planner
  I want the half-open windows, version guard and paging rules to hold at the exact boundary
  So that an appointment, a completion or a plan is never counted in or out by one second

  # ------------------------------------------------------------ inbound demand

  Scenario Outline: An INBOUND appointment whose door window starts at <start> is counted <n> time(s) in the window 08:00 to 10:00
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "<start>" to "2026-10-05T11:00:00Z" covering 1 ASN at event time "2026-10-04T09:00:00Z"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the response status is 200
    And the inbound demand is <n> appointment and <asns> ASN with 0 labour minutes

    Examples:
      | start                | n | asns |
      | 2026-10-05T08:00:00Z | 1 | 1    |
      | 2026-10-05T07:59:59Z | 0 | 0    |
      | 2026-10-05T09:59:59Z | 1 | 1    |
      | 2026-10-05T10:00:00Z | 0 | 0    |

  Scenario: An event with the stored version or an older one never changes an appointment
    Given dock-scheduling published DockAppointmentBooked version 2 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentRescheduled version 2 for appointment "appt-1" at site "SIM1" to the window "2026-10-05T11:00:00Z" to "2026-10-05T12:00:00Z" at event time "2026-10-04T10:00:00Z"
    And dock-scheduling published DockAppointmentRescheduled version 1 for appointment "appt-1" at site "SIM1" to the window "2026-10-05T13:00:00Z" to "2026-10-05T14:00:00Z" at event time "2026-10-04T11:00:00Z"
    And dock-scheduling published DockAppointmentCancelled version 2 for appointment "appt-1" at site "SIM1" at event time "2026-10-04T12:00:00Z"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 1 appointment and 2 ASNs with 0 labour minutes
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T11:00:00Z" to "2026-10-05T14:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes

  Scenario: A cancelled appointment that is booked again under a newer version is demand again
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentCancelled version 2 for appointment "appt-1" at site "SIM1" at event time "2026-10-04T10:00:00Z"
    And dock-scheduling published DockAppointmentBooked version 3 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:45:00Z" to "2026-10-05T09:45:00Z" covering 2 ASNs at event time "2026-10-04T11:00:00Z"
    When I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    And I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 1 appointment and 2 ASNs with 50 labour minutes

  Scenario: A booked INBOUND appointment that covers no ASN is a poison message and never demand
    Given dock-scheduling published an INBOUND booking "appt-empty" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering no ASN that is rejected as a poison message
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes

  Scenario: A negative per-ASN standard is refused, so labour minutes can never go below the appointment minutes
    When I declare an inbound labour standard of 30 minutes per appointment and -5 minutes per ASN for path "receive" at "SIM1"
    Then the response status is 422
    And the problem detail type is "negative-asn-minutes"
    When I list the inbound labour standards of "SIM1"
    Then 0 inbound labour standards are listed

  Scenario Outline: The inbound demand request without a <missing> is a 400 problem
    When I send GET "<query>"
    Then the response status is 400
    And the problem detail type is "<slug>"

    Examples:
      | missing  | query                                                                                                    | slug             |
      | location | /inbound-demand?path_id=receive&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T10:00:00Z        | missing-location |
      | path_id  | /inbound-demand?location=SIM1&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T10:00:00Z          | missing-path-id  |

  # -------------------------------------------------------- observed capacity

  Scenario Outline: A task completed at <at> is counted in a plan window of 08:00 to 10:00 (minute resolution)
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8, units_per_order 1 and packages_per_order 1
    And fulfillment-execution published 3 TaskCompleted events for task type "PICK" at "<at>"
    When I request the variance of the capacity plan with tasks_per_order 1
    Then the response status is 200
    And the observed throughput is 3 tasks

    Examples:
      | at                   |
      | 2026-10-05T08:00:00Z |
      | 2026-10-05T09:59:59Z |

  Scenario Outline: A task completed at <at> is outside a plan window of 08:00 to 10:00 (minute resolution)
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8, units_per_order 1 and packages_per_order 1
    And fulfillment-execution published 3 TaskCompleted events for task type "PICK" at "<at>"
    When I request the variance of the capacity plan with tasks_per_order 1
    Then the response status is 200
    And no completion fell inside the plan window
    And the variance is not comparable because "no task completions observed"

    Examples:
      | at                   |
      | 2026-10-05T07:59:59Z |
      | 2026-10-05T10:00:00Z |

  Scenario: Observed over-performance is a positive variance in orders and percent
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8, units_per_order 1 and packages_per_order 1
    And fulfillment-execution published 30 TaskCompleted events for task type "PICK" at "2026-10-05T09:00:00Z"
    When I request the variance of the capacity plan with tasks_per_order 2
    Then the observed throughput is 30 tasks
    And the planned capacity is 5 orders per hour and 10 over the window
    And the variance is 15 observed orders, 5 orders and 50 percent

  Scenario: The variance of a plan that does not exist is a 404 problem, and a tasks_per_order that is no number is a 400
    When I send GET "/capacity-plans/no-such-plan/variance?tasks_per_order=2"
    Then the response status is 404
    And the problem detail type is "capacity-plan-not-found"
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 8, units_per_order 1 and packages_per_order 1
    When I request the variance of the capacity plan with the tasks_per_order "lots"
    Then the response status is 400
    And the problem detail type is "malformed-tasks-per-order"

  # ------------------------------------------------------------- list paging

  Scenario Outline: A page of <limit> plans out of 3 has <cursor> next cursor
    Given I register a LABOR constraint of 1000 ORDER per HOUR for PICK at PAGE-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick only" with steps PICK
    And I create a capacity plan with assigned demand 1000 at "PAGE-ZONE" on path "pick-rebin-pack"
    And I create a capacity plan with assigned demand 2000 at "PAGE-ZONE" on path "pick-rebin-pack"
    And I create a capacity plan with assigned demand 3000 at "PAGE-ZONE" on path "pick-rebin-pack"
    When I list the capacity plans of every location with limit "<limit>"
    Then the response status is 200
    And the capacity plan list has <returned> plans
    And the response has <cursor> next cursor

    Examples:
      | limit | returned | cursor |
      | 2     | 2        | a      |
      | 3     | 3        | no     |
      | 4     | 3        | no     |

  Scenario: Following the next cursor serves the remaining plans, newest first, and then ends
    Given I register a LABOR constraint of 1000 ORDER per HOUR for PICK at PAGE-ZONE for the window "2026-10-05T08:00:00Z" to "2026-10-05T16:00:00Z"
    And I register a process path "pick-rebin-pack" named "Pick only" with steps PICK
    And I create a capacity plan with assigned demand 1000 at "PAGE-ZONE" on path "pick-rebin-pack"
    And I create a capacity plan with assigned demand 2000 at "PAGE-ZONE" on path "pick-rebin-pack"
    And I create a capacity plan with assigned demand 3000 at "PAGE-ZONE" on path "pick-rebin-pack"
    When I list the capacity plans of every location with limit "2"
    Then the capacity plan list has assigned demands 3000, 2000
    When I follow the next cursor
    Then the response status is 200
    And the capacity plan list has assigned demands 1000
    And the response has no next cursor

  Scenario Outline: A page size of <limit> is <outcome>
    When I list the capacity plans of every location with limit "<limit>"
    Then the response status is <status>

    Examples:
      | limit | outcome  | status |
      | 1     | accepted | 200    |
      | 500   | accepted | 200    |
      | 501   | refused  | 400    |

  Scenario Outline: A list request with <flaw> is a 400 problem
    When I send GET "<query>"
    Then the response status is 400
    And the problem detail type is "<slug>"

    Examples:
      | flaw                      | query                              | slug                    |
      | an undeclared parameter   | /capacity-plans?offset=5           | unknown-query-parameter |
      | a cursor that is no token | /capacity-plans?cursor=not-a-token | invalid-cursor          |
