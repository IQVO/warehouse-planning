Feature: Inbound labour demand from dock appointments
  As a warehouse planner
  I want a capacity plan to carry the dock appointments of its window and the labour
  they need
  So that the inbound path is part of the plan instead of a blind spot

  # dock-scheduling publishes DockAppointmentBooked / Rescheduled / Cancelled /
  # NoShow; warehouse-planning keeps a LOCAL read model of them (docs/adr/0017,
  # source switched by docs/adr/0020). Only INBOUND appointments are demand. An
  # appointment is demand in the half-open window [start, end) when its door
  # window STARTS at or after start and strictly before end; a reschedule moves
  # it, a cancellation or a no-show removes it. The events carry the site_id the
  # demand is attributed to and an aggregate version that orders them (apply
  # only version > stored). The labour minutes are NOT in the event: the
  # operator declares a standard (minutes per appointment plus optional minutes
  # per ASN) and without one an appointment contributes no demand and says so.
  # The events go through the REAL consumer's HandleMessage; the requests go
  # through the REAL HTTP surface.

  Scenario: Appointments starting in the window are priced with the declared standard
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-2" at site "SIM1" from "2026-10-05T09:00:00Z" to "2026-10-05T10:30:00Z" covering 1 ASN at event time "2026-10-04T09:05:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-3" at site "SIM1" from "2026-10-05T10:00:00Z" to "2026-10-05T11:00:00Z" covering 4 ASNs at event time "2026-10-04T09:10:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-4" at site "SIM1" from "2026-10-05T07:59:00Z" to "2026-10-05T08:30:00Z" covering 1 ASN at event time "2026-10-04T09:15:00Z"
    When I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    Then the response status is 201
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the response status is 200
    And the inbound demand is 2 appointments and 3 ASNs with 90 labour minutes
    And the inbound average heads are 0.75
    And the inbound standard is declared
    And the inbound demand as of is "2026-10-04T09:15:00Z"

  Scenario: Without a declared standard appointments are recorded but contribute no demand, visibly
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the response status is 200
    And the inbound demand is 1 appointment and 2 ASNs with 0 labour minutes
    And the inbound standard is missing

  Scenario: A late declaration applies to appointments that were already recorded
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 1 ASN at event time "2026-10-04T09:00:00Z"
    When I declare an inbound labour standard of 45 minutes per appointment and 0 minutes per ASN for path "receive" at "SIM1"
    And I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 1 appointment and 1 ASN with 45 labour minutes
    And the inbound standard is declared

  Scenario: A cancellation removes the appointment's demand
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentCancelled version 2 for appointment "appt-1" at site "SIM1" at event time "2026-10-04T10:00:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes

  Scenario: A no-show removes the appointment's demand
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentNoShow version 2 for INBOUND appointment "appt-1" at site "SIM1" at event time "2026-10-05T10:30:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes

  Scenario: A cancellation that arrives before its booking still wins
    Given dock-scheduling published DockAppointmentCancelled version 2 for appointment "appt-1" at site "SIM1" at event time "2026-10-04T10:00:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes

  Scenario: A no-show that arrives before its booking still wins
    Given dock-scheduling published DockAppointmentNoShow version 2 for INBOUND appointment "appt-1" at site "SIM1" at event time "2026-10-05T10:30:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes

  Scenario: A reschedule moves the demand to the new window
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentRescheduled version 2 for appointment "appt-1" at site "SIM1" to the window "2026-10-05T11:00:00Z" to "2026-10-05T12:00:00Z" at event time "2026-10-04T10:00:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T10:00:00Z" to "2026-10-05T14:00:00Z"
    Then the inbound demand is 1 appointment and 2 ASNs with 50 labour minutes

  Scenario: A reschedule that arrives before its booking keeps the newer window
    Given dock-scheduling published DockAppointmentRescheduled version 2 for appointment "appt-1" at site "SIM1" to the window "2026-10-05T11:00:00Z" to "2026-10-05T12:00:00Z" at event time "2026-10-04T10:00:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T10:00:00Z" to "2026-10-05T14:00:00Z"
    Then the inbound demand is 1 appointment and 2 ASNs with 50 labour minutes

  Scenario: An older version never undoes a newer one
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentCancelled version 3 for appointment "appt-1" at site "SIM1" at event time "2026-10-04T11:00:00Z"
    And dock-scheduling published DockAppointmentRescheduled version 2 for appointment "appt-1" at site "SIM1" to the window "2026-10-05T08:15:00Z" to "2026-10-05T09:15:00Z" at event time "2026-10-04T10:00:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes

  Scenario: A redelivered event changes nothing
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling redelivers its last event
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 1 appointment and 2 ASNs with 50 labour minutes

  Scenario: Outbound appointments, other dock-scheduling events, the retired inbound-receiving events and other sites are not demand
    Given dock-scheduling published DockAppointmentBooked version 1 for OUTBOUND appointment "out-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 0 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentCheckedIn for appointment "appt-1" at site "SIM1"
    And inbound-receiving, the retired source, published DockAppointmentBooked for appointment "legacy-1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 1 ASN at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-2" at site "SIM2" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 1 ASN at event time "2026-10-04T09:00:00Z"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 0 appointments and 0 ASNs with 0 labour minutes
    When I request the inbound demand for "SIM2" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 1 appointment and 1 ASN with 0 labour minutes

  Scenario: Each appointment is attributed to the site of its own events, with no configured site
    Given dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-2" at site "SIM2" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 3 ASNs at event time "2026-10-04T09:05:00Z"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 1 appointment and 2 ASNs with 0 labour minutes
    When I request the inbound demand for "SIM2" and path "receive" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    Then the inbound demand is 1 appointment and 3 ASNs with 0 labour minutes

  Scenario: A capacity plan carries the inbound demand of its window next to the order shortage
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-2" at site "SIM1" from "2026-10-05T10:00:00Z" to "2026-10-05T11:00:00Z" covering 1 ASN at event time "2026-10-04T09:05:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "pick-only" at "SIM1"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 12, units_per_order 1 and packages_per_order 1
    Then the response status is 201
    And the capacity plan shows 1 inbound appointment and 50 inbound labour minutes
    And the capacity plan reports the inbound standard as not missing
    And the order shortage is 2 and the inbound labour is not part of it

  Scenario: A plan whose site declared no inbound standard says so instead of guessing
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 2 ASNs at event time "2026-10-04T09:00:00Z"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 12, units_per_order 1 and packages_per_order 1
    Then the response status is 201
    And the capacity plan shows 1 inbound appointment and 0 inbound labour minutes
    And the capacity plan reports the inbound standard as missing
    And a warning mentions "inbound standard missing"
    And the order shortage is 2 and the inbound labour is not part of it

  Scenario: An outbound plan at a site that declared an inbound standard for another path is not nagged
    Given I register a LABOR constraint of 5 UNIT per HOUR for PICK at SIM1 for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z"
    And I register a process path "pick-only" named "Pick-Only" with steps PICK
    And dock-scheduling published DockAppointmentBooked version 1 for INBOUND appointment "appt-1" at site "SIM1" from "2026-10-05T08:30:00Z" to "2026-10-05T09:30:00Z" covering 1 ASN at event time "2026-10-04T09:00:00Z"
    And I declare an inbound labour standard of 30 minutes per appointment and 0 minutes per ASN for path "receive" at "SIM1"
    When I create a capacity plan for warehouse "WH-1" at site "SIM1" in location "SIM1" on path "pick-only" for the window "2026-10-05T08:00:00Z" to "2026-10-05T10:00:00Z" with assigned demand 12, units_per_order 1 and packages_per_order 1
    Then the response status is 201
    And the capacity plan reports the inbound standard as missing
    And the response has 0 warnings

  Scenario: Declared standards are listed and replaced, never guessed
    When I declare an inbound labour standard of 30 minutes per appointment and 10 minutes per ASN for path "receive" at "SIM1"
    Then the response status is 201
    When I declare an inbound labour standard of 40 minutes per appointment and 0 minutes per ASN for path "receive" at "SIM1"
    Then the response status is 200
    When I list the inbound labour standards of "SIM1"
    Then 1 inbound labour standard is listed

  Scenario: An invalid declaration or window is rejected with an RFC 7807 problem
    When I declare an inbound labour standard of 0 minutes per appointment and 5 minutes per ASN for path "receive" at "SIM1"
    Then the response status is 422
    And the problem detail type is "non-positive-appointment-minutes"
    When I request the inbound demand for "SIM1" and path "receive" for the window "2026-10-05T16:00:00Z" to "2026-10-05T08:00:00Z"
    Then the response status is 400
    And the problem detail type is "invalid-capacity-window"
