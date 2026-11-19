Feature: The drift between the labour plan and the heads workforce-management committed
  As a warehouse planner
  I want to hear when workforce-management commits a different number of heads than my labour plan needs
  So that a shift is never staffed off a plan nobody looked at again

  # docs/adr/0023 decision 3 (wes-work-planning ADR 0019's surfacing rule, kept): the drift of
  # a (site_id, shift_id, path_id) is committed_heads - planned_heads. Two triggers compare
  # them: a ShiftPlanCommitted line (in the labor consumer's unit of work, after its LABOR
  # constraint) and a labour plan commit that CHANGED the plan (in the plan's unit of work).
  # LabourPlanDriftDetected is raised when the drift is non-zero and differs from the last
  # drift RECORDED for the key; every computed drift is recorded, zero included, so a drift
  # that clears and returns is raised again. Neither trigger changes the plan or the
  # commitment. A line without shift_id records nothing; a redelivered event raises nothing.

  Scenario: A commitment that differs from the plan raises the drift
    Given I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    When workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-1"
    Then the outbox holds 1 LabourPlanDriftDetected event
    And the last LabourPlanDriftDetected has 7 planned heads, 10 committed heads and a drift of 3

  Scenario: A redelivered commitment raises nothing
    Given I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-1"
    And workforce-management committed 7 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-2"
    When workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-1"
    Then the outbox holds 1 LabourPlanDriftDetected event

  Scenario: The same drift again is not raised twice
    Given I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-1"
    When workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-2"
    Then the outbox holds 1 LabourPlanDriftDetected event

  Scenario: A drift that clears and returns is raised again
    Given I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-1"
    And workforce-management committed 7 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-2"
    When workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-3"
    Then the outbox holds 2 LabourPlanDriftDetected events
    And the last LabourPlanDriftDetected has 7 planned heads, 10 committed heads and a drift of 3

  Scenario: A commitment before any plan raises nothing until the plan is committed
    Given workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-1"
    And the outbox holds 0 LabourPlanDriftDetected events
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    Then the response status is 201
    And the outbox holds 1 LabourPlanDriftDetected event
    And the last LabourPlanDriftDetected has 7 planned heads, 10 committed heads and a drift of 3

  Scenario: A revised plan with a new drift raises it, an unchanged PUT raises nothing
    Given workforce-management committed 10 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-1"
    And I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 9 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 9 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    Then the outbox holds 2 LabourPlanDriftDetected events
    And the outbox holds 2 LabourPlanCommitted events
    And the last LabourPlanDriftDetected has 9 planned heads, 10 committed heads and a drift of 1

  Scenario: A line without a shift keeps the LABOR behaviour and records nothing for the drift
    Given workforce-management committed 10 heads for path "PICK" at site "SIM1" without a shift as event "wfm-1"
    When I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    Then the outbox holds 0 LabourPlanDriftDetected events

  Scenario: Neither trigger changes the labour plan
    Given I commit a labour plan for path "PICK" in shift "S1" at site "SIM1" with 7 planned heads, rate 10 units per hour, 8 hours and 12 installed stations
    And workforce-management committed 12 heads for path "PICK" in shift "S1" at site "SIM1" as event "wfm-1"
    When I send GET "/labour-plans/SIM1/S1/PICK"
    Then the response status is 200
    And the labour plan has 7 planned heads from "request" at version 1
