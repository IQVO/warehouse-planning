Feature: Process flows from process-path-management
  As a warehouse planner
  I want the ordered stages of a flow to come from process-path-management
  So that the topology of the floor has one owner and replays are harmless

  # docs/adr/0025 decision 1: every message is the full state of the flow
  # and carries its version; a message is applied only when its version is
  # greater than the stored one.

  Scenario: A first message for an unknown flow is stored
    Given process-path-management published ProcessFlowDefined "single-unit" named "Single unit" at version 1 with stages PICK, PACK
    Then the local copy holds flow "single-unit" at version 1 with stages PICK, PACK and status ACTIVE

  Scenario: A greater version replaces the stored flow
    Given process-path-management published ProcessFlowDefined "single-unit" named "Single unit" at version 1 with stages PICK, PACK
    When process-path-management published ProcessFlowRevised "single-unit" named "Single unit" at version 2 with stages PICK, REBIN, PACK
    Then the local copy holds flow "single-unit" at version 2 with stages PICK, REBIN, PACK and status ACTIVE

  Scenario: A lower version arriving late changes nothing
    Given process-path-management published ProcessFlowRevised "single-unit" named "Single unit" at version 2 with stages PICK, REBIN, PACK
    When process-path-management published ProcessFlowDefined "single-unit" named "Single unit" at version 1 with stages PICK, PACK
    Then the local copy holds flow "single-unit" at version 2 with stages PICK, REBIN, PACK and status ACTIVE

  Scenario: The startup republish at the current version changes nothing
    Given process-path-management published ProcessFlowRevised "single-unit" named "Single unit" at version 2 with stages PICK, REBIN, PACK
    When process-path-management published ProcessFlowRevised "single-unit" named "Renamed upstream" at version 2 with stages PICK, PACK
    Then the local copy holds flow "single-unit" at version 2 with stages PICK, REBIN, PACK and status ACTIVE

  Scenario: A redelivery of the same message changes nothing
    Given process-path-management published ProcessFlowDefined "single-unit" named "Single unit" at version 1 with stages PICK, PACK
    When process-path-management redelivers its last process flow message
    Then the local copy holds flow "single-unit" at version 1 with stages PICK, PACK and status ACTIVE

  Scenario: A retirement is stored as a RETIRED flow
    Given process-path-management published ProcessFlowRevised "single-unit" named "Single unit" at version 2 with stages PICK, REBIN, PACK
    When process-path-management published ProcessFlowRetired "single-unit" named "Single unit" at version 3 with stages PICK, REBIN, PACK
    Then the local copy holds flow "single-unit" at version 3 with stages PICK, REBIN, PACK and status RETIRED

  Scenario: A first message that is a retirement stores a RETIRED row
    Given process-path-management published ProcessFlowRetired "old-flow" named "Old flow" at version 4 with stages PICK, PACK
    Then the local copy holds flow "old-flow" at version 4 with stages PICK, PACK and status RETIRED

  Scenario: An unknown flow is absent
    Then the local copy holds no flow "never-published"

  # docs/adr/0025 decisions 2 and 3: the path capacity and the path reads
  # resolve a path id from an ACTIVE flow first; a local path without a flow
  # keeps working; every read says which one it used.

  Scenario: A flow with no local path is read by id and listed with source flow
    Given process-path-management published ProcessFlowDefined "single-unit" named "Single unit" at version 1 with stages PICK, PACK
    When I ask for process path "single-unit"
    Then the response status is 200
    And the process path comes from a flow source with steps PICK, PACK
    When I list the process paths
    Then the listed process paths are "single-unit:flow"

  Scenario: A locally declared path is read with source local
    When I register a process path "tote-path" named "Tote path" with steps PICK, REBIN, PACK
    And I ask for process path "tote-path"
    Then the response status is 200
    And the process path comes from a local source with steps PICK, REBIN, PACK

  Scenario: An id neither source knows is a 404
    When I ask for process path "nowhere-path"
    Then the response status is 404

  Scenario: A flow wins over a local path with the same id, and the list interleaves both by id
    Given process-path-management published ProcessFlowDefined "pick-rebin-pack" named "Pick-Rebin-Pack" at version 1 with stages PICK, PACK
    And process-path-management published ProcessFlowDefined "zz-flow" named "ZZ flow" at version 1 with stages SLAM
    When I register a process path "pick-rebin-pack" named "Local Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    And I register a process path "aa-local" named "AA local" with steps PICK
    And I ask for process path "pick-rebin-pack"
    Then the process path comes from a flow source with steps PICK, PACK
    When I list the process paths
    Then the listed process paths are "aa-local:local, pick-rebin-pack:flow, zz-flow:flow"

  Scenario: A retired flow stops resolving and the local path of that id is used again
    Given process-path-management published ProcessFlowDefined "pick-rebin-pack" named "Pick-Rebin-Pack" at version 1 with stages PICK, PACK
    And I register a process path "pick-rebin-pack" named "Local Pick-Rebin-Pack" with steps PICK, REBIN, PACK
    When process-path-management published ProcessFlowRetired "pick-rebin-pack" named "Pick-Rebin-Pack" at version 2 with stages PICK, PACK
    And I ask for process path "pick-rebin-pack"
    Then the process path comes from a local source with steps PICK, REBIN, PACK

  Scenario: A retired flow with no local path is a 404
    Given process-path-management published ProcessFlowRetired "old-flow" named "Old flow" at version 3 with stages PICK, PACK
    When I ask for process path "old-flow"
    Then the response status is 404

  Scenario: The path capacity composes the flow's stages and says so
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And process-path-management published ProcessFlowDefined "single-unit" named "Single unit" at version 1 with stages PICK, REBIN, PACK
    When I look up the capacity of process path "single-unit" at "PATH-ZONE-A" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response reports 1000 ORDER per HOUR bound by REBIN
    And the process path capacity response states source "flow"

  Scenario: A revision of the flow changes the steps the next read composes
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 2500 UNIT per HOUR for REBIN at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a LABOR constraint of 1800 PACKAGE per HOUR for PACK at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And process-path-management published ProcessFlowDefined "single-unit" named "Single unit" at version 1 with stages PICK, PACK
    When I look up the capacity of process path "single-unit" at "PATH-ZONE-A" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the process path capacity response reports 1600 ORDER per HOUR bound by PICK
    When process-path-management published ProcessFlowRevised "single-unit" named "Single unit" at version 2 with stages PICK, REBIN, PACK
    And I look up the capacity of process path "single-unit" at "PATH-ZONE-A" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the process path capacity response reports 1000 ORDER per HOUR bound by REBIN

  Scenario: A local path keeps working and its capacity says source local
    Given I register a LABOR constraint of 4000 UNIT per HOUR for PICK at PATH-ZONE-A for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z"
    And I register a process path "local-pick" named "Local pick" with steps PICK
    When I look up the capacity of process path "local-pick" at "PATH-ZONE-A" for the window "2026-10-05T08:00:00Z" to "2026-10-05T09:00:00Z" with units_per_order 2.5 and packages_per_order 1
    Then the response status is 200
    And the process path capacity response states source "local"
