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
