package main_test

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/cucumber/godog"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// The labour plan drift steps (docs/adr/0023 decision 3): workforce-
// management's commitments are fed through the REAL labor consumer's
// HandleMessage, the plans through PUT /labour-plans, and the drift is
// asserted through the outbox only.

// driftCommitTime is the CloudEvents time of every commitment the steps deliver.
const driftCommitTime = "2026-11-19T06:00:00Z"

// wireDrift gives the labor consumer its commitment trigger the way cmd/api
// does: the committed heads, the labour plans the HTTP surface writes, the
// world's outbox and encoder, on the consumer's own unit of work.
func (s *wave2State) wireDrift(plans *memory.LabourPlanRepo, ob *memory.OutboxRepo, encoder ports.EventEncoder) {
	s.labor.Commitments = &usecases.RecordShiftCommitment{
		Commitments: s.commitments, Plans: plans, Outbox: ob, Encoder: encoder, UnitOfWork: s.labor.UoW,
	}
}

// committedLine is one ShiftPlanCommitted line at a site; an empty shiftID
// leaves shift_id out.
func committedLine(heads int, pathID, shiftID, siteID string) map[string]any {
	d := map[string]any{"site_code": siteID, "path_id": pathID, "planned_heads": heads, "planned_rate": 10.0, "planned_hours": 8.0}
	if shiftID != "" {
		d["shift_id"] = shiftID
	}
	return d
}

func (w *world) workforceCommittedHeads(heads int, pathID, shiftID, siteID, eventID string) error {
	return w.deliverLabor(typeShiftPlanCommitted, eventID, committedLine(heads, pathID, shiftID, siteID), driftCommitTime)
}

func (w *world) workforceCommittedHeadsWithoutShift(heads int, pathID, siteID, eventID string) error {
	return w.deliverLabor(typeShiftPlanCommitted, eventID, committedLine(heads, pathID, "", siteID), driftCommitTime)
}

// theLastDriftIs reads the data of the newest LabourPlanDriftDetected in the outbox.
func (w *world) theLastDriftIs(planned, committed, drift int) error {
	var last []byte
	for _, m := range w.outbox.Messages() {
		if strings.HasSuffix(m.EventType, "."+labourplan.EventLabourPlanDriftDetected) {
			last = m.Value
		}
	}
	if last == nil {
		return fmt.Errorf("the outbox holds no %s", labourplan.EventLabourPlanDriftDetected)
	}
	var env struct {
		Data struct {
			PlannedHeads   int `json:"planned_heads"`
			CommittedHeads int `json:"committed_heads"`
			DriftHeads     int `json:"drift_heads"`
		} `json:"data"`
	}
	if err := json.Unmarshal(last, &env); err != nil {
		return err
	}
	if d := env.Data; d.PlannedHeads != planned || d.CommittedHeads != committed || d.DriftHeads != drift {
		return fmt.Errorf("expected planned %d, committed %d, drift %d; got %s", planned, committed, drift, string(last))
	}
	return nil
}

func registerDriftSteps(sc *godog.ScenarioContext, w *world) {
	sc.Step(`^workforce-management committed (-?\d+) heads for path "([^"]*)" in shift "([^"]*)" at site "([^"]*)" as event "([^"]*)"$`, w.workforceCommittedHeads)
	sc.Step(`^workforce-management committed (-?\d+) heads for path "([^"]*)" at site "([^"]*)" without a shift as event "([^"]*)"$`, w.workforceCommittedHeadsWithoutShift)
	sc.Step(`^the last LabourPlanDriftDetected has (\d+) planned heads, (\d+) committed heads and a drift of (-?\d+)$`, w.theLastDriftIs)
}
