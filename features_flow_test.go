package main_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cucumber/godog"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// flowState is the ADR 0025 slice of the BDD world: the in-memory copy of
// process-path-management's flows and the RecordProcessFlow use case the
// process flow consumer drives (the consumer's wire handling has its own
// tests against the pinned producer spec).
type flowState struct {
	flows  *memory.ProcessFlowRepo
	record *usecases.RecordProcessFlow
	seq    int
	last   struct {
		id    string
		flow  processflow.Flow
		valid bool
	}
}

func newFlowState() flowState {
	flows, processed := memory.NewProcessFlowRepo(), memory.NewProcessedEventRepo()
	return flowState{
		flows: flows,
		record: &usecases.RecordProcessFlow{
			UoW: memory.NewUnitOfWork(flows, processed), ProcessedEvents: processed, Flows: flows,
		},
	}
}

// parseStages reads "PICK, REBIN, PACK": each stage names the path of its
// own process type, the shape the producer's examples use.
func parseStages(raw string) []processflow.Stage {
	var out []processflow.Stage
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		out = append(out, processflow.Stage{PathID: s, ProcessType: processflow.ProcessType(s)})
	}
	return out
}

func (w *world) processFlowPublished(event, flowID, name string, version int, stages string) error {
	status := processflow.StatusActive
	if event == "ProcessFlowRetired" {
		status = processflow.StatusRetired
	}
	f, err := processflow.New(processflow.Params{
		FlowID: flowID, Name: name, Stages: parseStages(stages), Status: status,
		Version: int64(version), UpdatedAt: time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC),
	})
	if err != nil {
		return err
	}
	w.pf.seq++
	w.pf.last.id, w.pf.last.flow, w.pf.last.valid = fmt.Sprintf("bdd-flow-evt-%d", w.pf.seq), f, true
	_, err = w.pf.record.Handle(context.Background(), "process-flow-consumer", w.pf.last.id, f)
	return err
}

func (w *world) processFlowRedelivered() error {
	if !w.pf.last.valid {
		return fmt.Errorf("no process flow message was published yet")
	}
	_, err := w.pf.record.Handle(context.Background(), "process-flow-consumer", w.pf.last.id, w.pf.last.flow)
	return err
}

func (w *world) localFlowIs(flowID string, version int, stages, status string) error {
	f, ok := w.pf.flows.Get(flowID)
	if !ok {
		return fmt.Errorf("flow %q is not in the local copy", flowID)
	}
	var got []string
	for _, s := range f.Steps() {
		got = append(got, string(s))
	}
	if f.Version() != int64(version) || strings.Join(got, ", ") != stages || string(f.Status()) != status {
		return fmt.Errorf("flow %q = version %d, stages %s, status %s; want version %d, stages %s, status %s",
			flowID, f.Version(), strings.Join(got, ", "), f.Status(), version, stages, status)
	}
	return nil
}

func (w *world) localFlowIsAbsent(flowID string) error {
	if _, ok := w.pf.flows.Get(flowID); ok {
		return fmt.Errorf("flow %q is in the local copy", flowID)
	}
	return nil
}

func registerFlowSteps(sc *godog.ScenarioContext, w *world) {
	const ts = `\"([^\"]*)\"`
	sc.Step(`^process-path-management published (ProcessFlowDefined|ProcessFlowRevised|ProcessFlowRetired) `+ts+` named `+ts+` at version (\d+) with stages ([A-Z, ]+)$`, w.processFlowPublished)
	sc.Step(`^process-path-management redelivers its last process flow message$`, w.processFlowRedelivered)
	sc.Step(`^the local copy holds flow `+ts+` at version (\d+) with stages ([A-Z, ]+) and status (ACTIVE|RETIRED)$`, w.localFlowIs)
	sc.Step(`^the local copy holds no flow `+ts+`$`, w.localFlowIsAbsent)
}
