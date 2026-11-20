package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

var (
	driftEventAt  = time.Date(2026, 11, 19, 6, 0, 0, 0, time.UTC)
	driftDetected = time.Date(2026, 11, 19, 6, 0, 5, 0, time.UTC)
	errDriftStore = errors.New("committed heads store down")
)

// recordFails fails every RecordDrift: an infrastructure failure after the
// claim, the LABOR registration and the commitment upsert.
type recordFails struct{ *memory.CommittedHeadsRepo }

func (recordFails) RecordDrift(context.Context, string, string, string, int) error {
	return errDriftStore
}

// laborDriftHarness wires the REAL LaborCapacityConsumer with the commitment
// trigger of the labour plan drift (docs/adr/0023 decision 3) over the memory
// stores, the real fan-out encoder and ONE unit of work rolling them all back.
type laborDriftHarness struct {
	consumer    *kafkaconsumer.LaborCapacityConsumer
	pcs         *memory.ProcessCapacityRepo
	processed   *memory.ProcessedEventRepo
	plans       *memory.LabourPlanRepo
	commitments *memory.CommittedHeadsRepo
	outbox      *memory.OutboxRepo
}

func newLaborDriftHarness() laborDriftHarness {
	h := laborDriftHarness{
		pcs: memory.NewProcessCapacityRepo(), processed: memory.NewProcessedEventRepo(),
		plans: memory.NewLabourPlanRepo(), commitments: memory.NewCommittedHeadsRepo(), outbox: memory.NewOutboxRepo(),
	}
	uow := memory.NewUnitOfWork(h.pcs, h.processed, h.plans, h.commitments, h.outbox)
	h.consumer = &kafkaconsumer.LaborCapacityConsumer{
		Register: &usecases.RegisterProcessCapacityConstraint{Repo: h.pcs},
		Commitments: &usecases.RecordShiftCommitment{
			Commitments: h.commitments, Plans: h.plans, Outbox: h.outbox,
			Encoder: outboundkafka.NewFanoutEncoder(), UnitOfWork: uow,
			Now: func() time.Time { return driftDetected },
		},
		ProcessedEvents: h.processed, UoW: uow, Logger: testLogger(),
	}
	return h
}

// plan stores the WH1/SHIFT-1/PICK labour plan with n planned heads, PUT as
// "pick": the domain stores the canonical path code.
func (h laborDriftHarness) plan(t *testing.T, n int) {
	t.Helper()
	p, _, err := labourplan.New(labourplan.Spec{
		SiteID: "WH1", ShiftID: "SHIFT-1", PathID: "pick", PlannedHeads: n,
		RateUnitsPerHour: 40, Hours: 8, InstalledStations: 20, HeadsSource: labourplan.HeadsFromRequest,
	}, driftEventAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.plans.Save(context.Background(), p); err != nil {
		t.Fatal(err)
	}
}

// deliver hands one ShiftPlanCommitted line committing heads heads to the consumer.
func (h laborDriftHarness) deliver(t *testing.T, id string, heads int, mutate ...func(map[string]any)) error {
	t.Helper()
	d := defaultShiftPlanData()
	d["planned_heads"] = heads
	for _, m := range mutate {
		m(d)
	}
	return h.consumer.HandleMessage(context.Background(), shiftPlanCommittedEvent(t, id, driftEventAt, d))
}

// driftData decodes the data of every LabourPlanDriftDetected row in the
// outbox (integration and analytics copies alike).
func (h laborDriftHarness) driftData(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, m := range h.outbox.Messages() {
		if !strings.HasSuffix(m.EventType, "."+labourplan.EventLabourPlanDriftDetected) {
			continue
		}
		var env struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(m.Value, &env); err != nil {
			t.Fatal(err)
		}
		out = append(out, env.Data)
	}
	return out
}

func (h laborDriftHarness) commitment(t *testing.T) (labourplan.Commitment, bool) {
	t.Helper()
	c, found, err := h.commitments.Get(context.Background(), "WH1", "SHIFT-1", "PICK")
	if err != nil {
		t.Fatal(err)
	}
	return c, found
}

// laborRegistered reports whether the LABOR constraint of the default line is stored.
func (h laborDriftHarness) laborRegistered(t *testing.T) bool {
	t.Helper()
	pc, err := h.pcs.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", driftEventAt, driftEventAt.Add(8*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return pc != nil
}

func TestLaborConsumerDrift_ACommitmentAgainstAPlanRaisesTheDriftOnBothTopics(t *testing.T) {
	h := newLaborDriftHarness()
	h.plan(t, 7)
	if err := h.deliver(t, "evt-1", 10); err != nil {
		t.Fatal(err)
	}
	if !h.laborRegistered(t) {
		t.Fatal("the LABOR constraint is no longer registered")
	}
	drifts := h.driftData(t)
	if len(drifts) != 2 {
		t.Fatalf("%d LabourPlanDriftDetected rows, want 2 (integration + analytics)", len(drifts))
	}
	want := map[string]any{"site_id": "WH1", "shift_id": "SHIFT-1", "path_id": "PICK", "planned_heads": 7.0, "committed_heads": 10.0, "drift_heads": 3.0}
	for _, d := range drifts {
		for k, v := range want {
			if d[k] != v {
				t.Fatalf("drift data %s = %v, want %v (%v)", k, d[k], v, d)
			}
		}
	}
	wantCommitment(t, h, 10, 3)
}

// wantCommitment checks the stored WH1/SHIFT-1/PICK commitment: heads
// observed at the event time with lastDrift recorded.
func wantCommitment(t *testing.T, h laborDriftHarness, heads, lastDrift int) {
	t.Helper()
	c, found := h.commitment(t)
	if !found || c.Heads != heads || !c.ObservedAt.Equal(driftEventAt) || !c.HasLastDrift || c.LastDriftHeads != lastDrift {
		t.Fatalf("commitment = %+v (found %v), want %d heads with drift %d recorded", c, found, heads, lastDrift)
	}
}

func TestLaborConsumerDrift_ACommitmentMeetsThePlanWhateverTheCaseOfItsPath(t *testing.T) {
	h := newLaborDriftHarness()
	h.plan(t, 7) // PUT as "pick"
	if err := h.deliver(t, "evt-1", 10, func(d map[string]any) { d["path_id"] = "PICK" }); err != nil {
		t.Fatal(err)
	}
	// The same commitment under another spelling is the same key: no news.
	if err := h.deliver(t, "evt-2", 10, func(d map[string]any) { d["path_id"] = " Pick " }); err != nil {
		t.Fatal(err)
	}
	drifts := h.driftData(t)
	if len(drifts) != 2 {
		t.Fatalf("%d LabourPlanDriftDetected rows, want one drift (integration + analytics)", len(drifts))
	}
	for _, d := range drifts {
		if d["path_id"] != "PICK" || d["drift_heads"] != 3.0 {
			t.Fatalf("drift data = %v, want path_id PICK and drift_heads 3", d)
		}
	}
	wantCommitment(t, h, 10, 3)
}

func TestLaborConsumerDrift_ARedeliveredEventRaisesNothing(t *testing.T) {
	h := newLaborDriftHarness()
	h.plan(t, 7)
	if err := h.deliver(t, "evt-1", 10); err != nil {
		t.Fatal(err)
	}
	// The drift clears: were evt-1 processed again it would be news (+3).
	if err := h.deliver(t, "evt-2", 7); err != nil {
		t.Fatal(err)
	}
	if err := h.deliver(t, "evt-1", 10); err != nil {
		t.Fatal(err)
	}
	if n := len(h.driftData(t)); n != 2 {
		t.Fatalf("%d LabourPlanDriftDetected rows, want only evt-1's first delivery (2)", n)
	}
	wantCommitment(t, h, 7, 0)
}

func TestLaborConsumerDrift_NoPlanYetStoresOnlyTheCommitment(t *testing.T) {
	h := newLaborDriftHarness()
	if err := h.deliver(t, "evt-1", 10); err != nil {
		t.Fatal(err)
	}
	if !h.laborRegistered(t) || len(h.driftData(t)) != 0 {
		t.Fatalf("LABOR %v, drifts %v", h.laborRegistered(t), h.driftData(t))
	}
	if c, found := h.commitment(t); !found || c.Heads != 10 || c.HasLastDrift {
		t.Fatalf("commitment = %+v (found %v), want 10 heads and no drift", c, found)
	}
}

func TestLaborConsumerDrift_ALineWithoutShiftIDKeepsTodaysBehaviour(t *testing.T) {
	h := newLaborDriftHarness()
	h.plan(t, 7)
	err := h.deliver(t, "evt-1", 10, func(d map[string]any) { d["shift_id"] = "" })
	if err != nil {
		t.Fatal(err)
	}
	if !h.laborRegistered(t) {
		t.Fatal("a line without shift_id no longer registers its LABOR constraint")
	}
	if _, found := h.commitment(t); found || len(h.driftData(t)) != 0 {
		t.Fatal("a line without shift_id recorded something for the drift")
	}
}

func TestLaborConsumerDrift_AnInvalidCommitmentIsSkipped(t *testing.T) {
	h := newLaborDriftHarness()
	h.plan(t, 7)
	if err := h.deliver(t, "evt-1", -1); err != nil {
		t.Fatalf("a negative commitment must be skipped, not retried: %v", err)
	}
	if _, found := h.commitment(t); found || len(h.driftData(t)) != 0 {
		t.Fatal("a negative commitment was recorded")
	}
	if claimed, _ := h.processed.Claim(context.Background(), "labor-capacity-consumer", "evt-1"); claimed {
		t.Fatal("the skipped message was not claimed")
	}
}

func TestLaborConsumerDrift_AnInfrastructureFailureRollsTheWholeMessageBack(t *testing.T) {
	h := newLaborDriftHarness()
	h.plan(t, 7)
	h.consumer.Commitments.Commitments = recordFails{h.commitments}
	if err := h.deliver(t, "evt-1", 10); !errors.Is(err, errDriftStore) {
		t.Fatalf("err = %v, want the drift store failure (retry)", err)
	}
	if h.laborRegistered(t) {
		t.Fatal("the LABOR constraint survived the rolled-back unit of work")
	}
	if _, found := h.commitment(t); found || len(h.outbox.Messages()) != 0 {
		t.Fatal("the commitment or an outbox row survived the rolled-back unit of work")
	}
	if claimed, _ := h.processed.Claim(context.Background(), "labor-capacity-consumer", "evt-1"); !claimed {
		t.Fatal("the claim survived: the retry would be skipped")
	}
}
