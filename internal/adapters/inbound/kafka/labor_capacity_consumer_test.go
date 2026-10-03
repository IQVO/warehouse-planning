package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// shiftPlanCommittedEvent builds the exact CloudEvents 1.0 structured-mode
// bytes workforce-management publishes (per docs/adr/0001-...'s Addendum),
// using the raw sdk-go event package directly -- this test simulates an
// UPSTREAM producer, so it deliberately does not go through this
// service's own internal/adapters/kafka/cloudevents helper (which only
// ever builds THIS service's own outgoing envelope: source
// /warehouse/warehouse-planning, type com.warehouse.wes.warehouse-planning.*).
func shiftPlanCommittedEvent(t *testing.T, id string, occurredAt time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/workforce-management")
	e.SetType("com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted")
	e.SetSubject(data["building_id"].(string) + "/" + data["shift_id"].(string))
	e.SetTime(occurredAt)
	e.SetDataSchema("urn:warehouse:workforce-management:events:ShiftPlanCommitted:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func defaultShiftPlanData() map[string]any {
	return map[string]any{
		"building_id":   "WH1",
		"shift_id":      "SHIFT-1",
		"path_id":       "pick",
		"planned_heads": 10,
		"planned_rate":  40.0,
		"planned_hours": 8.0,
	}
}

func newLaborConsumer() (*kafkaconsumer.LaborCapacityConsumer, *memory.ProcessCapacityRepo) {
	h := newLaborHarness()
	return h.consumer, h.pcs
}

// TestLaborCapacityConsumer_ShiftPlanCommitted_RegistersLaborConstraint
// proves a normal ShiftPlanCommitted message updates the LABOR
// CapacityConstraint correctly: rate = planned_heads * planned_rate,
// window = [event time, event time + planned_hours), ProcessType =
// uppercase(path_id), Location = building_id.
func TestLaborCapacityConsumer_ShiftPlanCommitted_RegistersLaborConstraint(t *testing.T) {
	c, repo := newLaborConsumer()
	occurredAt := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	value := shiftPlanCommittedEvent(t, "evt-1", occurredAt, defaultShiftPlanData())

	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	windowEnd := occurredAt.Add(8 * time.Hour)
	pc, err := repo.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", occurredAt, windowEnd)
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow: %v", err)
	}
	if pc == nil {
		t.Fatal("expected a ProcessCapacity to have been registered")
	}

	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		t.Fatalf("EffectiveRate: %v", err)
	}
	if binding != processcapacity.ConstraintLabor {
		t.Fatalf("expected LABOR binding, got %v", binding)
	}
	if want := 400.0; effective.Quantity() != want {
		t.Fatalf("expected rate %v (10 heads * 40 rate), got %v", want, effective.Quantity())
	}
	if effective.Unit() != processcapacity.UnitUnit {
		t.Fatalf("expected UNIT, got %v", effective.Unit())
	}
}

// TestLaborCapacityConsumer_IgnoresUnknownType proves an unrecognized
// CloudEvents `type` is ignored, not an error.
func TestLaborCapacityConsumer_IgnoresUnknownType(t *testing.T) {
	c, repo := newLaborConsumer()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID("evt-x")
	e.SetSource("/warehouse/workforce-management")
	e.SetType("com.warehouse.wes.workforce-management.shiftplan.SomethingElseHappened")
	e.SetSubject("WH1/SHIFT-1")
	e.SetTime(time.Now().UTC())
	e.SetDataSchema("urn:warehouse:workforce-management:events:SomethingElseHappened:v1")
	if err := e.SetData("application/json", defaultShiftPlanData()); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	value, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("expected unknown type to be ignored without error, got %v", err)
	}
	pc, err := repo.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pc != nil {
		t.Fatal("expected nothing to have been registered for an unknown type")
	}
}

// TestLaborCapacityConsumer_MalformedMessage_SkippedNotCrashed proves a
// garbage (non-CloudEvents) message never returns an error or panics.
func TestLaborCapacityConsumer_MalformedMessage_SkippedNotCrashed(t *testing.T) {
	c, _ := newLaborConsumer()
	if err := c.HandleMessage(context.Background(), []byte("not json at all")); err != nil {
		t.Fatalf("expected malformed message to be skipped without error, got %v", err)
	}
}

// TestLaborCapacityConsumer_LegacyFlatEnvelope_Skipped proves a
// retired-shape flat envelope is rejected, never parsed as a fallback.
func TestLaborCapacityConsumer_LegacyFlatEnvelope_Skipped(t *testing.T) {
	c, repo := newLaborConsumer()
	legacy := []byte(`{"event_id":"legacy-1","event_type":"ShiftPlanCommitted","occurred_at":"2026-10-05T06:00:00Z","source":"workforce-management","data":{"building_id":"WH1","shift_id":"SHIFT-1","path_id":"pick","planned_heads":10,"planned_rate":40,"planned_hours":8}}`)

	if err := c.HandleMessage(context.Background(), legacy); err != nil {
		t.Fatalf("expected legacy flat envelope to be skipped without error, got %v", err)
	}
	pc, err := repo.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", time.Time{}, time.Time{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pc != nil {
		t.Fatal("expected the legacy flat envelope to never be applied")
	}
}

// TestLaborCapacityConsumer_ReplayingSameEventID_DoesNotDoubleCount is the
// idempotency proof: redelivering the SAME CloudEvents id twice (e.g. a
// consumer-group rebalance replay) must leave the effective rate
// unchanged, not doubled. Without the ProcessedEventRepository claim this
// test would fail because RegisterProcessCapacityConstraint's upsert
// REPLACES the LABOR constraint's rate in place either way for THIS
// scenario (same payload) -- so to actually prove double-processing is
// prevented (not just coincidentally idempotent), the second delivery
// carries a DIFFERENT planned_heads that would change the effective rate
// if it were applied again.
func TestLaborCapacityConsumer_ReplayingSameEventID_DoesNotDoubleCount(t *testing.T) {
	c, repo := newLaborConsumer()
	occurredAt := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)

	first := defaultShiftPlanData()
	value := shiftPlanCommittedEvent(t, "evt-replay-1", occurredAt, first)
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("first HandleMessage: %v", err)
	}

	// Redelivered with the SAME event id but a changed planned_heads --
	// if the idempotency guard were missing, this would overwrite the
	// LABOR constraint with a new (wrong) rate.
	replay := defaultShiftPlanData()
	replay["planned_heads"] = 999
	replayValue := shiftPlanCommittedEvent(t, "evt-replay-1", occurredAt, replay)
	if err := c.HandleMessage(context.Background(), replayValue); err != nil {
		t.Fatalf("replayed HandleMessage: %v", err)
	}

	windowEnd := occurredAt.Add(8 * time.Hour)
	pc, err := repo.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", occurredAt, windowEnd)
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow: %v", err)
	}
	if pc == nil {
		t.Fatal("expected a ProcessCapacity to have been registered")
	}
	effective, _, err := pc.EffectiveRate()
	if err != nil {
		t.Fatalf("EffectiveRate: %v", err)
	}
	if want := 400.0; effective.Quantity() != want {
		t.Fatalf("expected the replayed event to be skipped, leaving rate %v, got %v", want, effective.Quantity())
	}
}

// TestLaborCapacityConsumer_PropagatesProcessedEventsErrors proves a
// genuine infrastructure error from the idempotency store is returned,
// not swallowed.
func TestLaborCapacityConsumer_PropagatesProcessedEventsErrors(t *testing.T) {
	c, _ := newLaborConsumer()
	boom := errors.New("boom")
	c.ProcessedEvents = fakeProcessedEvents{err: boom}

	value := shiftPlanCommittedEvent(t, "evt-err", time.Now().UTC(), defaultShiftPlanData())
	if err := c.HandleMessage(context.Background(), value); !errors.Is(err, boom) {
		t.Fatalf("expected the processed-events error to propagate, got %v", err)
	}
}
