package kafka_test

import (
	"context"
	"errors"
	"testing"
	"time"
)

const laborName = "labor-capacity-consumer"

func laborQty(t *testing.T, h laborHarness, at time.Time) float64 {
	t.Helper()
	pc, err := h.pcs.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", at, at.Add(8*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if pc == nil {
		return -1
	}
	r, _, err := pc.EffectiveRate()
	if err != nil {
		t.Fatal(err)
	}
	return r.Quantity()
}

// Defect 2 (labor): the claim used to be committed before the upsert, and
// the upsert error was swallowed. A failed upsert must now un-claim the
// event, return an error, and the retry must apply.
func TestLaborConsumer_SaveFails_UnclaimsAndReturnsErrorThenRetryApplies(t *testing.T) {
	h := newLaborHarness()
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	msg := shiftPlanCommittedEvent(t, "evt-1", at, defaultShiftPlanData())

	h.flaky.failSaveAfter(0, 1)
	if err := h.consumer.HandleMessage(context.Background(), msg); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want injected infrastructure error", err)
	}
	if h.processed.Has(laborName, "evt-1") {
		t.Fatal("event stayed claimed after a failed upsert; the redelivery would be skipped")
	}
	if got := laborQty(t, h, at); got != -1 {
		t.Fatalf("constraint = %v after failed message, want none", got)
	}

	if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := laborQty(t, h, at); got != 400 {
		t.Fatalf("constraint after retry = %v, want 400", got)
	}
	if !h.processed.Has(laborName, "evt-1") {
		t.Fatal("event not claimed after the successful retry")
	}
}

func TestLaborConsumer_FindFails_ReturnsError(t *testing.T) {
	h := newLaborHarness()
	h.flaky.failFinds = 1
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	err := h.consumer.HandleMessage(context.Background(), shiftPlanCommittedEvent(t, "evt-1", at, defaultShiftPlanData()))
	if !errors.Is(err, errInjected) || h.processed.Has(laborName, "evt-1") {
		t.Fatalf("err = %v claimed=%v; want injected error and no claim", err, h.processed.Has(laborName, "evt-1"))
	}
}

// Deterministic problems return nil (no retry) -- including a domain
// validation rejection of the numbers in an otherwise well-formed event.
func TestLaborConsumer_DeterministicProblems_ReturnNil(t *testing.T) {
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	mut := func(k string, v any) map[string]any {
		d := defaultShiftPlanData()
		d[k] = v
		return d
	}
	tests := map[string][]byte{
		"not a CloudEvent":       []byte("nope"),
		"missing building":       shiftPlanCommittedEvent(t, "e1", at, mut("building_id", "")),
		"missing path":           shiftPlanCommittedEvent(t, "e2", at, mut("path_id", "")),
		"payload type mismatch":  shiftPlanCommittedEvent(t, "e3", at, mut("planned_heads", "ten")),
		"negative rate (domain)": shiftPlanCommittedEvent(t, "e4", at, mut("planned_rate", -1.0)),
		"zero hours (domain)":    shiftPlanCommittedEvent(t, "e5", at, mut("planned_hours", 0.0)),
	}
	for name, msg := range tests {
		t.Run(name, func(t *testing.T) {
			h := newLaborHarness()
			if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
				t.Fatalf("deterministic problem returned %v; must be nil so it is not retried forever", err)
			}
			if got := laborQty(t, h, at); got != -1 {
				t.Fatalf("an invalid event registered a constraint (%v)", got)
			}
		})
	}
}

func TestLaborConsumer_DomainRejection_StillMarksEventProcessed(t *testing.T) {
	h := newLaborHarness()
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	d := defaultShiftPlanData()
	d["planned_rate"] = -1.0
	if err := h.consumer.HandleMessage(context.Background(), shiftPlanCommittedEvent(t, "evt-bad", at, d)); err != nil {
		t.Fatal(err)
	}
	if !h.processed.Has(laborName, "evt-bad") {
		t.Fatal("a skipped (deterministic) event must still be recorded so redelivery is a no-op")
	}
}
