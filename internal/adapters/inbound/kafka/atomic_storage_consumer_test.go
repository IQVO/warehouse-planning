package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// These tests prove the atomic at-least-once behaviour of the facility
// consumer, now a pure tally maintainer: a failure part-way through a message
// (injected AFTER the tally mutation was applied) must leave the tally and the
// processed-event claim BOTH unchanged, HandleMessage must return a non-nil
// error, and a retry must then heal everything.

const storageName = "storage-capacity-consumer"

// arrayPayloadEvent is a valid CloudEvent whose JSON data is an array, not
// the object the consumer expects, so DataAs fails (malformed payload).
func arrayPayloadEvent(t *testing.T, eventName string, at time.Time) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID("evt-array")
	e.SetSource("/warehouse/facility-layout")
	e.SetType("com.warehouse.wms.facility-layout.locationslot." + eventName)
	e.SetSubject("X")
	e.SetTime(at)
	e.SetDataSchema("urn:warehouse:facility-layout:events:" + eventName + ":v1")
	if err := e.SetData("application/json", []any{1, 2}); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func registeredEvent(t *testing.T, id, code, zone, locType string) []byte {
	t.Helper()
	return locationSlotEvent(t, id, "LocationSlotRegistered", time.Now().UTC(), map[string]any{
		"locationCode": code, "zoneId": zone, "locationType": locType,
	})
}

// assertRolledBack fails unless the tally and the claim for (zone, locType,
// eventID) are both absent.
func (h storageHarness) assertRolledBack(t *testing.T, zone, locType, eventID string) {
	t.Helper()
	if got := h.tally.Count(zone, tally.TypeLocation, locType); got != 0 {
		t.Errorf("tally = %d after failed message, want 0 (rolled back)", got)
	}
	if h.processed.Has(storageName, eventID) {
		t.Error("event stayed claimed after a failed message; the redelivery would be skipped as already processed")
	}
}

// assertApplied fails unless tally and claim are consistent at want.
func (h storageHarness) assertApplied(t *testing.T, zone, locType, eventID string, want int) {
	t.Helper()
	if got := h.tally.Count(zone, tally.TypeLocation, locType); got != want {
		t.Errorf("tally = %d, want %d", got, want)
	}
	if !h.processed.Has(storageName, eventID) {
		t.Error("event not claimed after the successful handling")
	}
}

func TestStorageConsumer_FailureAfterTallyMutation_RollsBackTallyAndClaim(t *testing.T) {
	h := newStorageHarness()
	h.flaky.failNext(1)
	msg := registeredEvent(t, "evt-1", "WH1-A-01", "ZONE-A", "BULK")

	err := h.consumer.HandleMessage(context.Background(), msg)
	if !errors.Is(err, errInjected) {
		t.Fatalf("HandleMessage err = %v, want the injected infrastructure error (non-nil => retry)", err)
	}
	h.assertRolledBack(t, "ZONE-A", "BULK", "evt-1")

	// The retry (same bytes, as Kafka/our loop would redeliver) heals it.
	if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("retry HandleMessage: %v", err)
	}
	h.assertApplied(t, "ZONE-A", "BULK", "evt-1", 1)
}

// TestStorageConsumer_RedeliveryAfterSuccessAndAlreadyRegisteredNoOp covers
// the 'locationCode already registered' no-op: it must stay safe now that
// claim + tally are atomic.
//  1. redelivering the SAME event id is skipped by the claim;
//  2. a NEW event id for the SAME locationCode hits the already-registered
//     no-op and increments nothing;
//  3. after a ROLLED-BACK first attempt the tally registration is gone too,
//     so the retry does NOT hit the no-op (the old, never-healing path).
func TestStorageConsumer_RedeliveryAfterSuccessAndAlreadyRegisteredNoOp(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()

	// (3) first attempt fails after the tally mutation.
	h.flaky.failNext(1)
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1", "LOC-1", "Z", "BULK")); err == nil {
		t.Fatal("expected the injected failure")
	}
	// Retry with a DIFFERENT event id for the same locationCode (a producer
	// re-emit): must register for real, not no-op.
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1b", "LOC-1", "Z", "BULK")); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := h.tally.Count("Z", tally.TypeLocation, "BULK"); got != 1 {
		t.Fatalf("after retry tally = %d, want 1", got)
	}

	// (1) same event id redelivered after success: skipped.
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1b", "LOC-1", "Z", "BULK")); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	// (2) new event id, same locationCode: no-op.
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1c", "LOC-1", "Z", "BULK")); err != nil {
		t.Fatalf("re-emit: %v", err)
	}
	if got := h.tally.Count("Z", tally.TypeLocation, "BULK"); got != 1 {
		t.Fatalf("redelivery double-counted: tally = %d", got)
	}
	if !h.processed.Has(storageName, "evt-1c") {
		t.Error("the no-op event is still recorded as processed (a committed, harmless handling)")
	}
}

// A WorkCenter event tallies one bucket per activity. A failure after the
// buckets were incremented must roll back ALL of them and the claim.
func TestStorageConsumer_WorkCenterFailureAfterMutation_RollsBackEveryBucket(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()
	msg := workCenterEvent(t, "evt-wc", "WH1-PACK-01", "ZONE-B", "Pack", "QC")

	h.flaky.failNext(1)
	if err := h.consumer.HandleMessage(ctx, msg); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want injected", err)
	}
	if h.tally.Count("ZONE-B", tally.TypeStation, "PACK") != 0 || h.tally.Count("ZONE-B", tally.TypeStation, "QC") != 0 || h.processed.Has(storageName, "evt-wc") {
		t.Error("tally/claim survived the rollback")
	}

	if err := h.consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if h.tally.Count("ZONE-B", tally.TypeStation, "PACK") != 1 || h.tally.Count("ZONE-B", tally.TypeStation, "QC") != 1 {
		t.Error("retry did not tally both activities")
	}
}

func TestStorageConsumer_DecommissionFailsAfterMutation_RollsBack(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()
	for _, id := range []string{"r1", "r2"} {
		if err := h.consumer.HandleMessage(ctx, registeredEvent(t, id, "LOC-"+id, "Z", "BULK")); err != nil {
			t.Fatal(err)
		}
	}
	decom := locationSlotEvent(t, "d1", "LocationSlotDecommissioned", time.Now().UTC(), map[string]any{"locationCode": "LOC-r1"})

	h.flaky.failNext(1)
	if err := h.consumer.HandleMessage(ctx, decom); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want injected", err)
	}
	if h.tally.Count("Z", tally.TypeLocation, "BULK") != 2 || h.processed.Has(storageName, "d1") {
		t.Fatalf("decommission leaked state: tally=%d claimed=%v", h.tally.Count("Z", tally.TypeLocation, "BULK"), h.processed.Has(storageName, "d1"))
	}
	// The slot registration was restored too: the retry decrements for real.
	if err := h.consumer.HandleMessage(ctx, decom); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if got := h.tally.Count("Z", tally.TypeLocation, "BULK"); got != 1 {
		t.Fatalf("after retry tally = %d, want 1", got)
	}
}

func TestStorageConsumer_DeterministicBadMessages_ReturnNilWithoutTouchingTheDB(t *testing.T) {
	now := time.Now().UTC()
	tests := map[string][]byte{
		"not a CloudEvent":         []byte("definitely not a cloudevent"),
		"unknown type":             locationSlotEvent(t, "e", "SomethingElse", now, map[string]any{"locationCode": "X"}),
		"payload not an object":    arrayPayloadEvent(t, "LocationSlotRegistered", now),
		"zoneId wrong JSON type":   locationSlotEvent(t, "e", "LocationSlotRegistered", now, map[string]any{"locationCode": "X", "zoneId": 42}),
		"missing locationCode":     locationSlotEvent(t, "e", "LocationSlotRegistered", now, map[string]any{"zoneId": "Z", "locationType": "BULK", "locationCode": ""}),
		"missing zoneId":           locationSlotEvent(t, "e", "LocationSlotRegistered", now, map[string]any{"locationCode": "X", "locationType": "BULK", "zoneId": ""}),
		"storage without type":     locationSlotEvent(t, "e", "LocationSlotRegistered", now, map[string]any{"locationCode": "X", "zoneId": "Z", "locationType": ""}),
		"workcenter no activities": locationSlotEvent(t, "e", "LocationSlotRegistered", now, map[string]any{"locationCode": "X", "zoneId": "Z", "role": "WorkCenter"}),
		"unrecognized role":        locationSlotEvent(t, "e", "LocationSlotRegistered", now, map[string]any{"locationCode": "X", "zoneId": "Z", "role": "Wizard"}),
		"decommission empty code":  locationSlotEvent(t, "e", "LocationSlotDecommissioned", now, map[string]any{"locationCode": ""}),
		"decommission bad payload": arrayPayloadEvent(t, "LocationSlotDecommissioned", now),
	}
	for name, msg := range tests {
		t.Run(name, func(t *testing.T) {
			h := newStorageHarness()
			if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
				t.Fatalf("deterministic problem returned %v; it must be nil so the message is committed past, not retried", err)
			}
			if n := h.uow.calls.Load(); n != 0 {
				t.Errorf("opened %d unit(s) of work for a message that can never succeed", n)
			}
		})
	}
}
