package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// These tests prove the atomicity fix (defects 2 and 3): a failure on the
// SECOND step of a message (the ProcessCapacity constraint upsert, after
// the tally step already ran) must leave the tally, the constraint and the
// processed-event claim ALL unchanged, HandleMessage must return a
// non-nil error, and a retry must then heal everything. On the old code
// the tally stayed incremented, the claim stayed recorded, and the error
// was swallowed -- so these fail there.

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

// constraintQty returns the effective quantity of the STORAGE constraint
// for zone:locType, or -1 if no ProcessCapacity exists.
func (h storageHarness) constraintQty(t *testing.T, location string) float64 {
	t.Helper()
	return qty(t, h.pcs, "STORAGE", location)
}

func qty(t *testing.T, repo interface {
	FindByProcessLocationWindow(context.Context, processcapacity.ProcessType, string, time.Time, time.Time) (*processcapacity.ProcessCapacity, error)
}, processType, location string) float64 {
	t.Helper()
	pc, err := repo.FindByProcessLocationWindow(context.Background(), processcapacity.ProcessType(processType), location,
		kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if pc == nil {
		return -1
	}
	rate, _, err := pc.EffectiveRate()
	if err != nil {
		t.Fatalf("EffectiveRate: %v", err)
	}
	return rate.Quantity()
}

// assertRolledBack fails unless the first-step tally, the constraint and
// the claim for (zone, locType, eventID) are all absent.
func (h storageHarness) assertRolledBack(t *testing.T, zone, locType, eventID string) {
	t.Helper()
	if got := h.tally.Count(zone, "LOCATION", locType); got != 0 {
		t.Errorf("tally = %d after failed message, want 0 (rolled back)", got)
	}
	if got := h.constraintQty(t, zone+":"+locType); got != -1 {
		t.Errorf("constraint = %v after failed message, want none", got)
	}
	if h.processed.Has(storageName, eventID) {
		t.Error("event stayed claimed after a failed message; the redelivery would be skipped as already processed")
	}
}

// assertApplied fails unless tally, constraint and claim are consistent at want.
func (h storageHarness) assertApplied(t *testing.T, zone, locType, eventID string, want int) {
	t.Helper()
	if got := h.tally.Count(zone, "LOCATION", locType); got != want {
		t.Errorf("tally = %d, want %d", got, want)
	}
	if got := h.constraintQty(t, zone+":"+locType); got != float64(want) {
		t.Errorf("constraint = %v, want %d (tally and constraint consistent)", got, want)
	}
	if !h.processed.Has(storageName, eventID) {
		t.Error("event not claimed after the successful handling")
	}
}

func TestStorageConsumer_RegisterFailsAfterTally_RollsBackTallyConstraintAndClaim(t *testing.T) {
	steps := map[string]func(h storageHarness){
		"constraint Save fails": func(h storageHarness) { h.flaky.failSaveAfter(0, 1) },
		"constraint Find fails": func(h storageHarness) { h.flaky.failFinds = 1 },
	}
	for name, inject := range steps {
		t.Run(name, func(t *testing.T) {
			h := newStorageHarness()
			inject(h)
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
		})
	}
}

// TestStorageConsumer_RedeliveryAfterSuccessAndAlreadyRegisteredNoOp covers
// the 'locationCode already registered' no-op: it must stay safe now that
// everything is atomic. Verified, not assumed:
//  1. redelivering the SAME event id is skipped by the claim;
//  2. a NEW event id for the SAME locationCode hits the already-registered
//     no-op, increments nothing, and leaves the (already correct)
//     constraint alone;
//  3. after a ROLLED-BACK first attempt the tally registration is gone too,
//     so the retry does NOT hit the no-op (the old, never-healing path).
func TestStorageConsumer_RedeliveryAfterSuccessAndAlreadyRegisteredNoOp(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()

	// (3) first attempt fails after the tally step.
	h.flaky.failSaveAfter(0, 1)
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1", "LOC-1", "Z", "BULK")); err == nil {
		t.Fatal("expected the injected failure")
	}
	// Retry with a DIFFERENT event id for the same locationCode (a
	// producer re-emit): must register for real, not no-op.
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1b", "LOC-1", "Z", "BULK")); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if h.tally.Count("Z", "LOCATION", "BULK") != 1 || h.constraintQty(t, "Z:BULK") != 1 {
		t.Fatalf("after retry tally=%d constraint=%v, want 1/1", h.tally.Count("Z", "LOCATION", "BULK"), h.constraintQty(t, "Z:BULK"))
	}

	// (1) same event id redelivered after success: skipped.
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1b", "LOC-1", "Z", "BULK")); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	// (2) new event id, same locationCode: no-op.
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1c", "LOC-1", "Z", "BULK")); err != nil {
		t.Fatalf("re-emit: %v", err)
	}
	if h.tally.Count("Z", "LOCATION", "BULK") != 1 || h.constraintQty(t, "Z:BULK") != 1 {
		t.Fatalf("redelivery double-counted: tally=%d constraint=%v", h.tally.Count("Z", "LOCATION", "BULK"), h.constraintQty(t, "Z:BULK"))
	}
	if !h.processed.Has(storageName, "evt-1c") {
		t.Error("the no-op event is still recorded as processed (a committed, harmless handling)")
	}
}

// A WorkCenter event updates one constraint per activity. A failure on the
// SECOND activity's upsert must roll back the first activity's constraint
// and the whole tally, not leave PACK updated and QC stale.
func TestStorageConsumer_WorkCenterFailureOnSecondActivity_RollsBackFirstToo(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()
	msg := locationSlotEvent(t, "evt-wc", "LocationSlotRegistered", time.Now().UTC(), map[string]any{
		"locationCode": "WH1-PACK-01", "zoneId": "ZONE-B", "role": "WorkCenter", "activities": []any{"Pack", "QC"},
	})

	h.flaky.failSaveAfter(1, 1) // PACK saves, QC fails
	if err := h.consumer.HandleMessage(ctx, msg); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want injected", err)
	}
	if got := qty(t, h.pcs, "PACK", "ZONE-B"); got != -1 {
		t.Errorf("PACK constraint = %v after failed message, want rolled back", got)
	}
	if h.tally.Count("ZONE-B", "STATION", "PACK") != 0 || h.tally.Count("ZONE-B", "STATION", "QC") != 0 || h.processed.Has(storageName, "evt-wc") {
		t.Error("tally/claim survived the rollback")
	}

	if err := h.consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if qty(t, h.pcs, "PACK", "ZONE-B") != 1 || qty(t, h.pcs, "QC", "ZONE-B") != 1 {
		t.Error("retry did not register both STATION constraints")
	}
}

func TestStorageConsumer_DecommissionFailsAfterTally_RollsBack(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()
	for _, id := range []string{"r1", "r2"} {
		code := "LOC-" + id
		if err := h.consumer.HandleMessage(ctx, registeredEvent(t, id, code, "Z", "BULK")); err != nil {
			t.Fatal(err)
		}
	}
	decom := locationSlotEvent(t, "d1", "LocationSlotDecommissioned", time.Now().UTC(), map[string]any{"locationCode": "LOC-r1"})

	h.flaky.failSaveAfter(0, 1)
	if err := h.consumer.HandleMessage(ctx, decom); !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want injected", err)
	}
	if h.tally.Count("Z", "LOCATION", "BULK") != 2 || h.constraintQty(t, "Z:BULK") != 2 || h.processed.Has(storageName, "d1") {
		t.Fatalf("decommission leaked state: tally=%d constraint=%v claimed=%v",
			h.tally.Count("Z", "LOCATION", "BULK"), h.constraintQty(t, "Z:BULK"), h.processed.Has(storageName, "d1"))
	}
	// The slot registration was restored too: the retry decrements for real.
	if err := h.consumer.HandleMessage(ctx, decom); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if h.tally.Count("Z", "LOCATION", "BULK") != 1 || h.constraintQty(t, "Z:BULK") != 1 {
		t.Fatalf("after retry tally=%d constraint=%v, want 1/1", h.tally.Count("Z", "LOCATION", "BULK"), h.constraintQty(t, "Z:BULK"))
	}
}

// Defect 3's other half: a domain-validation rejection is skipped (nil, no
// retry, tally kept, event claimed) while an infrastructure error is not.
func TestStorageConsumer_DomainValidationErrorIsSkippedNotRolledBack(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()

	// Seed a STORAGE/Z:BULK aggregate whose native unit (UNIT) differs from
	// the unit the consumer registers (LINE) -> processcapacity.ErrUnitMismatch.
	w, _ := processcapacity.NewCapacityWindow(kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
	pc := processcapacity.NewProcessCapacity("STORAGE", "Z:BULK", w)
	rate, _ := processcapacity.NewCapacityRate(5, processcapacity.UnitUnit, time.Hour)
	if err := pc.AddConstraint(processcapacity.ConstraintEquipment, rate); err != nil {
		t.Fatal(err)
	}
	if err := h.pcs.Save(ctx, pc); err != nil {
		t.Fatal(err)
	}

	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1", "LOC-1", "Z", "BULK")); err != nil {
		t.Fatalf("a deterministic domain rejection must not be an error (it would retry forever), got %v", err)
	}
	if h.tally.Count("Z", "LOCATION", "BULK") != 1 {
		t.Error("tally was rolled back by a domain-validation skip; it must be kept")
	}
	if !h.processed.Has(storageName, "evt-1") {
		t.Error("event not marked processed after a deterministic skip; it would be redelivered forever")
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
