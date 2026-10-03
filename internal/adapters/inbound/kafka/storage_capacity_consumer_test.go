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
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// locationSlotEvent builds the exact CloudEvents 1.0 structured-mode
// bytes facility-layout publishes for either LocationSlotRegistered or
// LocationSlotDecommissioned (per docs/adr/0001-...'s Addendum), using
// the raw sdk-go event package directly to simulate the upstream
// producer -- see shiftPlanCommittedEvent's doc comment for why.
func locationSlotEvent(t *testing.T, id, eventName string, occurredAt time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/facility-layout")
	e.SetType("com.warehouse.wms.facility-layout.locationslot." + eventName)
	e.SetSubject(data["locationCode"].(string))
	e.SetTime(occurredAt)
	e.SetDataSchema("urn:warehouse:facility-layout:events:" + eventName + ":v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func newStorageConsumer() (*kafkaconsumer.StorageCapacityConsumer, *memory.ProcessCapacityRepo) {
	pcRepo := memory.NewProcessCapacityRepo()
	register := &usecases.RegisterProcessCapacityConstraint{Repo: pcRepo}
	return &kafkaconsumer.StorageCapacityConsumer{
		Register:        register,
		Tally:           memory.NewStorageTallyRepo(),
		ProcessedEvents: memory.NewProcessedEventRepo(),
		Logger:          testLogger(),
	}, pcRepo
}

// TestStorageCapacityConsumer_StorageSlotRegistered_IncrementsTally
// proves a normal LocationSlotRegistered (role absent, defaulting to
// Storage) increments the (zoneId, locationType) tally and registers a
// LOCATION CapacityConstraint with quantity 1.
func TestStorageCapacityConsumer_StorageSlotRegistered_IncrementsTally(t *testing.T) {
	c, repo := newStorageConsumer()
	value := locationSlotEvent(t, "evt-1", "LocationSlotRegistered", time.Now().UTC(), map[string]any{
		"locationCode": "WH1-A-01",
		"zoneId":       "ZONE-A",
		"locationType": "BULK",
	})

	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	pc, err := repo.FindByProcessLocationWindow(context.Background(), "STORAGE", "ZONE-A:BULK", kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow: %v", err)
	}
	if pc == nil {
		t.Fatal("expected a STORAGE/ZONE-A:BULK ProcessCapacity to have been registered")
	}
	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		t.Fatalf("EffectiveRate: %v", err)
	}
	if binding != processcapacity.ConstraintLocation {
		t.Fatalf("expected LOCATION binding, got %v", binding)
	}
	if effective.Quantity() != 1 {
		t.Fatalf("expected tally count 1, got %v", effective.Quantity())
	}

	// A second, DIFFERENT slot in the same zone/locationType increments
	// the SAME ProcessCapacity's LOCATION constraint to 2.
	value2 := locationSlotEvent(t, "evt-2", "LocationSlotRegistered", time.Now().UTC(), map[string]any{
		"locationCode": "WH1-A-02",
		"zoneId":       "ZONE-A",
		"locationType": "BULK",
	})
	if err := c.HandleMessage(context.Background(), value2); err != nil {
		t.Fatalf("HandleMessage (second slot): %v", err)
	}
	pc, err = repo.FindByProcessLocationWindow(context.Background(), "STORAGE", "ZONE-A:BULK", kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow: %v", err)
	}
	effective, _, err = pc.EffectiveRate()
	if err != nil {
		t.Fatalf("EffectiveRate: %v", err)
	}
	if effective.Quantity() != 2 {
		t.Fatalf("expected tally count 2 after a second distinct slot, got %v", effective.Quantity())
	}
}

// TestStorageCapacityConsumer_WorkCenterSlotRegistered_CreatesStationConstraintsForEachActivity
// proves a WorkCenter registration with 2 activities creates/updates 2
// distinct STATION constraints (one ProcessCapacity per activity, same
// zone).
func TestStorageCapacityConsumer_WorkCenterSlotRegistered_CreatesStationConstraintsForEachActivity(t *testing.T) {
	c, repo := newStorageConsumer()
	value := locationSlotEvent(t, "evt-wc-1", "LocationSlotRegistered", time.Now().UTC(), map[string]any{
		"locationCode": "WH1-PACK-01",
		"zoneId":       "ZONE-B",
		"role":         "WorkCenter",
		"activities":   []any{"Pack", "QC"},
	})

	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	packPC, err := repo.FindByProcessLocationWindow(context.Background(), "PACK", "ZONE-B", kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow(PACK): %v", err)
	}
	if packPC == nil {
		t.Fatal("expected a PACK/ZONE-B ProcessCapacity to have been registered")
	}
	if _, binding, err := packPC.EffectiveRate(); err != nil || binding != processcapacity.ConstraintStation {
		t.Fatalf("expected STATION binding for PACK, got %v (err %v)", binding, err)
	}

	qcPC, err := repo.FindByProcessLocationWindow(context.Background(), "QC", "ZONE-B", kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow(QC): %v", err)
	}
	if qcPC == nil {
		t.Fatal("expected a QC/ZONE-B ProcessCapacity to have been registered")
	}
	if _, binding, err := qcPC.EffectiveRate(); err != nil || binding != processcapacity.ConstraintStation {
		t.Fatalf("expected STATION binding for QC, got %v (err %v)", binding, err)
	}
}

// TestStorageCapacityConsumer_Decommissioned_DecrementsTally proves a
// LocationSlotDecommissioned decrements the matching tally and
// re-registers the constraint with the new (lower) count.
func TestStorageCapacityConsumer_Decommissioned_DecrementsTally(t *testing.T) {
	c, repo := newStorageConsumer()
	register := func(id, code string) {
		value := locationSlotEvent(t, id, "LocationSlotRegistered", time.Now().UTC(), map[string]any{
			"locationCode": code,
			"zoneId":       "ZONE-A",
			"locationType": "BULK",
		})
		if err := c.HandleMessage(context.Background(), value); err != nil {
			t.Fatalf("HandleMessage(register %s): %v", code, err)
		}
	}
	register("evt-1", "WH1-A-01")
	register("evt-2", "WH1-A-02")

	decommission := locationSlotEvent(t, "evt-decom-1", "LocationSlotDecommissioned", time.Now().UTC(), map[string]any{
		"locationCode": "WH1-A-01",
	})
	if err := c.HandleMessage(context.Background(), decommission); err != nil {
		t.Fatalf("HandleMessage(decommission): %v", err)
	}

	pc, err := repo.FindByProcessLocationWindow(context.Background(), "STORAGE", "ZONE-A:BULK", kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow: %v", err)
	}
	if pc == nil {
		t.Fatal("expected the ProcessCapacity to still exist after one decommission")
	}
	effective, _, err := pc.EffectiveRate()
	if err != nil {
		t.Fatalf("EffectiveRate: %v", err)
	}
	if effective.Quantity() != 1 {
		t.Fatalf("expected tally count 1 after decommissioning one of two slots, got %v", effective.Quantity())
	}
}

// TestStorageCapacityConsumer_DecommissionUntrackedSlot_LogsWarningNotCrash
// proves a decommission for a locationCode never registered is a no-op,
// never a crash and never a negative count.
func TestStorageCapacityConsumer_DecommissionUntrackedSlot_LogsWarningNotCrash(t *testing.T) {
	c, _ := newStorageConsumer()
	decommission := locationSlotEvent(t, "evt-decom-unknown", "LocationSlotDecommissioned", time.Now().UTC(), map[string]any{
		"locationCode": "NEVER-REGISTERED",
	})
	if err := c.HandleMessage(context.Background(), decommission); err != nil {
		t.Fatalf("expected an untracked-slot decommission to be a no-op, got %v", err)
	}
}

// TestStorageCapacityConsumer_MalformedMessage_SkippedNotCrashed proves a
// garbage (non-CloudEvents) message never returns an error or panics.
func TestStorageCapacityConsumer_MalformedMessage_SkippedNotCrashed(t *testing.T) {
	c, _ := newStorageConsumer()
	if err := c.HandleMessage(context.Background(), []byte("definitely not a cloudevent")); err != nil {
		t.Fatalf("expected malformed message to be skipped without error, got %v", err)
	}
}

// TestStorageCapacityConsumer_ReplayingSameEventID_DoesNotDoubleCount is
// the critical idempotency proof for the INCREMENT-style tally: without
// the ProcessedEventRepository claim, redelivering the exact same
// LocationSlotRegistered message would increment the tally a second
// time. This test would FAIL if HandleMessage didn't dedupe on
// CloudEvents id (the memory.StorageTallyRepo's own
// per-locationCode-already-registered guard would ALSO prevent this
// specific redelivery from double counting, so this test additionally
// proves the id-based dedupe path is exercised by asserting via a
// second, DIFFERENT locationCode replayed under the SAME event id --
// which the per-locationCode guard alone could not catch).
func TestStorageCapacityConsumer_ReplayingSameEventID_DoesNotDoubleCount(t *testing.T) {
	c, repo := newStorageConsumer()
	occurredAt := time.Now().UTC()

	value := locationSlotEvent(t, "evt-replay-1", "LocationSlotRegistered", occurredAt, map[string]any{
		"locationCode": "WH1-A-01",
		"zoneId":       "ZONE-A",
		"locationType": "BULK",
	})
	if err := c.HandleMessage(context.Background(), value); err != nil {
		t.Fatalf("first HandleMessage: %v", err)
	}

	// Redelivered with the SAME CloudEvents id but a DIFFERENT
	// locationCode -- if dedupe were only the tally repo's
	// already-registered-locationCode guard (not the CloudEvents id
	// claim), this second, distinct locationCode would still increment
	// the tally to 2.
	replay := locationSlotEvent(t, "evt-replay-1", "LocationSlotRegistered", occurredAt, map[string]any{
		"locationCode": "WH1-A-99",
		"zoneId":       "ZONE-A",
		"locationType": "BULK",
	})
	if err := c.HandleMessage(context.Background(), replay); err != nil {
		t.Fatalf("replayed HandleMessage: %v", err)
	}

	pc, err := repo.FindByProcessLocationWindow(context.Background(), "STORAGE", "ZONE-A:BULK", kafkaconsumer.StandingWindowStart, kafkaconsumer.StandingWindowEnd)
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
	if effective.Quantity() != 1 {
		t.Fatalf("expected the replayed event (same id) to be skipped, leaving tally at 1, got %v", effective.Quantity())
	}
}

// TestStorageCapacityConsumer_PropagatesTallyErrors proves a genuine
// infrastructure error from the tally store is returned, not swallowed.
func TestStorageCapacityConsumer_PropagatesTallyErrors(t *testing.T) {
	c, _ := newStorageConsumer()
	boom := errors.New("boom")
	c.Tally = fakeFailingTally{err: boom}

	value := locationSlotEvent(t, "evt-err", "LocationSlotRegistered", time.Now().UTC(), map[string]any{
		"locationCode": "WH1-A-01",
		"zoneId":       "ZONE-A",
		"locationType": "BULK",
	})
	if err := c.HandleMessage(context.Background(), value); !errors.Is(err, boom) {
		t.Fatalf("expected the tally store error to propagate, got %v", err)
	}
}
