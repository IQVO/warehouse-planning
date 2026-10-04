package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
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

func workCenterEvent(t *testing.T, id, code, zone string, activities ...any) []byte {
	t.Helper()
	return locationSlotEvent(t, id, "LocationSlotRegistered", time.Now().UTC(), map[string]any{
		"locationCode": code, "zoneId": zone, "role": "WorkCenter", "activities": activities,
	})
}

// TestStorageCapacityConsumer_StorageSlotRegistered_IncrementsTally proves a
// normal LocationSlotRegistered (role absent, defaulting to Storage)
// increments the (zoneId, locationType) tally -- and ONLY the tally.
func TestStorageCapacityConsumer_StorageSlotRegistered_IncrementsTally(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-1", "WH1-A-01", "ZONE-A", "BULK")); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got := h.tally.Count("ZONE-A", tally.TypeLocation, "BULK"); got != 1 {
		t.Fatalf("tally = %d, want 1", got)
	}

	// A second, DIFFERENT slot in the same zone/locationType increments to 2.
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-2", "WH1-A-02", "ZONE-A", "BULK")); err != nil {
		t.Fatalf("HandleMessage (second slot): %v", err)
	}
	if got := h.tally.Count("ZONE-A", tally.TypeLocation, "BULK"); got != 2 {
		t.Fatalf("tally = %d after a second distinct slot, want 2", got)
	}
}

// TestStorageCapacityConsumer_WorkCenterSlotRegistered_TalliesEachActivity
// proves a WorkCenter registration with 2 activities increments one STATION
// tally bucket per (upper-cased) activity in the slot's zone.
func TestStorageCapacityConsumer_WorkCenterSlotRegistered_TalliesEachActivity(t *testing.T) {
	h := newStorageHarness()
	if err := h.consumer.HandleMessage(context.Background(), workCenterEvent(t, "evt-wc-1", "WH1-PACK-01", "ZONE-B", "Pack", "QC")); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if got := h.tally.Count("ZONE-B", tally.TypeStation, "PACK"); got != 1 {
		t.Errorf("PACK stations = %d, want 1", got)
	}
	if got := h.tally.Count("ZONE-B", tally.TypeStation, "QC"); got != 1 {
		t.Errorf("QC stations = %d, want 1", got)
	}
}

// TestStorageCapacityConsumer_Decommissioned_DecrementsTally proves a
// LocationSlotDecommissioned decrements the matching tally.
func TestStorageCapacityConsumer_Decommissioned_DecrementsTally(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()
	for _, r := range []struct{ id, code string }{{"evt-1", "WH1-A-01"}, {"evt-2", "WH1-A-02"}} {
		if err := h.consumer.HandleMessage(ctx, registeredEvent(t, r.id, r.code, "ZONE-A", "BULK")); err != nil {
			t.Fatalf("HandleMessage(register %s): %v", r.code, err)
		}
	}
	decommission := locationSlotEvent(t, "evt-decom-1", "LocationSlotDecommissioned", time.Now().UTC(), map[string]any{"locationCode": "WH1-A-01"})
	if err := h.consumer.HandleMessage(ctx, decommission); err != nil {
		t.Fatalf("HandleMessage(decommission): %v", err)
	}
	if got := h.tally.Count("ZONE-A", tally.TypeLocation, "BULK"); got != 1 {
		t.Fatalf("tally = %d after decommissioning one of two slots, want 1", got)
	}
}

// TestStorageCapacityConsumer_DecommissionUntrackedSlot_LogsWarningNotCrash
// proves a decommission for a locationCode never registered is a no-op,
// never a crash and never a negative count.
func TestStorageCapacityConsumer_DecommissionUntrackedSlot_LogsWarningNotCrash(t *testing.T) {
	h := newStorageHarness()
	decommission := locationSlotEvent(t, "evt-decom-unknown", "LocationSlotDecommissioned", time.Now().UTC(), map[string]any{"locationCode": "NEVER-REGISTERED"})
	if err := h.consumer.HandleMessage(context.Background(), decommission); err != nil {
		t.Fatalf("expected an untracked-slot decommission to be a no-op, got %v", err)
	}
}

// TestStorageCapacityConsumer_MalformedMessage_SkippedNotCrashed proves a
// garbage (non-CloudEvents) message never returns an error or panics.
func TestStorageCapacityConsumer_MalformedMessage_SkippedNotCrashed(t *testing.T) {
	h := newStorageHarness()
	if err := h.consumer.HandleMessage(context.Background(), []byte("definitely not a cloudevent")); err != nil {
		t.Fatalf("expected malformed message to be skipped without error, got %v", err)
	}
}

// TestStorageCapacityConsumer_ReplayingSameEventID_DoesNotDoubleCount is the
// critical idempotency proof for the INCREMENT-style tally: redelivering a
// message with the same CloudEvents id is skipped by the processed-event
// claim. A second, DIFFERENT locationCode replayed under the SAME event id
// proves the id-based dedupe path (the tally's own per-locationCode guard
// alone could not catch it).
func TestStorageCapacityConsumer_ReplayingSameEventID_DoesNotDoubleCount(t *testing.T) {
	h := newStorageHarness()
	ctx := context.Background()
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-replay-1", "WH1-A-01", "ZONE-A", "BULK")); err != nil {
		t.Fatalf("first HandleMessage: %v", err)
	}
	if err := h.consumer.HandleMessage(ctx, registeredEvent(t, "evt-replay-1", "WH1-A-99", "ZONE-A", "BULK")); err != nil {
		t.Fatalf("replayed HandleMessage: %v", err)
	}
	if got := h.tally.Count("ZONE-A", tally.TypeLocation, "BULK"); got != 1 {
		t.Fatalf("tally = %d, want the replayed event (same id) skipped, leaving 1", got)
	}
}

// TestStorageCapacityConsumer_PropagatesTallyErrors proves a genuine
// infrastructure error from the tally store is returned, not swallowed.
func TestStorageCapacityConsumer_PropagatesTallyErrors(t *testing.T) {
	h := newStorageHarness()
	boom := errors.New("boom")
	h.consumer.Tally = fakeFailingTally{err: boom}
	if err := h.consumer.HandleMessage(context.Background(), registeredEvent(t, "evt-err", "WH1-A-01", "ZONE-A", "BULK")); !errors.Is(err, boom) {
		t.Fatalf("expected the tally store error to propagate, got %v", err)
	}
}

// The facility consumer registers no ProcessCapacity of its own any more:
// the stations it tallies reach path capacity ONLY through read-time
// composition with the declared StationStandard (ADR 0002). This drives the
// real consumer and the real GetProcessPathCapacity use case over shared
// in-memory repos. Zones are matched to the site by the zone-id prefix
// `<site>-`: SIM2's and an unrelated zone's stations do not count for SIM1.
func TestStorageCapacityConsumer_StationTallyFeedsPathCapacityByComposition(t *testing.T) {
	ctx := context.Background()
	h := newStorageHarness()
	for i := 0; i < 10; i++ {
		code := "SIM1-OPS-WC-" + string(rune('A'+i))
		if err := h.consumer.HandleMessage(ctx, workCenterEvent(t, "evt-sim1-"+code, code, "SIM1-OPS-WC", "Pack")); err != nil {
			t.Fatal(err)
		}
	}
	if err := h.consumer.HandleMessage(ctx, workCenterEvent(t, "evt-sim2", "SIM2-OPS-WC-A", "SIM2-OPS-WC", "Pack")); err != nil {
		t.Fatal(err)
	}
	if err := h.consumer.HandleMessage(ctx, workCenterEvent(t, "evt-orphan", "ORPHAN-WC-A", "ORPHAN-WC", "Pack")); err != nil {
		t.Fatal(err)
	}

	window, start, end := processcapacityWindow(t)
	pcs, paths, standards := memory.NewProcessCapacityRepo(), memory.NewProcessPathRepo(), memory.NewStationStandardRepo()
	pack := processcapacity.NewProcessCapacity("PACK", "SIM1", window)
	rate, err := processcapacity.NewCapacityRate(2500, processcapacity.UnitPackage, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := pack.AddConstraint(processcapacity.ConstraintLabor, rate); err != nil {
		t.Fatal(err)
	}
	if err := pcs.Save(ctx, pack); err != nil {
		t.Fatal(err)
	}
	if _, err := (&usecases.RegisterProcessPath{Repo: paths}).Handle(ctx, usecases.RegisterProcessPathCommand{
		ID: "pack-only", Name: "Pack only", Steps: []processpath.ProcessType{"PACK"},
	}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := (&usecases.DeclareStationStandard{Repo: standards}).Handle(ctx, usecases.DeclareStationStandardCommand{
		Location: "SIM1", ProcessType: "PACK", Quantity: 180, Unit: processcapacity.UnitPackage, Period: time.Hour,
	}); err != nil {
		t.Fatal(err)
	}

	one := 1.0
	got, err := (&usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: standards, Tally: h.tally}).
		Handle(ctx, usecases.GetProcessPathCapacityCommand{
			ProcessPathID: "pack-only", Location: "SIM1", WindowStart: start, WindowEnd: end, PackagesPerOrder: &one,
		})
	if err != nil {
		t.Fatal(err)
	}
	// 10 SIM1 stations x 180 = 1800 < LABOR 2500; the SIM2 and orphan zones add nothing.
	if got.NormalizedRate.Quantity() != 1800 || got.BottleneckConstraint != processcapacity.ConstraintStation {
		t.Fatalf("path capacity = %v bound by %s, want 1800 bound by STATION", got.NormalizedRate.Quantity(), got.BottleneckConstraint)
	}
}

func processcapacityWindow(t *testing.T) (processcapacity.CapacityWindow, time.Time, time.Time) {
	t.Helper()
	start := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	end := start.Add(8 * time.Hour)
	w, err := processcapacity.NewCapacityWindow(start, end)
	if err != nil {
		t.Fatal(err)
	}
	return w, start, end
}
