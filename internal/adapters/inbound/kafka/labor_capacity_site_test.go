package kafka_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
)

// ADR 0021: workforce-management's ShiftPlanCommitted names the site in
// `site_code` (canonical) and in `building_id` (deprecated, same value). The
// consumer reads site_code first and falls back to building_id for messages
// produced before site_code existed.

func shiftPlanLineEvent(t *testing.T, id string, at time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/workforce-management")
	e.SetType("com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted")
	e.SetSubject("site/SHIFT-1")
	e.SetTime(at)
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

func shiftLine(site, building string) map[string]any {
	d := map[string]any{"shift_id": "SHIFT-1", "path_id": "pick", "planned_heads": 10, "planned_rate": 40.0, "planned_hours": 8.0}
	if site != "" {
		d["site_code"] = site
	}
	if building != "" {
		d["building_id"] = building
	}
	return d
}

func laborRegisteredAt(t *testing.T, msg []byte, at time.Time, location string) bool {
	t.Helper()
	h := newLaborHarness()
	if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	pc, err := h.pcs.FindByProcessLocationWindow(context.Background(), "PICK", location, at, at.Add(8*time.Hour))
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow: %v", err)
	}
	return pc != nil
}

func TestLaborCapacityConsumer_SiteCodeIsTheCanonicalSite(t *testing.T) {
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	msg := shiftPlanLineEvent(t, "evt-sc", at, shiftLine("WH2", "WH1"))
	if !laborRegisteredAt(t, msg, at, "WH2") {
		t.Fatal("no LABOR constraint at site_code WH2")
	}
	if laborRegisteredAt(t, msg, at, "WH1") {
		t.Fatal("constraint registered at the deprecated building_id, want site_code")
	}
}

func TestLaborCapacityConsumer_BuildingIDIsTheFallbackForOlderMessages(t *testing.T) {
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	if !laborRegisteredAt(t, shiftPlanLineEvent(t, "evt-b", at, shiftLine("", "WH1")), at, "WH1") {
		t.Fatal("no LABOR constraint at building_id WH1 when site_code is absent")
	}
	if !laborRegisteredAt(t, shiftPlanLineEvent(t, "evt-w", at, shiftLine("  ", "WH1")), at, "WH1") {
		t.Fatal("a blank site_code must fall back to building_id")
	}
}

func TestLaborCapacityConsumer_SiteCodeAloneIsEnough(t *testing.T) {
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	if !laborRegisteredAt(t, shiftPlanLineEvent(t, "evt-s", at, shiftLine("WH3", "")), at, "WH3") {
		t.Fatal("no LABOR constraint at site_code WH3 when building_id is absent")
	}
}

func TestLaborCapacityConsumer_NoSiteAtAllIsSkipped(t *testing.T) {
	at := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	h := newLaborHarness()
	if err := h.consumer.HandleMessage(context.Background(), shiftPlanLineEvent(t, "evt-n", at, shiftLine("", ""))); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if n := h.uow.calls.Load(); n != 0 {
		t.Fatalf("unit of work ran %d times for a line with no site, want 0", n)
	}
}
