package kafka

import (
	"bytes"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// The golden files testdata/golden/CapacityPlanSuperseded.{events,analytics}.json
// pin the exact bytes of CapacityPlanSuperseded (docs/adr/0024 decision 3) on
// both streams. order-management consumes it against this wire (its ADR
// 0052), so any change here is a contract change.

const goldenNewerPlanID = "9d1f0c2a-7b3e-4f5a-8c6d-2e4b1a0f9c87"

func goldenSuperseded() capacityplan.CapacityPlanSuperseded {
	return capacityplan.CapacityPlanSuperseded{
		Header:       capacityplan.Header{PlanID: goldenPlanID, At: goldenAt},
		SupersededBy: goldenNewerPlanID,
		WarehouseID:  "WH-1",
		SiteID:       "SIM1",
		Location:     "PATH-ZONE-A",
		PathID:       "pick-rebin-pack",
		WindowStart:  goldenStart,
		WindowEnd:    goldenEnd,
	}
}

func TestGolden_CapacityPlanSupersededOnBothStreams(t *testing.T) {
	for _, tc := range []struct {
		stream, topic string
		enc           func() planEncoder
	}{
		{"events", "warehouse.warehouse-planning.events", func() planEncoder { return goldenEncoder() }},
		{"analytics", "warehouse.warehouse-planning.analytics", func() planEncoder { return goldenAnalyticsEncoder() }},
	} {
		t.Run(tc.stream, func(t *testing.T) {
			msgs, err := tc.enc().EncodeCapacityPlanEvents(goldenSuperseded())
			if err != nil || len(msgs) != 1 {
				t.Fatalf("Encode = %d msgs, %v", len(msgs), err)
			}
			assertSupersededMessage(t, msgs[0], tc.stream, tc.topic)
		})
	}
}

// planEncoder is the CapacityPlan half of ports.EventEncoder.
type planEncoder interface {
	EncodeCapacityPlanEvents(events ...capacityplan.Event) ([]outbox.Message, error)
}

func assertSupersededMessage(t *testing.T, m outbox.Message, stream, topic string) {
	t.Helper()
	file := "CapacityPlanSuperseded." + stream + ".json"
	if want := readGolden(t, file); !bytes.Equal(m.Value, want) {
		t.Errorf("value =\n%s\nwant (testdata/golden/%s)\n%s", m.Value, file, want)
	}
	if m.Topic != topic {
		t.Errorf("Topic = %q, want %q", m.Topic, topic)
	}
	if want := "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanSuperseded"; m.EventType != want {
		t.Errorf("EventType = %q, want %q", m.EventType, want)
	}
	if m.EventID != goldenEvtID || m.Subject != goldenPlanID || string(m.Key) != goldenPlanID {
		t.Errorf("id/subject/key = %q/%q/%q, want the OLDER plan %s as subject and key", m.EventID, m.Subject, m.Key, goldenPlanID)
	}
	if want := "urn:warehouse:warehouse-planning:" + stream + ":CapacityPlanSuperseded:v1"; m.DataSchema != want {
		t.Errorf("DataSchema = %q, want %q", m.DataSchema, want)
	}
	assertGoldenHeaderAndDecode(t, m)
}

// The fan-out writes the two golden messages, integration first, one id.
func TestGolden_FanoutWritesTheTwoSupersededMessages(t *testing.T) {
	msgs, err := goldenFanout().EncodeCapacityPlanEvents(goldenSuperseded())
	if err != nil || len(msgs) != 2 {
		t.Fatalf("Fanout = %d, %v", len(msgs), err)
	}
	assertSupersededMessage(t, msgs[0], "events", "warehouse.warehouse-planning.events")
	assertSupersededMessage(t, msgs[1], "analytics", "warehouse.warehouse-planning.analytics")
}

// site_id is always on the wire, "" for a plan stored before migration 0008
// (order-management falls back to it only when location is absent).
func TestCapacityPlanSuperseded_EmptySiteIDIsEmittedAsEmptyString(t *testing.T) {
	ev := goldenSuperseded()
	ev.SiteID = ""
	msgs, err := goldenEncoder().EncodeCapacityPlanEvents(ev)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msgs[0].Value), `"site_id":"","location":"PATH-ZONE-A"`) {
		t.Errorf("value = %s", msgs[0].Value)
	}
}
