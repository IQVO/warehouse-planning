package kafka

import (
	"errors"
	"strings"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// The ANALYTICS goldens pin the exact bytes of each event on
// warehouse.warehouse-planning.analytics: same `type`, same `id`, source,
// subject, time and key as the integration message, `dataschema` on the
// analytics stream, and (Published only) the additive binding_constraint.
// They sit next to the UNCHANGED integration goldens in encoder_test.go.

func goldenAnalyticsEncoder() *AnalyticsEncoder {
	return &AnalyticsEncoder{NewID: func() string { return goldenEvtID }}
}

func analyticsGoldenCases() []struct {
	name  string
	event capacityplan.Event
	want  string
} {
	h := capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}
	return []struct {
		name  string
		event capacityplan.Event
		want  string
	}{
		{
			name: "CapacityPlanCreated",
			event: capacityplan.CapacityPlanCreated{
				Header: h, WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
				WindowStart: goldenStart, WindowEnd: goldenEnd,
				AssignedDemand: 12000, PathCapacity: 1000, CapacityOverWindow: 8000, Shortage: 4000,
				BottleneckStep: "REBIN",
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:analytics:CapacityPlanCreated:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","assigned_demand":12000,"path_capacity":1000,"capacity_over_window":8000,"shortage":4000,"bottleneck_step":"REBIN","status":"DRAFT"}}`,
		},
		{
			name: "CapacityPlanPublished",
			event: capacityplan.CapacityPlanPublished{
				Header: h, WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
				WindowStart: goldenStart, WindowEnd: goldenEnd,
				AssignedDemand: 12000, PathCapacity: 1000, CapacityOverWindow: 8000, Shortage: 4000,
				BottleneckStep: "REBIN", BottleneckConstraint: "STATION",
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:analytics:CapacityPlanPublished:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","assigned_demand":12000,"path_capacity":1000,"capacity_over_window":8000,"shortage":4000,"bottleneck_step":"REBIN","published_at":"2026-10-04T21:45:10Z","binding_constraint":"STATION"}}`,
		},
		{
			name: "CapacityShortageDetected",
			event: capacityplan.CapacityShortageDetected{
				Header: h, WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
				WindowStart: goldenStart, WindowEnd: goldenEnd,
				AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000, BottleneckStep: "REBIN",
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:analytics:CapacityShortageDetected:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","assigned_demand":12000,"capacity_over_window":8000,"shortage":4000,"bottleneck_step":"REBIN"}}`,
		},
		{
			name: "BottleneckDetected",
			event: capacityplan.BottleneckDetected{
				Header: h, WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
				WindowStart: goldenStart, WindowEnd: goldenEnd, BottleneckStep: "REBIN", PathCapacity: 1000,
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:analytics:BottleneckDetected:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","bottleneck_step":"REBIN","path_capacity":1000}}`,
		},
	}
}

func TestGolden_AnalyticsTypes(t *testing.T) {
	for _, tc := range analyticsGoldenCases() {
		t.Run(tc.name, func(t *testing.T) {
			msgs, err := goldenAnalyticsEncoder().Encode(tc.event)
			if err != nil || len(msgs) != 1 {
				t.Fatalf("Encode = %d msgs, %v", len(msgs), err)
			}
			m := msgs[0]
			if string(m.Value) != tc.want {
				t.Errorf("value =\n%s\nwant\n%s", m.Value, tc.want)
			}
			if m.Topic != "warehouse.warehouse-planning.analytics" {
				t.Errorf("Topic = %q", m.Topic)
			}
			if wantType := "com.warehouse.wes.warehouse-planning.capacityplan." + tc.name; m.EventType != wantType {
				t.Errorf("EventType = %q, want %q (the SAME type as the integration topic)", m.EventType, wantType)
			}
			if m.EventID != goldenEvtID || m.Subject != goldenPlanID || string(m.Key) != goldenPlanID {
				t.Errorf("id/subject/key = %q/%q/%q", m.EventID, m.Subject, m.Key)
			}
			if wantSchema := "urn:warehouse:warehouse-planning:analytics:" + tc.name + ":v1"; m.DataSchema != wantSchema {
				t.Errorf("DataSchema = %q, want %q", m.DataSchema, wantSchema)
			}
			assertGoldenHeaderAndDecode(t, m)
		})
	}
}

func TestAnalyticsEncoder_TopicOverrideAndRandomIDs(t *testing.T) {
	h := capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}
	msgs, err := (&AnalyticsEncoder{Topic: "unique.analytics"}).Encode(
		capacityplan.CapacityPlanCreated{Header: h}, capacityplan.BottleneckDetected{Header: h})
	if err != nil || len(msgs) != 2 {
		t.Fatalf("Encode = %d, %v", len(msgs), err)
	}
	if msgs[0].Topic != "unique.analytics" || msgs[1].Topic != "unique.analytics" {
		t.Errorf("topics = %q, %q", msgs[0].Topic, msgs[1].Topic)
	}
	if len(msgs[0].EventID) != 36 || msgs[0].EventID == msgs[1].EventID {
		t.Errorf("ids = %q, %q: want distinct UUIDs", msgs[0].EventID, msgs[1].EventID)
	}
}

func TestAnalyticsEncoder_FailsOnAnUnknownEventInsteadOfDroppingIt(t *testing.T) {
	_, err := goldenAnalyticsEncoder().Encode(unpublishedEvent{capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}})
	if err == nil || !strings.Contains(err.Error(), "SomethingElse") {
		t.Fatalf("err = %v, want a not-published error naming the event", err)
	}
}

// The binding constraint is a plain string on the wire; an empty one (a plan
// stored before it was recorded) is emitted as "" rather than omitted.
func TestAnalyticsPublished_EmptyBindingConstraintIsEmittedAsEmptyString(t *testing.T) {
	msgs, err := goldenAnalyticsEncoder().Encode(capacityplan.CapacityPlanPublished{
		Header: capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}, BottleneckStep: "REBIN",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(msgs[0].Value), `"binding_constraint":""}}`) {
		t.Errorf("value = %s", msgs[0].Value)
	}
}

func TestFanoutEncoder_OneOccurrenceTwoMessagesSameIDAndType(t *testing.T) {
	ids := []string{"id-created", "id-published"}
	f := NewFanoutEncoder()
	f.NewID = func() string { id := ids[0]; ids = ids[1:]; return id }
	h := capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}

	msgs, err := f.Encode(
		capacityplan.CapacityPlanCreated{Header: h, BottleneckStep: "REBIN"},
		capacityplan.CapacityPlanPublished{Header: h, BottleneckStep: "REBIN", BottleneckConstraint: "LABOR"},
	)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(msgs) != 4 {
		t.Fatalf("got %d messages, want 4 (integration + analytics per event)", len(msgs))
	}
	want := []struct{ id, topic, schemaStream, event string }{
		{"id-created", Topic, "events", "CapacityPlanCreated"},
		{"id-created", AnalyticsTopic, "analytics", "CapacityPlanCreated"},
		{"id-published", Topic, "events", "CapacityPlanPublished"},
		{"id-published", AnalyticsTopic, "analytics", "CapacityPlanPublished"},
	}
	for i, w := range want {
		m := msgs[i]
		if m.EventID != w.id || m.Topic != w.topic {
			t.Errorf("msg %d id/topic = %q/%q, want %q/%q", i, m.EventID, m.Topic, w.id, w.topic)
		}
		if m.EventType != "com.warehouse.wes.warehouse-planning.capacityplan."+w.event {
			t.Errorf("msg %d type = %q", i, m.EventType)
		}
		if m.DataSchema != "urn:warehouse:warehouse-planning:"+w.schemaStream+":"+w.event+":v1" {
			t.Errorf("msg %d dataschema = %q", i, m.DataSchema)
		}
		if !strings.Contains(string(m.Value), `"id":"`+w.id+`"`) {
			t.Errorf("msg %d value does not carry the CloudEvents id %q: %s", i, w.id, m.Value)
		}
	}
}

// The integration half of the fan-out is byte-identical to what the plain
// Encoder produces for the same id: adding the analytics stream changed
// nothing about the integration contract.
func TestFanoutEncoder_IntegrationMessagesAreByteIdenticalToTheIntegrationEncoder(t *testing.T) {
	f := NewFanoutEncoder()
	f.NewID = func() string { return goldenEvtID }
	for _, tc := range analyticsGoldenCases() {
		fan, err := f.Encode(tc.event)
		if err != nil || len(fan) != 2 {
			t.Fatalf("%s: Fanout = %d, %v", tc.name, len(fan), err)
		}
		plain, err := goldenEncoder().Encode(tc.event)
		if err != nil || len(plain) != 1 {
			t.Fatalf("%s: Encode = %d, %v", tc.name, len(plain), err)
		}
		if !equalMessages(fan[0], plain[0]) {
			t.Errorf("%s: integration message changed under the fan-out:\n%+v\nvs\n%+v", tc.name, fan[0], plain[0])
		}
		analytics, err := goldenAnalyticsEncoder().Encode(tc.event)
		if err != nil || !equalMessages(fan[1], analytics[0]) {
			t.Errorf("%s: analytics message differs from AnalyticsEncoder's: %v", tc.name, err)
		}
	}
}

func TestFanoutEncoder_RandomIDsDifferPerOccurrenceButMatchWithinOne(t *testing.T) {
	h := capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}
	msgs, err := NewFanoutEncoder().Encode(capacityplan.CapacityPlanPublished{Header: h}, capacityplan.BottleneckDetected{Header: h})
	if err != nil || len(msgs) != 4 {
		t.Fatalf("Encode = %d, %v", len(msgs), err)
	}
	if msgs[0].EventID != msgs[1].EventID || msgs[2].EventID != msgs[3].EventID || msgs[0].EventID == msgs[2].EventID {
		t.Errorf("ids = %q %q %q %q", msgs[0].EventID, msgs[1].EventID, msgs[2].EventID, msgs[3].EventID)
	}
	if len(msgs[0].EventID) != 36 {
		t.Errorf("id %q is not a UUID", msgs[0].EventID)
	}
}

func TestFanoutEncoder_FailsOnAnUnknownEvent(t *testing.T) {
	_, err := NewFanoutEncoder().Encode(unpublishedEvent{capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}})
	if err == nil {
		t.Fatal("want an error for an event neither stream publishes")
	}
	var empty []capacityplan.Event
	if msgs, err := NewFanoutEncoder().Encode(empty...); err != nil || len(msgs) != 0 {
		t.Fatalf("no events = %v, %v", msgs, errors.Unwrap(err))
	}
}

func equalMessages(a, b outbox.Message) bool {
	if a.EventID != b.EventID || a.Topic != b.Topic || a.EventType != b.EventType || a.Subject != b.Subject ||
		a.DataSchema != b.DataSchema || string(a.Key) != string(b.Key) || string(a.Value) != string(b.Value) ||
		len(a.Headers) != len(b.Headers) {
		return false
	}
	for i := range a.Headers {
		if a.Headers[i] != b.Headers[i] {
			return false
		}
	}
	return true
}
