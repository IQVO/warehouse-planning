package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// The golden tests below pin the EXACT bytes of every published type: all
// CloudEvents attributes, the data payload (snake_case), the Kafka key and
// the content-type header. A change to any of them is a contract change
// (breaking => a new .v2 type + dataschema, see integration-events.md).

const (
	goldenPlanID = "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10"
	goldenEvtID  = "6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33"
	wantCT       = "application/cloudevents+json; charset=UTF-8"
)

var (
	goldenStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	goldenEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	// 18:45:10 in BRT (UTC-3) == 21:45:10Z: proves `time` is emitted in UTC.
	goldenAt = time.Date(2026, 10, 4, 18, 45, 10, 0, time.FixedZone("BRT", -3*3600))
)

func goldenEncoder() *Encoder { return &Encoder{NewID: func() string { return goldenEvtID }} }

func TestGolden_PublishedTypes(t *testing.T) {
	h := capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}
	cases := []struct {
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
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanCreated","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:events:CapacityPlanCreated:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","assigned_demand":12000,"path_capacity":1000,"capacity_over_window":8000,"shortage":4000,"bottleneck_step":"REBIN","status":"DRAFT"}}`,
		},
		{
			name: "CapacityPlanPublished",
			event: capacityplan.CapacityPlanPublished{
				Header: h, WarehouseID: "WH-1", SiteID: "SIM1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
				WindowStart: goldenStart, WindowEnd: goldenEnd,
				AssignedDemand: 12000, PathCapacity: 1000, CapacityOverWindow: 8000, Shortage: 4000,
				BottleneckStep: "REBIN",
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:events:CapacityPlanPublished:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","site_id":"SIM1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","assigned_demand":12000,"path_capacity":1000,"capacity_over_window":8000,"shortage":4000,"bottleneck_step":"REBIN","published_at":"2026-10-04T21:45:10Z"}}`,
		},
		{
			name: "CapacityShortageDetected",
			event: capacityplan.CapacityShortageDetected{
				Header: h, WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
				WindowStart: goldenStart, WindowEnd: goldenEnd,
				AssignedDemand: 12000, CapacityOverWindow: 8000, Shortage: 4000, BottleneckStep: "REBIN",
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.CapacityShortageDetected","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:events:CapacityShortageDetected:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","assigned_demand":12000,"capacity_over_window":8000,"shortage":4000,"bottleneck_step":"REBIN"}}`,
		},
		{
			name: "BottleneckDetected",
			event: capacityplan.BottleneckDetected{
				Header: h, WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
				WindowStart: goldenStart, WindowEnd: goldenEnd, BottleneckStep: "REBIN", PathCapacity: 1000,
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.BottleneckDetected","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:events:BottleneckDetected:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","bottleneck_step":"REBIN","path_capacity":1000}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { assertGolden(t, tc.name, tc.event, tc.want) })
	}
}

func assertGolden(t *testing.T, name string, event capacityplan.Event, want string) {
	t.Helper()
	msgs, err := goldenEncoder().Encode(event)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("Encode = %d msgs, %v", len(msgs), err)
	}
	m := msgs[0]
	if string(m.Value) != want {
		t.Errorf("value =\n%s\nwant\n%s", m.Value, want)
	}
	if m.Topic != "warehouse.warehouse-planning.events" {
		t.Errorf("Topic = %q", m.Topic)
	}
	if wantType := "com.warehouse.wes.warehouse-planning.capacityplan." + name; m.EventType != wantType {
		t.Errorf("EventType = %q, want %q (full CloudEvents type)", m.EventType, wantType)
	}
	if m.EventID != goldenEvtID || m.Subject != goldenPlanID || string(m.Key) != goldenPlanID {
		t.Errorf("id/subject/key = %q/%q/%q", m.EventID, m.Subject, m.Key)
	}
	if wantSchema := "urn:warehouse:warehouse-planning:events:" + name + ":v1"; m.DataSchema != wantSchema {
		t.Errorf("DataSchema = %q, want %q", m.DataSchema, wantSchema)
	}
	assertGoldenHeaderAndDecode(t, m)
}

func assertGoldenHeaderAndDecode(t *testing.T, m outbox.Message) {
	t.Helper()
	ct := cloudevents.ContentTypeHeader()
	if len(m.Headers) != 1 || m.Headers[0].Key != "content-type" || m.Headers[0].Value != wantCT ||
		m.Headers[0].Key != ct.Key || m.Headers[0].Value != string(ct.Value) {
		t.Errorf("Headers = %+v, want exactly the content-type header from cloudevents.ContentTypeHeader()", m.Headers)
	}
	// Round-trips through the validating decoder.
	if ev, err := cloudevents.Decode(m.Value); err != nil || ev.ID() != goldenEvtID || ev.Type() != m.EventType {
		t.Errorf("Decode = %v, %v", ev, err)
	}
}

func TestEncode_FansOutOneMessagePerEventWithDistinctIDs(t *testing.T) {
	h := capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}
	msgs, err := NewEncoder().Encode(
		capacityplan.CapacityPlanPublished{Header: h},
		capacityplan.CapacityShortageDetected{Header: h},
		capacityplan.BottleneckDetected{Header: h},
	)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("got %d messages, want 3", len(msgs))
	}
	seen := map[string]bool{}
	for _, m := range msgs {
		if len(m.EventID) != 36 || seen[m.EventID] {
			t.Errorf("event id %q is not a fresh UUID", m.EventID)
		}
		seen[m.EventID] = true
	}
}

func TestEncode_NoEventsNoMessages(t *testing.T) {
	msgs, err := NewEncoder().Encode()
	if err != nil || len(msgs) != 0 {
		t.Fatalf("Encode() = %v, %v", msgs, err)
	}
}

type unpublishedEvent struct{ capacityplan.Header }

func (unpublishedEvent) EventName() string { return "SomethingElse" }

func TestEncode_RejectsAnUnpublishedEventType(t *testing.T) {
	if _, err := NewEncoder().Encode(unpublishedEvent{}); err == nil {
		t.Fatal("want an error for an event type this adapter does not publish")
	}
}

func TestEncode_PropagatesCloudEventsValidationFailure(t *testing.T) {
	// An event with an empty aggregate id has no subject; the helper rejects it.
	_, err := NewEncoder().Encode(capacityplan.CapacityPlanCreated{Header: capacityplan.Header{At: goldenAt}})
	if err == nil {
		t.Fatal("want an error for an empty subject")
	}
	if errors.Is(err, cloudevents.ErrNotCloudEvent) {
		t.Fatalf("unexpected decode error %v", err)
	}
}

func TestEncode_TopicOverride(t *testing.T) {
	e := &Encoder{NewID: func() string { return goldenEvtID }, Topic: "some.other.topic"}
	msgs, err := e.Encode(capacityplan.BottleneckDetected{Header: capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}})
	if err != nil || msgs[0].Topic != "some.other.topic" {
		t.Fatalf("Encode = %+v, %v; want the overridden topic", msgs, err)
	}
}

// A plan stored before the site id existed (migration 0008) publishes with an
// EMPTY site id: site_id is omitted (omitempty) and the v1 payload stays
// byte-identical to the pre-site_id shape, so legacy consumers and replays
// of the persisted outbox bytes see no change.
func TestGolden_PublishedWithoutSiteIDIsTheLegacyShape(t *testing.T) {
	h := capacityplan.Header{PlanID: goldenPlanID, At: goldenAt}
	msgs, err := goldenEncoder().Encode(capacityplan.CapacityPlanPublished{
		Header: h, WarehouseID: "WH-1", Location: "PATH-ZONE-A", PathID: "pick-rebin-pack",
		WindowStart: goldenStart, WindowEnd: goldenEnd,
		AssignedDemand: 12000, PathCapacity: 1000, CapacityOverWindow: 8000, Shortage: 4000,
		BottleneckStep: "REBIN",
	})
	if err != nil || len(msgs) != 1 {
		t.Fatalf("Encode = %d msgs, %v", len(msgs), err)
	}
	want := `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:events:CapacityPlanPublished:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","assigned_demand":12000,"path_capacity":1000,"capacity_over_window":8000,"shortage":4000,"bottleneck_step":"REBIN","published_at":"2026-10-04T21:45:10Z"}}`
	if string(msgs[0].Value) != want {
		t.Errorf("value =\n%s\nwant\n%s", msgs[0].Value, want)
	}
}

// A legacy v1 payload WITHOUT site_id still decodes through the service's
// own validating CloudEvents decoder (the consumer-side guarantee: the field
// is additive, absence never breaks a decode).
func TestEncode_PublishedLegacyPayloadStillDecodes(t *testing.T) {
	const legacy = `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished","subject":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:events:CapacityPlanPublished:v1","time":"2026-10-04T21:45:10Z","data":{"plan_id":"0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10","warehouse_id":"WH-1","location":"PATH-ZONE-A","path_id":"pick-rebin-pack","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","assigned_demand":12000,"path_capacity":1000,"capacity_over_window":8000,"shortage":4000,"bottleneck_step":"REBIN","published_at":"2026-10-04T21:45:10Z"}}`
	ev, err := cloudevents.Decode([]byte(legacy))
	if err != nil {
		t.Fatalf("a legacy v1 payload without site_id must decode: %v", err)
	}
	var data struct {
		SiteID *string `json:"site_id"`
	}
	if err := ev.DataAs(&data); err != nil {
		t.Fatalf("DataAs: %v", err)
	}
	if data.SiteID != nil {
		t.Errorf("site_id = %q, want absent (nil)", *data.SiteID)
	}
}
