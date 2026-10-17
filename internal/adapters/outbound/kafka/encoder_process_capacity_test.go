package kafka

import (
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// The golden tests below pin the EXACT bytes of the two published
// ProcessCapacity types: all CloudEvents attributes, the data payload
// (snake_case), the Kafka key (the aggregate's canonical identity
// PICK/location/window) and the content-type header. A change to any of
// them is a contract change (breaking => a new .v2 type + dataschema, see
// integration-events.md).

var (
	goldenPCRate4000 = mustGoldenRate(4000)
	goldenPCRate3500 = mustGoldenRate(3500)
	goldenPCSubject  = "PICK/GOLDEN-ZONE/2026-10-05T08:00:00Z/2026-10-05T16:00:00Z"
)

func mustGoldenRate(q float64) processcapacity.CapacityRate {
	r, err := processcapacity.NewCapacityRate(q, processcapacity.UnitUnit, time.Hour)
	if err != nil {
		panic(err)
	}
	return r
}

func goldenPCHeader() processcapacity.Header {
	return processcapacity.Header{
		ProcessType: "PICK",
		Location:    "GOLDEN-ZONE",
		WindowStart: goldenStart,
		WindowEnd:   goldenEnd,
		At:          goldenAt,
	}
}

func TestGolden_ProcessCapacityEvents(t *testing.T) {
	cases := []struct {
		name  string
		event processcapacity.Event
		want  string
	}{
		{
			name: "ProcessCapacityRegistered",
			event: processcapacity.ProcessCapacityRegistered{
				Header:         goldenPCHeader(),
				ConstraintType: processcapacity.ConstraintLabor,
				Rate:           goldenPCRate4000,
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityRegistered","subject":"` + goldenPCSubject + `","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:events:ProcessCapacityRegistered:v1","time":"2026-10-04T21:45:10Z","data":{"process_type":"PICK","location":"GOLDEN-ZONE","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","constraint_type":"LABOR","quantity":4000,"unit":"UNIT","period_seconds":3600}}`,
		},
		{
			name: "ProcessCapacityChanged",
			event: processcapacity.ProcessCapacityChanged{
				Header:          goldenPCHeader(),
				ConstraintType:  processcapacity.ConstraintLocation,
				PreviousRate:    goldenPCRate4000,
				PreviousBinding: processcapacity.ConstraintLabor,
				NewRate:         goldenPCRate3500,
				NewBinding:      processcapacity.ConstraintLocation,
			},
			want: `{"specversion":"1.0","id":"6f1c2b7e-4c3a-4a1d-9f0b-6c2b8a7d1e33","source":"/warehouse/warehouse-planning","type":"com.warehouse.wes.warehouse-planning.processcapacity.ProcessCapacityChanged","subject":"` + goldenPCSubject + `","datacontenttype":"application/json","dataschema":"urn:warehouse:warehouse-planning:events:ProcessCapacityChanged:v1","time":"2026-10-04T21:45:10Z","data":{"process_type":"PICK","location":"GOLDEN-ZONE","window_start":"2026-10-05T08:00:00Z","window_end":"2026-10-05T16:00:00Z","constraint_type":"LOCATION","previous_quantity":4000,"previous_unit":"UNIT","previous_period_seconds":3600,"previous_binding_constraint":"LABOR","effective_quantity":3500,"effective_unit":"UNIT","effective_period_seconds":3600,"effective_binding_constraint":"LOCATION"}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msgs, err := goldenEncoder().EncodeProcessCapacityEvents(tc.event)
			if err != nil {
				t.Fatal(err)
			}
			if len(msgs) != 1 {
				t.Fatalf("want 1 message, got %d", len(msgs))
			}
			assertGoldenProcessCapacityMessage(t, msgs[0], tc.want)
		})
	}
}

// assertGoldenProcessCapacityMessage pins one message's full wire form.
func assertGoldenProcessCapacityMessage(t *testing.T, m outbox.Message, want string) {
	t.Helper()
	if string(m.Value) != want {
		t.Errorf("value mismatch\n got: %s\nwant: %s", m.Value, want)
	}
	if m.Topic != Topic {
		t.Errorf("topic = %q, want %q", m.Topic, Topic)
	}
	if m.Subject != goldenPCSubject || string(m.Key) != goldenPCSubject {
		t.Errorf("subject/key = %q / %q, want %q", m.Subject, m.Key, goldenPCSubject)
	}
	if len(m.Headers) != 1 || m.Headers[0].Key != "content-type" || m.Headers[0].Value != wantCT {
		t.Errorf("headers = %v", m.Headers)
	}
}

// The analytics fanout carries PLAN facts only: a ProcessCapacity event
// becomes exactly one message, on the integration topic, with no analytics
// copy -- and the analytics encoder itself refuses one handed directly.
func TestProcessCapacityEventsGetNoAnalyticsCopy(t *testing.T) {
	reg := processcapacity.ProcessCapacityRegistered{
		Header: goldenPCHeader(), ConstraintType: processcapacity.ConstraintLabor, Rate: goldenPCRate4000,
	}

	f := &FanoutEncoder{Integration: &Encoder{}, Analytics: &AnalyticsEncoder{}, NewID: func() string { return goldenEvtID }}
	msgs, err := f.EncodeProcessCapacityEvents(reg)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("want 1 message (integration only), got %d", len(msgs))
	}
	if msgs[0].Topic != Topic {
		t.Errorf("topic = %q, want integration %q", msgs[0].Topic, Topic)
	}
	if msgs[0].DataSchema != "urn:warehouse:warehouse-planning:events:ProcessCapacityRegistered:v1" {
		t.Errorf("dataschema = %q", msgs[0].DataSchema)
	}

	if _, err := (&AnalyticsEncoder{}).EncodeProcessCapacityEvents(reg); err == nil ||
		!strings.Contains(err.Error(), "no analytics payload") {
		t.Errorf("AnalyticsEncoder.EncodeProcessCapacityEvents = %v, want the plan-facts-only refusal", err)
	}
}
