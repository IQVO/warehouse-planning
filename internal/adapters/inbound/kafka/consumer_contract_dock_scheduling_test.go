package kafka

// Event consumer contract tests (E1-E4) for the dock-scheduling edge:
// DockAppointmentConsumer subscribes to DockSchedulingTopic
// (warehouse.dock-scheduling.events, wired in NewDockAppointmentConsumer's
// kafka.ReaderConfig) and dispatches on the full CloudEvents types
// typeDockAppointmentBooked / Rescheduled / Cancelled / NoShow (ADR 0020;
// the retired inbound-receiving source must fall through to the default
// branch). The contract is the PINNED copy of dock-scheduling's own
// apis/asyncapi.yaml (testdata/dock-scheduling.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/dock-scheduling@develop).
//
// Golden fixtures are the producer spec's OWN examples. dock-scheduling
// documents its `type` strings only in the examples (no schema const); the
// loaders fall back to the example payload's type, verified per file.

import (
	"context"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

// newDockContractHarness wires the REAL DockAppointmentConsumer over the
// memory repositories (the same shape the external unit tests use).
func newDockContractHarness() (*DockAppointmentConsumer, *memory.InboundAppointmentRepo) {
	repo, processed := memory.NewInboundAppointmentRepo(), memory.NewProcessedEventRepo()
	return &DockAppointmentConsumer{
		Record: &usecases.RecordInboundAppointment{
			UoW:             memory.NewUnitOfWork(repo, processed),
			ProcessedEvents: processed,
			Appointments:    repo,
		},
		Logger: contractLogger(),
	}, repo
}

// dockGoldenExamples returns the pinned producer spec's examples for one
// message on the subscribed channel.
func dockGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	return loadPinnedAsyncAPI(t, pinnedDockSchedulingSpec).goldenMessage(t, message).Examples
}

// E1: the topic this repo's reader subscribes to (DockSchedulingTopic,
// wired by NewDockAppointmentConsumer) is documented by the producer's
// pinned spec as a 'subscribe:' channel — dock-scheduling's file is
// channel-perspective, its own topics sit under 'subscribe:' — and the
// channel's message list documents every type HandleMessage dispatches on.
func TestPinnedAsyncAPICoversSubscriptions_DockScheduling(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedDockSchedulingSpec)
	documented := dockDocumentedTypes(t, doc, "subscribe")

	for _, routed := range []string{
		typeDockAppointmentBooked, typeDockAppointmentRescheduled,
		typeDockAppointmentCancelled, typeDockAppointmentNoShow,
	} {
		if !documented[routed] {
			t.Errorf("pinned dock-scheduling spec does not document %s on %s (documented: %v)", routed, DockSchedulingTopic, documented)
		}
	}
	// The retired inbound-receiving source is documented NOWHERE on this
	// channel: its types fall through to the default branch and are never
	// consumed (ADR 0020).
	for _, retired := range []string{
		"com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentBooked",
		"com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentCancelled",
	} {
		if documented[retired] {
			t.Errorf("pinned dock-scheduling spec documents retired inbound-receiving type %s on %s", retired, DockSchedulingTopic)
		}
	}
}

// dockDocumentedTypes maps every message on the topic's operation to the
// CloudEvents type string its payload documents.
func dockDocumentedTypes(t *testing.T, doc *pinnedAsyncAPI, op string) map[string]bool {
	t.Helper()
	documented := map[string]bool{}
	for _, name := range doc.channelMessageRefs(t, DockSchedulingTopic, op) {
		documented[doc.documentedConst(t, doc.message(t, name), "type")] = true
	}
	return documented
}

// E2: every routed-type example decodes through the REAL cloudevents.Decode
// and routes through the REAL DockAppointmentConsumer.HandleMessage into the
// inbound-demand read model; the replayed CloudEvents id records nothing
// more (RecordInboundAppointment's Processed gate is keyed on the event id).
func TestGoldenEventsDecode_DockScheduling(t *testing.T) {
	for _, message := range []string{
		"DockAppointmentBooked", "DockAppointmentRescheduled",
		"DockAppointmentCancelled", "DockAppointmentNoShow",
	} {
		for _, ex := range dockGoldenExamples(t, message) {
			t.Run(message+"/"+ex.Name, func(t *testing.T) { runDockGoldenCase(t, ex) })
		}
	}
}

// runDockGoldenCase drives one golden dock-scheduling example through the
// real decode + HandleMessage path: the model gains exactly the appointment
// the example documents, and a replay of the same CloudEvents id changes
// nothing (set size before/after).
func runDockGoldenCase(t *testing.T, ex pinnedExample) {
	t.Helper()
	consumer, repo := newDockContractHarness()
	ctx := context.Background()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("handle golden example: %v", err)
	}
	if repo.Len() != 1 {
		t.Fatalf("golden example left %d appointments, want exactly 1", repo.Len())
	}
	data := ex.Payload["data"].(map[string]any)
	wantID, _ := data["appointment_id"].(string)
	appt, ok := repo.Get(wantID)
	if !ok {
		t.Fatalf("appointment %q (the subject/data appointment_id) is not in the model", wantID)
	}
	assertDockAppointment(t, ex.Name, appt, data)

	if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if repo.Len() != 1 {
		t.Errorf("replayed CloudEvents id %q changed the model size (%d appointments)", ex.Payload["id"], repo.Len())
	}
}

// assertDockAppointment checks the folded appointment against the golden
// example's own data (yaml.v3 decodes whole numbers as int: coerce).
func assertDockAppointment(t *testing.T, name string, appt inbound.Appointment, data map[string]any) {
	t.Helper()
	if got, _ := data["site_id"].(string); appt.Location() != got {
		t.Errorf("%s: location %q, want the event's site_id %q", name, appt.Location(), got)
	}
	if got, _ := data["direction"].(string); got != "" && string(appt.Direction()) != got {
		t.Errorf("%s: direction %q, want %q", name, appt.Direction(), got)
	}
	asns, _ := data["asn_numbers"].([]any)
	if len(asns) != 0 && appt.ASNCount() != len(asns) {
		t.Errorf("%s: ASNCount %d, want %d", name, appt.ASNCount(), len(asns))
	}
	if v := data["version"]; v != nil {
		if appt.Version() != coerceInt64(t, v) {
			t.Errorf("%s: version %d, want %v", name, appt.Version(), v)
		}
	}
}

// coerceInt64 numerically coerces a yaml.v3-decoded whole number
// (int on whole values) into the int64 the DTOs decode.
func coerceInt64(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int:
		return int64(n)
	case int64:
		return n
	case float64:
		return int64(n)
	default:
		t.Fatalf("coerce %v (%T) as an integer", v, v)
		return 0
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source /warehouse/dock-scheduling,
// subject, time, datacontenttype application/json, dataschema
// urn:warehouse:dock-scheduling:events:<Name>:v1 — and the pinned spec's
// envelope schema requires exactly those attributes.
func TestCloudEventsEnvelopeInvariants_DockScheduling(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedDockSchedulingSpec)
	for _, message := range []string{
		"DockAppointmentBooked", "DockAppointmentRescheduled",
		"DockAppointmentCancelled", "DockAppointmentNoShow",
	} {
		for _, ex := range dockGoldenExamples(t, message) {
			evt := decodeGolden(t, ex)
			assertCloudEventsEnvelope(t, ex.Name, evt, "/warehouse/dock-scheduling",
				"urn:warehouse:dock-scheduling:events:"+message+":v1")
		}
	}
	assertEnvelopeRequired(t, doc, "DockAppointmentBooked")
}

// E4: this consumer dead-letters deterministic poison (a non-CloudEvents
// message fails decode and HandleMessage returns the poison marker the run
// loop dead-letters once and never retries); unknown ADDITIVE data
// attributes must not fail decode or handling; an unknown event TYPE on the
// shared topic is silently skipped (nil, no model change, no DLQ).
func TestUnknownAttributesTolerated_DockScheduling(t *testing.T) {
	golden := dockGoldenExamples(t, "DockAppointmentBooked")[0].Payload

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, repo := newDockContractHarness()
	if err := consumer.HandleMessage(context.Background(), goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if repo.Len() != 1 {
		t.Errorf("unknown data attributes must not change the outcome (%d appointments)", repo.Len())
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.dock-scheduling.dockcalendar.DoorClosureScheduled"
	if err := consumer.HandleMessage(context.Background(), goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if repo.Len() != 1 {
		t.Errorf("unknown event type was acted on (%d appointments)", repo.Len())
	}

	if err := consumer.HandleMessage(context.Background(), goldenJSON(t, map[string]any{"appointment_id": "appt-legacy", "site_id": "WH1"})); !isPoison(err) {
		t.Errorf("non-CloudEvents message returned %v, want the poison marker (the DLQ trigger)", err)
	}
}
