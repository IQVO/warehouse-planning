package kafka

// Shared E3 bodies for the event consumer contract tests: the CloudEvents
// envelope invariants asserted on every golden fixture, and the spec-side
// required-attribute assertion. Kept here so each per-producer contract
// file stays under the file-length budget.

import (
	"testing"

	ce "github.com/cloudevents/sdk-go/v2/event"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
)

// decodeGolden decodes one golden example's payload through the REAL
// cloudevents.Decode (structured-mode JSON bytes).
func decodeGolden(t *testing.T, ex pinnedExample) ce.Event {
	t.Helper()
	evt, err := cloudevents.Decode(goldenJSON(t, ex.Payload))
	if err != nil {
		t.Fatalf("decode golden example %s: %v", ex.Name, err)
	}
	return evt
}

// assertCloudEventsEnvelope is the shared E3 body: one decoded golden
// fixture against the fleet envelope invariants. source and dataschema are
// the values the producer's pinned spec documents (its schema consts or its
// own examples) — derived from the pin, never a formula.
func assertCloudEventsEnvelope(t *testing.T, name string, evt ce.Event, source, dataschema string) {
	t.Helper()
	if evt.SpecVersion() != cloudevents.SpecVersion {
		t.Errorf("%s: specversion %q, want %q", name, evt.SpecVersion(), cloudevents.SpecVersion)
	}
	if evt.ID() == "" || evt.Subject() == "" || evt.Time().IsZero() {
		t.Errorf("%s: id/subject/time must all be present (got %q/%q/%v)", name, evt.ID(), evt.Subject(), evt.Time())
	}
	if evt.Source() != source {
		t.Errorf("%s: source %q, want %q", name, evt.Source(), source)
	}
	if evt.DataMediaType() != cloudevents.DataContentType {
		t.Errorf("%s: datacontenttype %q, want %q", name, evt.DataMediaType(), cloudevents.DataContentType)
	}
	if evt.DataSchema() != dataschema {
		t.Errorf("%s: dataschema %q, want %q", name, evt.DataSchema(), dataschema)
	}
}

// assertEnvelopeRequired is the shared E3 spec-side body: the pinned
// message's envelope schema must require every fleet-mandatory attribute.
func assertEnvelopeRequired(t *testing.T, doc *pinnedAsyncAPI, message string) {
	t.Helper()
	required := doc.envelopeRequired(t, doc.message(t, message).Payload)
	for _, attr := range []string{"specversion", "id", "source", "type", "subject", "time", "datacontenttype", "dataschema", "data"} {
		if !required[attr] {
			t.Errorf("pinned spec's %s envelope does not require %q (required: %v)", message, attr, required)
		}
	}
}
