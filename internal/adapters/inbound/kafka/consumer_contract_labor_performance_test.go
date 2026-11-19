package kafka

// Event consumer contract tests (E1-E4) for the labor-performance edge (ADR
// 0022): LaborStandardConsumer subscribes to LaborPerformanceTopic
// (warehouse.labor-performance.events, labor-performance's INTEGRATION topic,
// wired in NewLaborStandardConsumer) and dispatches on the full CloudEvents
// types typeLaborStandardDefined and typeLaborStandardRevised; every other
// type on the topic (TaskPerformanceRecorded, ...) is ignored. The contract is
// the PINNED copy of labor-performance's own apis/asyncapi.yaml
// (testdata/labor-performance.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/labor-performance@develop). labor-performance's file
// documents its own integration topic under 'publish:'.
//
// Golden fixtures are the producer spec's OWN integration examples (a first
// PICK standard, a CASE standard with team lift, a tightened PICK revision).

import (
	"context"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

const (
	pinnedLaborPerformanceSpec = "testdata/labor-performance.asyncapi.yaml"
	laborStandardDefinedMsg    = "LaborStandardDefinedIntegration"
	laborStandardRevisedMsg    = "LaborStandardRevisedIntegration"
	contractTestStandardSite   = "SITE-GRU-01"
)

// newStandardContractHarness wires the REAL LaborStandardConsumer over the
// memory repositories (the same shape the external unit tests use).
func newStandardContractHarness() (*LaborStandardConsumer, *memory.LaborStandardRepo) {
	repo, processed := memory.NewLaborStandardRepo(), memory.NewProcessedEventRepo()
	return &LaborStandardConsumer{
		Record: &usecases.RecordLaborStandard{
			UoW:             memory.NewUnitOfWork(repo, processed),
			ProcessedEvents: processed,
			Standards:       repo,
		},
		Logger: contractLogger(),
	}, repo
}

// laborPerformanceGoldenExamples returns the pinned producer spec's examples
// for one message on the integration channel.
func laborPerformanceGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	return loadPinnedAsyncAPI(t, pinnedLaborPerformanceSpec).goldenMessage(t, message).Examples
}

// E1: the topic this repo's reader subscribes to (LaborPerformanceTopic) is
// documented by the producer's pinned spec, and the channel's message list
// documents both types HandleMessage acts on. TaskPerformanceRecorded stays
// documented-but-not-routed on the same channel.
func TestPinnedAsyncAPICoversSubscriptions_LaborPerformance(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedLaborPerformanceSpec)
	documented := map[string]bool{}
	for _, name := range doc.channelMessageRefs(t, LaborPerformanceTopic, "publish") {
		documented[doc.documentedConst(t, doc.message(t, name), "type")] = true
	}
	for _, want := range []string{typeLaborStandardDefined, typeLaborStandardRevised} {
		if !documented[want] {
			t.Errorf("pinned labor-performance spec does not document %s on %s (documented: %v)", want, LaborPerformanceTopic, documented)
		}
	}
	if !documented["com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded"] {
		t.Errorf("pinned labor-performance spec no longer documents TaskPerformanceRecorded on %s; revisit the ignore list", LaborPerformanceTopic)
	}
}

// E2: every standard example decodes through the REAL cloudevents.Decode and
// routes through the REAL LaborStandardConsumer.HandleMessage into the local
// copy: exactly one standard per example, and the replayed CloudEvents id
// records nothing more.
func TestGoldenEventsDecode_LaborPerformance(t *testing.T) {
	for _, message := range []string{laborStandardDefinedMsg, laborStandardRevisedMsg} {
		for _, ex := range laborPerformanceGoldenExamples(t, message) {
			t.Run(message+"/"+ex.Name, func(t *testing.T) { assertGoldenStandardRecordedOnce(t, ex) })
		}
	}
}

// assertGoldenStandardRecordedOnce handles one golden example twice (the
// second pass is the replay) and expects exactly one stored standard, the
// example's own standard_id.
func assertGoldenStandardRecordedOnce(t *testing.T, ex pinnedExample) {
	t.Helper()
	consumer, repo := newStandardContractHarness()
	ctx := context.Background()
	for range 2 {
		if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
			t.Fatalf("handle golden example: %v", err)
		}
		if repo.Len() != 1 {
			t.Fatalf("golden example recorded %d standards, want exactly 1", repo.Len())
		}
	}
	id := ex.Payload["data"].(map[string]any)["standard_id"].(string)
	if _, ok := repo.Get(id); !ok {
		t.Errorf("standard %q from the golden example is not in the local copy", id)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope with source /warehouse/labor-performance and the INTEGRATION
// dataschema (urn:warehouse:labor-performance:events:<Event>:v1, never the
// analytics one), and the spec requires the envelope attributes.
func TestCloudEventsEnvelopeInvariants_LaborPerformance(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedLaborPerformanceSpec)
	for message, event := range map[string]string{
		laborStandardDefinedMsg: "LaborStandardDefined",
		laborStandardRevisedMsg: "LaborStandardRevised",
	} {
		for _, ex := range laborPerformanceGoldenExamples(t, message) {
			assertCloudEventsEnvelope(t, ex.Name, decodeGolden(t, ex), "/warehouse/labor-performance",
				"urn:warehouse:labor-performance:events:"+event+":v1")
		}
		assertEnvelopeRequired(t, doc, message)
	}
}

// E4: unknown ADDITIVE data attributes must not fail decode or handling; the
// optional site_code (labor-performance ADR 0038) scopes the stored standard
// to that site; the producer's own TaskPerformanceRecorded example on the
// shared topic is silently skipped; a non-CloudEvents message is poison.
func TestUnknownAttributesTolerated_LaborPerformance(t *testing.T) {
	golden := laborPerformanceGoldenExamples(t, laborStandardDefinedMsg)[0].Payload
	ctx := context.Background()

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["site_code"] = contractTestStandardSite
	consumer, repo := newStandardContractHarness()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes and a site_code: %v", err)
	}
	stored, ok := repo.Get(data["standard_id"].(string))
	if !ok || stored.SiteCode() != contractTestStandardSite {
		t.Errorf("site-scoped standard stored as %+v (found %v), want site %q", stored, ok, contractTestStandardSite)
	}

	performance := laborPerformanceGoldenExamples(t, "TaskPerformanceRecordedIntegration")[0].Payload
	if err := consumer.HandleMessage(ctx, goldenJSON(t, performance)); err != nil {
		t.Fatalf("TaskPerformanceRecorded must be skipped, not an error: %v", err)
	}
	if repo.Len() != 1 {
		t.Errorf("TaskPerformanceRecorded was acted on (%d standards)", repo.Len())
	}

	if err := consumer.HandleMessage(ctx, goldenJSON(t, map[string]any{"standard_id": "std-legacy"})); !isPoison(err) {
		t.Errorf("non-CloudEvents message returned %v, want the poison marker (the DLQ trigger)", err)
	}
}
