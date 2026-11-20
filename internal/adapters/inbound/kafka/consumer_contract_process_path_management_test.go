package kafka

// Event consumer contract tests (E1-E4) for the process-path-management edge
// (ADR 0025): ProcessFlowConsumer subscribes to ProcessPathManagementTopic
// (warehouse.process-path-management.events, process-path-management's
// INTEGRATION topic, wired in NewProcessFlowConsumer) and dispatches on the full
// CloudEvents types typeProcessFlowDefined, typeProcessFlowRevised and
// typeProcessFlowRetired; every other type on the topic (ProcessPath*,
// CPTScheduleChanged) is ignored. The contract is the PINNED copy of
// process-path-management's own apis/asyncapi.yaml
// (testdata/process-path-management.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy drifts
// from IQVO/process-path-management@develop). Its file documents its own
// integration topic under 'publish:'.
//
// Golden fixtures are the producer spec's OWN integration examples (one per flow
// type); the producer's spec carries an example for each, so none is added here.

import (
	"context"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

const (
	pinnedProcessPathManagementSpec = "testdata/process-path-management.asyncapi.yaml"
	processFlowDefinedMsg           = "ProcessFlowDefined"
	processFlowRevisedMsg           = "ProcessFlowRevised"
	processFlowRetiredMsg           = "ProcessFlowRetired"
)

// newFlowContractHarness wires the REAL ProcessFlowConsumer over the memory
// repositories (the same shape the external unit tests use).
func newFlowContractHarness() (*ProcessFlowConsumer, *memory.ProcessFlowRepo) {
	repo, processed := memory.NewProcessFlowRepo(), memory.NewProcessedEventRepo()
	return &ProcessFlowConsumer{
		Record: &usecases.RecordProcessFlow{
			UoW:             memory.NewUnitOfWork(repo, processed),
			ProcessedEvents: processed,
			Flows:           repo,
		},
		Logger: contractLogger(),
	}, repo
}

func processPathManagementGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	return loadPinnedAsyncAPI(t, pinnedProcessPathManagementSpec).goldenMessage(t, message).Examples
}

// E1: the topic this repo's reader subscribes to (ProcessPathManagementTopic) is
// documented by the producer's pinned spec, and the channel's message list
// documents the three types HandleMessage acts on. The ProcessPath* and
// CPTScheduleChanged types stay documented-but-not-routed on the same channel.
func TestPinnedAsyncAPICoversSubscriptions_ProcessPathManagement(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedProcessPathManagementSpec)
	documented := map[string]bool{}
	for _, name := range doc.channelMessageRefs(t, ProcessPathManagementTopic, "publish") {
		documented[doc.documentedConst(t, doc.message(t, name), "type")] = true
	}
	for _, want := range []string{typeProcessFlowDefined, typeProcessFlowRevised, typeProcessFlowRetired} {
		if !documented[want] {
			t.Errorf("pinned process-path-management spec does not document %s on %s (documented: %v)", want, ProcessPathManagementTopic, documented)
		}
	}
	for _, ignored := range []string{
		"com.warehouse.wes.process-path-management.processpath.ProcessPathCreated",
		"com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged",
	} {
		if !documented[ignored] {
			t.Errorf("pinned spec no longer documents %s on %s; revisit the ignore list", ignored, ProcessPathManagementTopic)
		}
	}
}

// E2: every flow example decodes through the REAL cloudevents.Decode and routes
// through the REAL ProcessFlowConsumer.HandleMessage into the local copy: exactly
// one flow per example, at the example's own version, and the replayed CloudEvents
// id records nothing more.
func TestGoldenEventsDecode_ProcessPathManagement(t *testing.T) {
	for _, message := range []string{processFlowDefinedMsg, processFlowRevisedMsg, processFlowRetiredMsg} {
		for _, ex := range processPathManagementGoldenExamples(t, message) {
			t.Run(message+"/"+ex.Name, func(t *testing.T) { assertGoldenFlowRecordedOnce(t, ex) })
		}
	}
}

func assertGoldenFlowRecordedOnce(t *testing.T, ex pinnedExample) {
	t.Helper()
	consumer, repo := newFlowContractHarness()
	ctx := context.Background()
	for range 2 {
		if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
			t.Fatalf("handle golden example: %v", err)
		}
		if repo.Len() != 1 {
			t.Fatalf("golden example stored %d flows, want exactly 1", repo.Len())
		}
	}
	data := ex.Payload["data"].(map[string]any)
	stored, ok := repo.Get(data["flow_id"].(string))
	if !ok {
		t.Fatalf("flow %q from the golden example is not in the local copy", data["flow_id"])
	}
	if int(stored.Version()) != data["version"].(int) || string(stored.Status()) != data["status"].(string) {
		t.Errorf("stored flow = version %d, status %s; example says version %v, status %v", stored.Version(), stored.Status(), data["version"], data["status"])
	}
	if got, want := len(stored.Stages()), len(data["stages"].([]any)); got != want {
		t.Errorf("stored %d stages, example has %d", got, want)
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0 envelope
// with source /warehouse/process-path-management and the INTEGRATION dataschema
// (urn:warehouse:process-path-management:events:<Event>:v1, never the analytics
// one), and the spec requires the envelope attributes.
func TestCloudEventsEnvelopeInvariants_ProcessPathManagement(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedProcessPathManagementSpec)
	for message, event := range map[string]string{
		processFlowDefinedMsg: "ProcessFlowDefined",
		processFlowRevisedMsg: "ProcessFlowRevised",
		processFlowRetiredMsg: "ProcessFlowRetired",
	} {
		for _, ex := range processPathManagementGoldenExamples(t, message) {
			assertCloudEventsEnvelope(t, ex.Name, decodeGolden(t, ex), "/warehouse/process-path-management",
				"urn:warehouse:process-path-management:events:"+event+":v1")
		}
		assertEnvelopeRequired(t, doc, message)
	}
}

// E4: unknown ADDITIVE data attributes must not fail decode or handling; the
// producer's own ProcessPathCreated example on the shared topic is silently
// skipped; a non-CloudEvents message is poison; a stage whose process_type the
// closed set does not know is poison (the message is never applied).
func TestUnknownAttributesTolerated_ProcessPathManagement(t *testing.T) {
	golden := processPathManagementGoldenExamples(t, processFlowDefinedMsg)[0].Payload
	ctx := context.Background()

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["stages"].([]any)[0].(map[string]any)["future_stage_field"] = 7
	consumer, repo := newFlowContractHarness()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if repo.Len() != 1 {
		t.Fatalf("flow with unknown additive attributes stored %d flows, want 1", repo.Len())
	}

	created := processPathManagementGoldenExamples(t, "ProcessPathCreated")[0].Payload
	if err := consumer.HandleMessage(ctx, goldenJSON(t, created)); err != nil {
		t.Fatalf("ProcessPathCreated must be skipped, not an error: %v", err)
	}
	if repo.Len() != 1 {
		t.Errorf("ProcessPathCreated was acted on (%d flows)", repo.Len())
	}

	unknown := clonePayload(processPathManagementGoldenExamples(t, processFlowRevisedMsg)[0].Payload)
	unknown["data"].(map[string]any)["stages"].([]any)[0].(map[string]any)["process_type"] = "TELEPORT"
	unknown["id"] = "00000000-0000-4000-8000-000000000001"
	if err := consumer.HandleMessage(ctx, goldenJSON(t, unknown)); !isPoison(err) {
		t.Errorf("unknown process_type returned %v, want the poison marker (the DLQ trigger)", err)
	}
	if repo.Len() != 1 {
		t.Errorf("a rejected flow was applied (%d flows)", repo.Len())
	}

	if err := consumer.HandleMessage(ctx, goldenJSON(t, map[string]any{"flow_id": "legacy"})); !isPoison(err) {
		t.Errorf("non-CloudEvents message returned %v, want the poison marker (the DLQ trigger)", err)
	}
}
