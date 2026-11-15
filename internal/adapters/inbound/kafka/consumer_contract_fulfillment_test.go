package kafka

// Event consumer contract tests (E1-E4) for the fulfillment-execution edge:
// ObservedCapacityConsumer subscribes to FulfillmentTopic
// (warehouse.fulfillment.events, wired in NewObservedCapacityConsumer's
// kafka.ReaderConfig) and dispatches on the full CloudEvents type
// typeTaskCompleted (TaskCompleted is the ONLY type it acts on; every other
// type on the topic is ignored — ADR 0017). The contract is the PINNED copy
// of fulfillment-execution's own apis/asyncapi.yaml
// (testdata/fulfillment-execution.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/fulfillment-execution@develop).
//
// Golden fixtures are the producer spec's OWN examples (PICK, RELOCATE and
// REPLENISH completions).

import (
	"context"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// newObservedContractHarness wires the REAL ObservedCapacityConsumer over
// the memory repositories (the same shape the external unit tests use).
func newObservedContractHarness() (*ObservedCapacityConsumer, *memory.ObservedCapacityRepo) {
	repo, processed := memory.NewObservedCapacityRepo(), memory.NewProcessedEventRepo()
	return &ObservedCapacityConsumer{
		Record: &usecases.RecordObservedCompletion{
			UoW:             memory.NewUnitOfWork(repo, processed),
			ProcessedEvents: processed,
			Observed:        repo,
		},
		Location: contractTestObservedLocation,
		Logger:   contractLogger(),
	}, repo
}

// fulfillmentGoldenExamples returns the pinned producer spec's examples for
// one message on the subscribed channel.
func fulfillmentGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	return loadPinnedAsyncAPI(t, pinnedFulfillmentSpec).goldenMessage(t, message).Examples
}

// E1: the topic this repo's reader subscribes to (FulfillmentTopic, wired
// by NewObservedCapacityConsumer) is documented by the producer's pinned
// spec as a 'subscribe:' channel — fulfillment-execution's file is
// channel-perspective, its own topics sit under 'subscribe:' — and the
// channel's message list documents typeTaskCompleted, the only type
// HandleMessage acts on.
func TestPinnedAsyncAPICoversSubscriptions_Fulfillment(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	documented := map[string]bool{}
	for _, name := range doc.channelMessageRefs(t, FulfillmentTopic, "subscribe") {
		documented[doc.documentedConst(t, doc.message(t, name), "type")] = true
	}
	if !documented[typeTaskCompleted] {
		t.Errorf("pinned fulfillment-execution spec does not document %s on %s (documented: %v)", typeTaskCompleted, FulfillmentTopic, documented)
	}
	// The ignored siblings stay documented-but-not-routed: dispatching on
	// one type must never silently widen (TaskAvailable is documented on
	// the same channel and deliberately not consumed).
	if !documented["com.warehouse.wes.fulfillment-execution.task.TaskAvailable"] {
		t.Errorf("pinned fulfillment-execution spec no longer documents TaskAvailable on %s; revisit the ignore list", FulfillmentTopic)
	}
}

// E2: every TaskCompleted example decodes through the REAL
// cloudevents.Decode and routes through the REAL
// ObservedCapacityConsumer.HandleMessage into the observed-capacity model:
// exactly one completion attributed to the CONFIGURED site (the events
// carry none), and the replayed CloudEvents id records nothing more.
func TestGoldenEventsDecode_Fulfillment(t *testing.T) {
	for _, ex := range fulfillmentGoldenExamples(t, "TaskCompleted") {
		t.Run(ex.Name, func(t *testing.T) {
			consumer, repo := newObservedContractHarness()
			ctx := context.Background()
			if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
				t.Fatalf("handle golden example: %v", err)
			}
			if repo.Tasks() != 1 {
				t.Fatalf("golden example recorded %d completions, want exactly 1", repo.Tasks())
			}
			if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
				t.Fatalf("replay: %v", err)
			}
			if repo.Tasks() != 1 {
				t.Errorf("replayed CloudEvents id %q recorded again (%d completions)", ex.Payload["id"], repo.Tasks())
			}
		})
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source /warehouse/fulfillment-execution,
// subject, time, datacontenttype application/json, dataschema
// urn:warehouse:fulfillment-execution:events:TaskCompleted:v1 — and the
// structured-mode Kafka content-type header the examples carry matches this
// repo's cloudevents.MediaType constant.
func TestCloudEventsEnvelopeInvariants_Fulfillment(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFulfillmentSpec)
	for _, ex := range fulfillmentGoldenExamples(t, "TaskCompleted") {
		assertCloudEventsEnvelope(t, ex.Name, decodeGolden(t, ex), "/warehouse/fulfillment-execution",
			"urn:warehouse:fulfillment-execution:events:TaskCompleted:v1")
		if got := ex.Headers["content-type"]; got != cloudevents.MediaType {
			t.Errorf("%s: Kafka content-type header %q, want this repo's constant %q", ex.Name, got, cloudevents.MediaType)
		}
	}
	assertEnvelopeRequired(t, doc, "TaskCompleted")
}

// E4: this consumer dead-letters deterministic poison (a non-CloudEvents
// message fails decode and HandleMessage returns the poison marker the run
// loop dead-letters once and never retries); unknown ADDITIVE data
// attributes must not fail decode or handling; an unknown event TYPE on
// the shared topic is silently skipped (nil, no completion, no DLQ).
func TestUnknownAttributesTolerated_Fulfillment(t *testing.T) {
	golden := fulfillmentGoldenExamples(t, "TaskCompleted")[0].Payload

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, repo := newObservedContractHarness()
	ctx := context.Background()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if repo.Tasks() != 1 {
		t.Errorf("unknown data attributes must not change the outcome (%d completions)", repo.Tasks())
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
	if err := consumer.HandleMessage(ctx, goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if repo.Tasks() != 1 {
		t.Errorf("unknown event type was acted on (%d completions)", repo.Tasks())
	}

	if err := consumer.HandleMessage(ctx, goldenJSON(t, map[string]any{"task_id": "task-legacy"})); !isPoison(err) {
		t.Errorf("non-CloudEvents message returned %v, want the poison marker (the DLQ trigger)", err)
	}
}
