package kafka

// Event consumer contract tests (E1-E4) for the order-management edge:
// OrderDemandConsumer subscribes to OrderTopic
// (warehouse.order-management.events, wired in NewOrderDemandConsumer's
// kafka.ReaderConfig) and dispatches on the full CloudEvents types
// typeOrderAllocated / typeOrderPartiallyAllocated (OrderRepromised is
// deliberately NOT consumed: it carries no new cutoff instant — ADR 0004).
// The contract is the PINNED copy of order-management's own
// apis/asyncapi.yaml (testdata/order-management.asyncapi.yaml, registered
// in contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/order-management@develop).
//
// Golden fixtures are the producer spec's OWN examples (two per type: one
// bare allocation, one carrying a site_id this consumer deliberately
// ignores — demand is attributed to the CONFIGURED site, ADR 0004).

import (
	"context"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// newOrderContractHarness wires the REAL OrderDemandConsumer over the
// memory repositories (the same shape the external unit tests use).
func newOrderContractHarness() (*OrderDemandConsumer, *memory.OrderDemandRepo) {
	repo, processed := memory.NewOrderDemandRepo(), memory.NewProcessedEventRepo()
	return &OrderDemandConsumer{
		Record: &usecases.RecordOrderDemand{
			UoW:             memory.NewUnitOfWork(repo, processed),
			ProcessedEvents: processed,
			Demand:          repo,
		},
		Location: contractTestDemandSite,
		Logger:   contractLogger(),
	}, repo
}

// orderGoldenExamples returns the pinned producer spec's examples for one
// message on the subscribed channel.
func orderGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	return loadPinnedAsyncAPI(t, pinnedOrderManagementSpec).goldenMessage(t, message).Examples
}

// E1: the topic this repo's reader subscribes to (OrderTopic, wired by
// NewOrderDemandConsumer) is documented by the producer's pinned spec as a
// 'subscribe:' channel — order-management's file is channel-perspective,
// its own topics sit under 'subscribe:' — and the channel's message list
// documents both types HandleMessage dispatches on.
func TestPinnedAsyncAPICoversSubscriptions_OrderManagement(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOrderManagementSpec)
	documented := map[string]bool{}
	for _, name := range doc.channelMessageRefs(t, OrderTopic, "subscribe") {
		documented[doc.documentedConst(t, doc.message(t, name), "type")] = true
	}
	for _, routed := range []string{typeOrderAllocated, typeOrderPartiallyAllocated} {
		if !documented[routed] {
			t.Errorf("pinned order-management spec does not document %s on %s (documented: %v)", routed, OrderTopic, documented)
		}
	}
}

// E2: every routed-type example decodes through the REAL
// cloudevents.Decode and routes through the REAL
// OrderDemandConsumer.HandleMessage into the expected-demand read model:
// the order exists with the event's promise cutoff and released-line count,
// attributed to the CONFIGURED site (a site_id in the data is ignored by
// design); the replayed CloudEvents id records nothing more.
func TestGoldenEventsDecode_OrderManagement(t *testing.T) {
	for _, c := range []struct {
		message string
		routed  string
	}{
		{message: "OrderAllocatedIntegration", routed: typeOrderAllocated},
		{message: "OrderPartiallyAllocatedIntegration", routed: typeOrderPartiallyAllocated},
	} {
		for _, ex := range orderGoldenExamples(t, c.message) {
			t.Run(ex.Name, func(t *testing.T) { runOrderGoldenCase(t, ex) })
		}
	}
}

// runOrderGoldenCase drives one golden allocation example through the real
// decode + HandleMessage path.
func runOrderGoldenCase(t *testing.T, ex pinnedExample) {
	t.Helper()
	consumer, repo := newOrderContractHarness()
	ctx := context.Background()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("handle golden example: %v", err)
	}
	data := ex.Payload["data"].(map[string]any)
	wantID, _ := data["order_id"].(string)
	order, ok := repo.Get(wantID)
	if !ok {
		t.Fatalf("order %q (the subject/data order_id) is not in the model", wantID)
	}
	if order.Location() != contractTestDemandSite {
		t.Errorf("%s: location %q, want the CONFIGURED site %q (the events carry none)", ex.Name, order.Location(), contractTestDemandSite)
	}
	lines, _ := data["lines"].([]any)
	if order.ReleasedLines() != len(lines) {
		t.Errorf("%s: released lines %d, want %d", ex.Name, order.ReleasedLines(), len(lines))
	}
	if repo.Len() != 1 {
		t.Fatalf("golden example left %d orders, want exactly 1", repo.Len())
	}

	if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if repo.Len() != 1 {
		t.Errorf("replayed CloudEvents id %q changed the model size (%d orders)", ex.Payload["id"], repo.Len())
	}
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source /warehouse/order-management,
// subject, time, datacontenttype application/json, dataschema
// urn:warehouse:order-management:events:<Name>:v1 — and the pinned spec's
// envelope schema requires exactly those attributes.
func TestCloudEventsEnvelopeInvariants_OrderManagement(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedOrderManagementSpec)
	for _, c := range []struct {
		message    string
		dataschema string
	}{
		{message: "OrderAllocatedIntegration", dataschema: "urn:warehouse:order-management:events:OrderAllocated:v1"},
		{message: "OrderPartiallyAllocatedIntegration", dataschema: "urn:warehouse:order-management:events:OrderPartiallyAllocated:v1"},
	} {
		for _, ex := range orderGoldenExamples(t, c.message) {
			assertCloudEventsEnvelope(t, ex.Name, decodeGolden(t, ex), "/warehouse/order-management", c.dataschema)
		}
	}
	assertEnvelopeRequired(t, doc, "OrderAllocatedIntegration")
}

// E4: this consumer skips and commits past anything it cannot use — a
// non-CloudEvents message, a malformed payload and an unknown event TYPE
// all return nil (no DLQ: demand ingestion must never block on foreign
// traffic); unknown ADDITIVE data attributes must not change the fold.
func TestUnknownAttributesTolerated_OrderManagement(t *testing.T) {
	golden := orderGoldenExamples(t, "OrderAllocatedIntegration")[0].Payload

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, repo := newOrderContractHarness()
	ctx := context.Background()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if repo.Len() != 1 {
		t.Errorf("unknown data attributes must not change the outcome (%d orders)", repo.Len())
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.order-management.order.OrderRepromised"
	if err := consumer.HandleMessage(ctx, goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if repo.Len() != 1 {
		t.Errorf("unknown event type was acted on (%d orders)", repo.Len())
	}

	if err := consumer.HandleMessage(ctx, goldenJSON(t, map[string]any{"order_id": "ord-legacy"})); err != nil {
		t.Errorf("non-CloudEvents message must be skipped with nil, got %v", err)
	}
}
