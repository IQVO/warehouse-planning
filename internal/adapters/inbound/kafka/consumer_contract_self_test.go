package kafka

// Event consumer contract tests for the SELF-PIN: this repo's own
// apis/asyncapi.yaml (testdata/warehouse-planning.asyncapi.yaml, a
// byte-copy registered in contracts/pinned.json with THIS repo as its own
// producer). The own file is channel-perspective: the topics this service
// PUBLISHES sit under 'subscribe:' (warehouse.warehouse-planning.events
// written by the outbox relay, and warehouse.warehouse-planning.analytics
// written by the projector twin) while the topics it consumes sit under
// 'publish:' — the five producer pins cover those.
//
// E1-self: the events channel documents every integration type the outbound
// encoder emits and the analytics channel documents every type the
// AnalyticsConsumer folds (analyticsKinds); E2-self drives the analytics
// golden examples through the REAL AnalyticsConsumer (self-consumption);
// E3-self asserts the envelope invariants on every own example.

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	outkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/analytics/report"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// selfAnalyticsTopic is the projector twin's topic (its DLQ is this name +
// DLQSuffix). It is one of this repo's OWN 'subscribe:' channels.
const selfAnalyticsTopic = "warehouse.warehouse-planning.analytics"

// contractProjection is the fake writer port: it records applied ids and
// answers applied=false for a replay (the Projection contract).
type contractProjection struct {
	applied map[string]bool
}

func (p *contractProjection) Apply(_ context.Context, e report.PlanEvent) (bool, error) {
	if p.applied == nil {
		p.applied = map[string]bool{}
	}
	if p.applied[e.EventID] {
		return false, nil
	}
	p.applied[e.EventID] = true
	return true, nil
}

// selfDocumentedTypes maps every message on one of the own 'subscribe:'
// channels to the CloudEvents type string its payload documents.
func selfDocumentedTypes(t *testing.T, topic string) map[string]string {
	t.Helper()
	doc := loadPinnedAsyncAPI(t, pinnedSelfSpec)
	documented := map[string]string{}
	for _, name := range doc.channelMessageRefs(t, topic, "subscribe") {
		documented[doc.documentedConst(t, doc.message(t, name), "type")] = name
	}
	return documented
}

// selfGoldenExamples returns the own pinned spec's examples for one
// message (message names are unique across the file's channels).
func selfGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	return loadPinnedAsyncAPI(t, pinnedSelfSpec).goldenMessage(t, message).Examples
}

// E1-self (integration stream): the events channel documents every type
// the outbound encoder emits — the four CapacityPlan facts, the two
// ProcessCapacity facts and the three ADR 0023 facts, built with the same
// cloudevents.Type the encoder calls.
func TestPinnedAsyncAPICoversSubscriptions_SelfEvents(t *testing.T) {
	documented := selfDocumentedTypes(t, outkafka.Topic)
	emitted := append(
		planEventTypes(),
		cloudevents.Type(outkafka.ProcessCapacityEntity, processcapacity.EventProcessCapacityRegistered),
		cloudevents.Type(outkafka.ProcessCapacityEntity, processcapacity.EventProcessCapacityChanged),
	)
	emitted = append(emitted, requirementEventTypes()...)
	for _, typ := range emitted {
		if _, ok := documented[typ]; !ok {
			t.Errorf("own pinned spec does not document emitted type %s on %s (documented: %v)", typ, outkafka.Topic, keysOf(documented))
		}
	}
}

// requirementEventTypes lists the three ADR 0023 types, published on BOTH
// own topics.
func requirementEventTypes() []string {
	return []string{
		cloudevents.Type(outkafka.WorkloadForecastEntity, workloadforecast.EventWorkloadForecastReceived),
		cloudevents.Type(outkafka.LabourPlanEntity, labourplan.EventLabourPlanCommitted),
		cloudevents.Type(outkafka.LabourPlanEntity, labourplan.EventLabourPlanDriftDetected),
	}
}

// E1-self (analytics stream, ADR 0023): the analytics channel documents the
// analytics copy of the three requirement-side types too, though the
// projector does not route them (they are acknowledged unprojected).
func TestPinnedAsyncAPICoversSubscriptions_SelfAnalyticsRequirementTypes(t *testing.T) {
	documented := selfDocumentedTypes(t, selfAnalyticsTopic)
	for _, typ := range requirementEventTypes() {
		if _, ok := documented[typ]; !ok {
			t.Errorf("own pinned spec does not document analytics type %s on %s", typ, selfAnalyticsTopic)
		}
		if _, routed := analyticsKinds[typ]; routed {
			t.Errorf("%s is routed by the projector; this wave acknowledges it unprojected", typ)
		}
	}
}

// planEventTypes lists the five CapacityPlan integration types.
func planEventTypes() []string {
	return []string{
		cloudevents.Type(outkafka.Entity, capacityplan.EventCapacityPlanCreated),
		cloudevents.Type(outkafka.Entity, capacityplan.EventCapacityPlanPublished),
		cloudevents.Type(outkafka.Entity, capacityplan.EventCapacityShortageDetected),
		cloudevents.Type(outkafka.Entity, capacityplan.EventBottleneckDetected),
		cloudevents.Type(outkafka.Entity, capacityplan.EventCapacityPlanSuperseded),
	}
}

// E1-self (analytics stream, docs/adr/0024 decision 3): the analytics channel
// documents CapacityPlanSuperseded, which the projector acknowledges
// unprojected (a superseded plan stays counted as published in the reports).
func TestPinnedAsyncAPICoversSubscriptions_SelfAnalyticsSuperseded(t *testing.T) {
	typ := cloudevents.Type(outkafka.Entity, capacityplan.EventCapacityPlanSuperseded)
	if _, ok := selfDocumentedTypes(t, selfAnalyticsTopic)[typ]; !ok {
		t.Errorf("own pinned spec does not document analytics type %s on %s", typ, selfAnalyticsTopic)
	}
	if _, routed := analyticsKinds[typ]; routed {
		t.Errorf("%s is routed by the projector; it is acknowledged unprojected", typ)
	}
}

// keysOf is a stable-enough key list for failure messages.
func keysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// E1-self (analytics stream): the analytics channel documents exactly the
// types the AnalyticsConsumer routes on (analyticsKinds): the analytics
// twins REUSE the integration event name — no -Analytics suffix on `type`.
func TestPinnedAsyncAPICoversSubscriptions_SelfAnalytics(t *testing.T) {
	documented := selfDocumentedTypes(t, selfAnalyticsTopic)
	if len(analyticsKinds) == 0 {
		t.Fatal("analyticsKinds routing table is empty; the self-pin E1 cannot assert coverage")
	}
	for typ := range analyticsKinds {
		if _, ok := documented[typ]; !ok {
			t.Errorf("own pinned spec does not document analytics type %s on %s (documented: %v)", typ, selfAnalyticsTopic, keysOf(documented))
		}
	}
}

// E2-self: every analytics golden example decodes through the REAL
// cloudevents.Decode and projects through the REAL AnalyticsConsumer.handle
// (self-consumption of the projector twin): each example is applied exactly
// once, its replay answers applied=false (the Projection dedupe contract),
// and an unknown type on the analytics topic is ignored.
func TestGoldenEventsDecode_SelfAnalytics(t *testing.T) {
	projection := &contractProjection{}
	consumer := &AnalyticsConsumer{Projection: projection, Logger: contractLogger()}
	ctx := context.Background()
	handled := 0
	for typ, message := range selfDocumentedTypes(t, selfAnalyticsTopic) {
		if _, routed := analyticsKinds[typ]; !routed {
			continue
		}
		for _, ex := range selfGoldenExamples(t, message) {
			msg := kafkago.Message{Value: goldenJSON(t, ex.Payload)}
			if err := consumer.handle(ctx, msg); err != nil {
				t.Fatalf("%s: project golden analytics example: %v", ex.Name, err)
			}
			if !projection.applied[ex.Payload["id"].(string)] {
				t.Fatalf("%s: golden analytics example was not applied", ex.Name)
			}
			if err := consumer.handle(ctx, msg); err != nil {
				t.Fatalf("%s: replay: %v", ex.Name, err)
			}
			handled++
		}
	}
	if handled != len(analyticsKinds) {
		t.Fatalf("drove %d golden analytics examples, want one per routed kind (%d)", handled, len(analyticsKinds))
	}

	unknown := clonePayload(selfGoldenExamples(t, "CapacityPlanCreatedAnalytics")[0].Payload)
	unknown["type"] = "com.warehouse.wes.warehouse-planning.capacityplan.SomethingElse"
	if err := consumer.handle(ctx, kafkago.Message{Value: goldenJSON(t, unknown)}); err != nil {
		t.Fatalf("unknown analytics type must be ignored, not an error: %v", err)
	}
	if len(projection.applied) != handled {
		t.Errorf("projected %d events after an unknown type, want the %d golden examples only", len(projection.applied), handled)
	}
}

// E3-self: every own example — integration AND analytics — carries the
// fleet-mandatory CloudEvents 1.0 envelope with THIS repo's source
// constant, and the dataschema the message's own schema pins (events use
// the events stream, analytics twins the analytics stream, both reusing
// the integration event name with their .vN suffix).
func TestCloudEventsEnvelopeInvariants_Self(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedSelfSpec)
	for _, message := range []string{"CapacityPlanCreated", "ProcessCapacityRegistered", "CapacityPlanCreatedAnalytics",
		"WorkloadForecastReceived", "LabourPlanCommitted", "LabourPlanDriftDetected",
		"WorkloadForecastReceivedAnalytics", "LabourPlanCommittedAnalytics", "LabourPlanDriftDetectedAnalytics",
		"CapacityPlanSuperseded", "CapacityPlanSupersededAnalytics"} {
		msg := doc.message(t, message)
		wantSchema := doc.documentedConst(t, msg, "dataschema")
		for _, ex := range selfGoldenExamples(t, message) {
			assertCloudEventsEnvelope(t, ex.Name, decodeGolden(t, ex), cloudevents.Source, wantSchema)
		}
	}
}
