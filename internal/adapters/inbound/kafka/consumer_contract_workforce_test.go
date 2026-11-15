package kafka

// Event consumer contract tests (E1-E4) for the workforce-management edge:
// LaborCapacityConsumer subscribes to LaborTopic (warehouse.workforce.events
// — the fleet alias: workforce-management's topics are named
// warehouse.workforce.*) and dispatches on the full CloudEvents type
// typeShiftPlanCommitted, registering one LABOR capacity constraint per
// PathPlan line. The contract is the PINNED copy of workforce-management's
// own apis/asyncapi.yaml (testdata/workforce-management.asyncapi.yaml,
// registered in contracts/pinned.json; the pinned-contracts CI job fails
// when the copy drifts from IQVO/workforce-management@develop).
//
// Golden fixtures are the producer spec's OWN examples (a committed PACK
// line: 3 heads, rate 50, 24 planned hours).

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// newLaborContractHarness wires the REAL LaborCapacityConsumer over the
// memory repositories (the same shape the external unit tests use).
func newLaborContractHarness() (*LaborCapacityConsumer, *memory.ProcessCapacityRepo) {
	pcs, processed := memory.NewProcessCapacityRepo(), memory.NewProcessedEventRepo()
	return &LaborCapacityConsumer{
		Register:        &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessedEvents: processed,
		UoW:             memory.NewUnitOfWork(pcs, processed),
		Logger:          contractLogger(),
	}, pcs
}

// workforceGoldenExamples returns the pinned producer spec's examples for
// one message on the subscribed channel.
func workforceGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	return loadPinnedAsyncAPI(t, pinnedWorkforceSpec).goldenMessage(t, message).Examples
}

// E1: the topic this repo's reader subscribes to (LaborTopic, wired by
// NewLaborCapacityConsumer) is documented by the producer's pinned spec as
// a 'subscribe:' channel — workforce-management's file is
// channel-perspective, its own topics sit under 'subscribe:' — and the
// channel's message list documents typeShiftPlanCommitted, the only type
// HandleMessage acts on.
func TestPinnedAsyncAPICoversSubscriptions_Workforce(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedWorkforceSpec)
	documented := map[string]bool{}
	for _, name := range doc.channelMessageRefs(t, LaborTopic, "subscribe") {
		documented[doc.documentedConst(t, doc.message(t, name), "type")] = true
	}
	if !documented[typeShiftPlanCommitted] {
		t.Errorf("pinned workforce-management spec does not document %s on %s (documented: %v)", typeShiftPlanCommitted, LaborTopic, documented)
	}
}

// E2: the golden ShiftPlanCommitted example decodes through the REAL
// cloudevents.Decode and routes through the REAL
// LaborCapacityConsumer.HandleMessage: exactly one LABOR constraint is
// registered for (PACK, BLD1) over [event time, event time+planned_hours)
// with rate planned_heads*planned_rate in UNIT/HOUR (the confirmed contract
// carries no explicit rate unit — this repo's documented default), and the
// replayed CloudEvents id registers nothing more.
func TestGoldenEventsDecode_Workforce(t *testing.T) {
	for _, ex := range workforceGoldenExamples(t, "ShiftPlanCommitted") {
		t.Run(ex.Name, func(t *testing.T) { runWorkforceGoldenCase(t, ex) })
	}
}

// runWorkforceGoldenCase drives one golden ShiftPlanCommitted example
// through the real decode + HandleMessage path.
func runWorkforceGoldenCase(t *testing.T, ex pinnedExample) {
	t.Helper()
	consumer, pcs := newLaborContractHarness()
	ctx := context.Background()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("handle golden example: %v", err)
	}
	data := ex.Payload["data"].(map[string]any)
	at := goldenEventTime(t, ex)
	hours := time.Duration(coerceInt64(t, data["planned_hours"])) * time.Hour
	pc, err := pcs.FindByProcessLocationWindow(ctx, "PACK", "BLD1", at, at.Add(hours))
	if err != nil {
		t.Fatalf("FindByProcessLocationWindow: %v", err)
	}
	if pc == nil {
		t.Fatal("no ProcessCapacity registered for (PACK, BLD1) over the planned window")
	}
	want := float64(coerceInt64(t, data["planned_heads"])) * goldenFloat(t, data["planned_rate"])
	effective, binding, err := pc.EffectiveRate()
	if err != nil {
		t.Fatalf("EffectiveRate: %v", err)
	}
	if binding != processcapacity.ConstraintLabor {
		t.Errorf("binding constraint %v, want LABOR", binding)
	}
	if effective.Quantity() != want {
		t.Errorf("rate %v, want planned_heads*planned_rate = %v", effective.Quantity(), want)
	}
	if effective.Unit() != processcapacity.UnitUnit {
		t.Errorf("unit %v, want UNIT (the documented default)", effective.Unit())
	}

	if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("replay: %v", err)
	}
	pc2, err := pcs.FindByProcessLocationWindow(ctx, "PACK", "BLD1", at, at.Add(hours))
	if err != nil {
		t.Fatalf("replay lookup: %v", err)
	}
	if pc2 == nil {
		t.Fatal("replay removed the registered constraint")
	}
	if e2, _, _ := pc2.EffectiveRate(); e2.Quantity() != want {
		t.Errorf("replayed CloudEvents id %q changed the rate (%v)", ex.Payload["id"], e2.Quantity())
	}
}

// goldenEventTime parses the golden fixture's CloudEvents time.
func goldenEventTime(t *testing.T, ex pinnedExample) time.Time {
	t.Helper()
	raw, _ := ex.Payload["time"].(string)
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("parse golden time %q: %v", raw, err)
	}
	return at
}

// goldenFloat numerically coerces a yaml.v3-decoded number.
func goldenFloat(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case int:
		return float64(n)
	case int64:
		return float64(n)
	case float64:
		return n
	default:
		t.Fatalf("coerce %v (%T) as a number", v, v)
		return 0
	}
}

// E3: the golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source /warehouse/workforce-management,
// subject, time, datacontenttype application/json, dataschema
// urn:warehouse:workforce-management:events:ShiftPlanCommitted:v1 — and the
// pinned spec's envelope schema requires exactly those attributes.
func TestCloudEventsEnvelopeInvariants_Workforce(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedWorkforceSpec)
	for _, ex := range workforceGoldenExamples(t, "ShiftPlanCommitted") {
		assertCloudEventsEnvelope(t, ex.Name, decodeGolden(t, ex), "/warehouse/workforce-management",
			"urn:warehouse:workforce-management:events:ShiftPlanCommitted:v1")
	}
	assertEnvelopeRequired(t, doc, "ShiftPlanCommitted")
}

// E4: this consumer skips and commits past anything it cannot use — a
// non-CloudEvents message and an unknown event TYPE both return nil (no
// DLQ: labor ingestion must never block on foreign traffic); unknown
// ADDITIVE data attributes must not change the registered constraint.
func TestUnknownAttributesTolerated_Workforce(t *testing.T) {
	golden := workforceGoldenExamples(t, "ShiftPlanCommitted")[0].Payload

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, pcs := newLaborContractHarness()
	ctx := context.Background()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	at := goldenEventTime(t, workforceGoldenExamples(t, "ShiftPlanCommitted")[0])
	pc, err := pcs.FindByProcessLocationWindow(ctx, "PACK", "BLD1", at, at.Add(24*time.Hour))
	if err != nil || pc == nil {
		t.Fatalf("unknown data attributes dropped the constraint (pc=%v err=%v)", pc, err)
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCancelled"
	if err := consumer.HandleMessage(ctx, goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}

	if err := consumer.HandleMessage(ctx, goldenJSON(t, map[string]any{"building_id": "BLD1"})); err != nil {
		t.Errorf("non-CloudEvents message must be skipped with nil, got %v", err)
	}
}
