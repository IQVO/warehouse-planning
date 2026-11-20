package kafka

// Event consumer contract tests (E1-E4) for the facility-layout edge:
// StorageCapacityConsumer subscribes to FacilityTopic
// (warehouse.facility.events) and dispatches on the full CloudEvents types
// typeLocationSlotRegistered / typeLocationSlotDecommissioned, folding
// location slots into the storage tally. The contract is the PINNED copy of
// facility-layout's own apis/asyncapi.yaml
// (testdata/facility-layout.asyncapi.yaml, registered in
// contracts/pinned.json; the pinned-contracts CI job fails when the copy
// drifts from IQVO/facility-layout@develop).
//
// Golden fixtures are the producer spec's OWN examples. The spec's three
// LocationSlotRegistered examples cover two documented skip roles (Yard,
// Dock — not Storage/WorkCenter, so the tally maintainer skips them) and
// one Storage role that folds a Location bucket. The spec documents
// LocationSlotDecommissioned WITHOUT examples: that type gets E1 coverage
// only, with the gap named in the PR body (producer example backfill
// pending; a pin is never hand-edited to add one).

import (
	"context"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// newStorageContractHarness wires the REAL StorageCapacityConsumer over
// the memory repositories (the same shape the external unit tests use).
func newStorageContractHarness() (*StorageCapacityConsumer, *memory.StorageTallyRepo, *memory.ProcessedEventRepo) {
	processed, tallyRepo := memory.NewProcessedEventRepo(), memory.NewStorageTallyRepo()
	return &StorageCapacityConsumer{
		Tally:           tallyRepo,
		ProcessedEvents: processed,
		UoW:             memory.NewUnitOfWork(tallyRepo, processed),
		Logger:          contractLogger(),
	}, tallyRepo, processed
}

// facilityGoldenExamples returns the pinned producer spec's examples for
// one message on the subscribed channel.
func facilityGoldenExamples(t *testing.T, message string) []pinnedExample {
	t.Helper()
	return loadPinnedAsyncAPI(t, pinnedFacilityLayoutSpec).goldenMessage(t, message).Examples
}

// E1: the topic this repo's reader subscribes to (FacilityTopic, wired by
// NewStorageCapacityConsumer) is documented by the producer's pinned spec
// as a 'subscribe:' channel — facility-layout's file is
// channel-perspective, its own topics sit under 'subscribe:' — and the
// channel's message list documents both types HandleMessage dispatches on.
func TestPinnedAsyncAPICoversSubscriptions_FacilityLayout(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFacilityLayoutSpec)
	documented := map[string]bool{}
	for _, name := range doc.channelMessageRefs(t, FacilityTopic, "subscribe") {
		documented[doc.documentedConst(t, doc.message(t, name), "type")] = true
	}
	for _, routed := range []string{typeLocationSlotRegistered, typeLocationSlotDecommissioned} {
		if !documented[routed] {
			t.Errorf("pinned facility-layout spec does not document %s on %s (documented: %v)", routed, FacilityTopic, documented)
		}
	}
}

// E2: every LocationSlotRegistered example decodes through the REAL
// cloudevents.Decode and routes through the REAL
// StorageCapacityConsumer.HandleMessage. A Storage-role example folds
// exactly one Location bucket keyed by its (zoneId, locationType); Yard-
// and Dock-role examples are the documented unrecognized-role skip (no
// bucket, no error); the replayed CloudEvents id folds nothing more.
//
// LocationSlotDecommissioned: the pinned spec documents NO examples — E2
// golden coverage is impossible without inventing a payload; the type is
// covered by E1, and the gap is named in the PR body.
func TestGoldenEventsDecode_FacilityLayout(t *testing.T) {
	t.Run("LocationSlotDecommissioned", func(t *testing.T) {
		t.Skipf("pinned facility-layout spec documents locationSlotDecommissioned without examples; golden decode pending producer example backfill (E1 covers the type; never hand-edit a pin)")
	})
	for _, ex := range facilityGoldenExamples(t, "locationSlotRegistered") {
		t.Run(ex.Name, func(t *testing.T) { runFacilityGoldenCase(t, ex) })
	}
}

// runFacilityGoldenCase drives one golden LocationSlotRegistered example
// through the real decode + HandleMessage path.
func runFacilityGoldenCase(t *testing.T, ex pinnedExample) {
	t.Helper()
	consumer, tallyRepo, processed := newStorageContractHarness()
	ctx := context.Background()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("handle golden example: %v", err)
	}
	data := ex.Payload["data"].(map[string]any)
	role, _ := data["role"].(string)
	assertFacilityFold(t, ex, data, role, tallyRepo, processed)

	// Dedupe: the same CloudEvents id never folds twice.
	if err := consumer.HandleMessage(ctx, goldenJSON(t, ex.Payload)); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if role == "" || role == roleStorage {
		if got := tallyRepo.Count(data["zoneId"].(string), tally.TypeLocation, data["locationType"].(string)); got != 1 {
			t.Errorf("replayed CloudEvents id folded again (%d buckets)", got)
		}
	}
}

// assertFacilityFold asserts the documented outcome of one golden example:
// a Storage role (or an absent role, which defaults to Storage) folds
// exactly one claimed Location bucket; any other role is the documented
// parse skip.
func assertFacilityFold(t *testing.T, ex pinnedExample, data map[string]any, role string, tallyRepo *memory.StorageTallyRepo, processed *memory.ProcessedEventRepo) {
	t.Helper()
	zone, _ := data["zoneId"].(string)
	locType, _ := data["locationType"].(string)
	id, _ := ex.Payload["id"].(string)
	if role != "" && role != roleStorage {
		assertFacilitySkip(t, role, zone, locType, id, tallyRepo, processed)
		return
	}
	if got := tallyRepo.Count(zone, tally.TypeLocation, locType); got != 1 {
		t.Fatalf("Storage example folded %d Location buckets for (%s,%s), want 1", got, zone, locType)
	}
	if !processed.Has(storageConsumerName, id) {
		t.Fatalf("folded event was not claimed under %s", storageConsumerName)
	}
}

// assertFacilitySkip asserts the documented unrecognized-role skip: no
// bucket changes, and no processed-event claim — the skip happens BEFORE
// the claim, so a later re-publish under the same id is re-evaluated, not
// swallowed.
func assertFacilitySkip(t *testing.T, role, zone, locType, id string, tallyRepo *memory.StorageTallyRepo, processed *memory.ProcessedEventRepo) {
	t.Helper()
	if got := tallyRepo.Count(zone, tally.TypeLocation, locType) + stationBuckets(tallyRepo, zone); got != 0 {
		t.Fatalf("role %q is a documented skip, but %d buckets changed", role, got)
	}
	if processed.Has(storageConsumerName, id) {
		t.Fatalf("role %q skip claimed the CloudEvents id anyway; parse skips claim nothing", role)
	}
}

// stationBuckets sums the per-activity Station buckets of one zone (zero
// when the example registered none).
func stationBuckets(tallyRepo *memory.StorageTallyRepo, zone string) int {
	total := 0
	for _, activity := range []string{"PACK", "QC", "PICK"} {
		total += tallyRepo.Count(zone, tally.TypeStation, activity)
	}
	return total
}

// E3: every golden fixture carries the fleet-mandatory CloudEvents 1.0
// envelope — specversion 1.0, id, source /warehouse/facility-layout,
// subject, time, datacontenttype application/json, dataschema
// urn:warehouse:facility-layout:events:LocationSlotRegistered:v1 — and the
// pinned spec's envelope schema requires exactly those attributes.
func TestCloudEventsEnvelopeInvariants_FacilityLayout(t *testing.T) {
	doc := loadPinnedAsyncAPI(t, pinnedFacilityLayoutSpec)
	for _, ex := range facilityGoldenExamples(t, "locationSlotRegistered") {
		assertCloudEventsEnvelope(t, ex.Name, decodeGolden(t, ex), "/warehouse/facility-layout",
			"urn:warehouse:facility-layout:events:LocationSlotRegistered:v1")
	}
	assertEnvelopeRequired(t, doc, "locationSlotRegistered")
}

// E4: this consumer skips and commits past anything it cannot use — a
// non-CloudEvents message and an unknown event TYPE both return nil (no
// DLQ on this path: the storage tally must never block on foreign
// traffic); unknown ADDITIVE data attributes must not change the fold.
func TestUnknownAttributesTolerated_FacilityLayout(t *testing.T) {
	golden := facilityGoldenExamples(t, "locationSlotRegistered")[2].Payload // storageBinWithoutIdentity

	tolerant := clonePayload(golden)
	data := tolerant["data"].(map[string]any)
	data["future_additive_field"] = "unknown-to-this-consumer"
	data["nested_extension"] = map[string]any{"deep": []any{1, "two"}}
	consumer, tallyRepo, _ := newStorageContractHarness()
	ctx := context.Background()
	if err := consumer.HandleMessage(ctx, goldenJSON(t, tolerant)); err != nil {
		t.Fatalf("handle with unknown data attributes: %v", err)
	}
	if got := tallyRepo.Count("WH1-STOR-AMB", tally.TypeLocation, "PalletRack"); got != 1 {
		t.Errorf("unknown data attributes must not change the fold (%d buckets)", got)
	}

	otherType := clonePayload(golden)
	otherType["type"] = "com.warehouse.wms.facility-layout.site.SiteRegistered"
	if err := consumer.HandleMessage(ctx, goldenJSON(t, otherType)); err != nil {
		t.Fatalf("unknown event type must be skipped, not an error: %v", err)
	}
	if got := tallyRepo.Count("WH1-STOR-AMB", tally.TypeLocation, "PalletRack"); got != 1 {
		t.Errorf("unknown event type was acted on (%d buckets)", got)
	}

	if err := consumer.HandleMessage(ctx, goldenJSON(t, map[string]any{"locationCode": "WH1-A-01"})); err != nil {
		t.Errorf("non-CloudEvents message must be skipped with nil, got %v", err)
	}
}
