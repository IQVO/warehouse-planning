package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

const (
	typeStandardDefined   = "com.warehouse.wes.labor-performance.standard.LaborStandardDefined"
	typeStandardRevised   = "com.warehouse.wes.labor-performance.standard.LaborStandardRevised"
	typeTaskPerformance   = "com.warehouse.wes.labor-performance.performance.TaskPerformanceRecorded"
	laborStandardConsumer = "labor-standard-consumer"
)

var (
	standardAt     = time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	standardFrom   = time.Date(2026, 11, 18, 9, 0, 0, 0, time.UTC)
	standardWindow = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
)

// standardWire builds the exact CloudEvents 1.0 structured-mode bytes
// labor-performance publishes on its integration topic (docs/adr/0022
// decision 2: same type and data as the analytics copy, dataschema on the
// events stream, subject = standard id).
func standardWire(t *testing.T, id, typ string, at time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/labor-performance")
	e.SetType(typ)
	if sid, _ := data["standard_id"].(string); sid != "" {
		e.SetSubject(sid)
	} else {
		e.SetSubject("std-unknown")
	}
	if !at.IsZero() {
		e.SetTime(at)
	}
	e.SetDataSchema("urn:warehouse:labor-performance:events:LaborStandardDefined:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// definedData is a LaborStandardDefined `data` as the producer's pinned
// shape documents it (required fields only).
func definedData(id string, seconds int64) map[string]any {
	return map[string]any{
		"standard_id": id, "task_type": "PICK", "expected_seconds": seconds,
		"effective_from": standardFrom.Format(time.RFC3339),
	}
}

type flakyStandards struct {
	*memory.LaborStandardRepo
	mu       sync.Mutex
	failures int
	records  int
}

func (f *flakyStandards) Record(ctx context.Context, s laborstandard.Standard) (bool, error) {
	f.mu.Lock()
	f.records++
	fail := f.failures > 0
	if fail {
		f.failures--
	}
	f.mu.Unlock()
	if fail {
		return false, errInjected
	}
	return f.LaborStandardRepo.Record(ctx, s)
}

func (f *flakyStandards) recordCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records
}

type standardHarness struct {
	consumer  *kafkaconsumer.LaborStandardConsumer
	repo      *memory.LaborStandardRepo
	flaky     *flakyStandards
	processed *memory.ProcessedEventRepo
	uow       *countingUoW
}

func newStandardHarness() standardHarness {
	repo, processed := memory.NewLaborStandardRepo(), memory.NewProcessedEventRepo()
	flaky := &flakyStandards{LaborStandardRepo: repo}
	uow := &countingUoW{inner: memory.NewUnitOfWork(repo, processed)}
	return standardHarness{
		consumer: &kafkaconsumer.LaborStandardConsumer{
			Record: &usecases.RecordLaborStandard{UoW: uow, ProcessedEvents: processed, Standards: flaky},
			Logger: testLogger(), Retry: fastRetry,
		},
		repo: repo, flaky: flaky, processed: processed, uow: uow,
	}
}

func (h standardHarness) handle(t *testing.T, msgs ...[]byte) {
	t.Helper()
	for _, m := range msgs {
		if err := h.consumer.HandleMessage(context.Background(), m); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
}

func (h standardHarness) inForce(t *testing.T, site string) (laborstandard.Standard, bool) {
	t.Helper()
	s, ok, err := h.repo.InForce(context.Background(), site, "PICK", standardWindow)
	if err != nil {
		t.Fatal(err)
	}
	return s, ok
}

func TestLaborStandardConsumer_DefinedIsStoredWithEveryFact(t *testing.T) {
	h := newStandardHarness()
	data := definedData("std-0003", 45)
	data["travel_component_seconds"] = 15
	h.handle(t, standardWire(t, "evt-1", typeStandardDefined, standardAt, data))
	s, ok := h.repo.Get("std-0003")
	if !ok {
		t.Fatal("standard not in the copy")
	}
	if s.TaskType() != "PICK" || s.ExpectedSeconds() != 45 || s.SiteCode() != "" || s.HandlingUnit() != laborstandard.HandlingUnitNone ||
		!s.EffectiveFrom().Equal(standardFrom) || !s.AsOf().Equal(standardAt) || s.TravelComponentSeconds() == nil || *s.TravelComponentSeconds() != 15 {
		t.Fatalf("standard = %+v", s)
	}
	if !h.processed.Has(laborStandardConsumer, "evt-1") {
		t.Fatal("the event id was not claimed under labor-standard-consumer")
	}
}

// The optional fields of the pinned shape (decision 2): site_code (ADR 0038),
// handling_unit and the Revised-only previous_expected_seconds and team-lift
// fields decode; the ignored ones change nothing.
func TestLaborStandardConsumer_RevisedWithTheOptionalFields(t *testing.T) {
	h := newStandardHarness()
	revised := definedData("std-0002", 40)
	revised["previous_expected_seconds"] = 45
	revised["site_code"] = "SIM1"
	caseStd := definedData("std-0004", 30)
	caseStd["handling_unit"] = "CASE"
	caseStd["team_lift_threshold_g"] = 18000
	caseStd["team_lift_expected_seconds"] = 75
	h.handle(t,
		standardWire(t, "evt-r", typeStandardRevised, standardAt, revised),
		standardWire(t, "evt-c", typeStandardDefined, standardAt, caseStd),
	)
	if s, ok := h.repo.Get("std-0002"); !ok || s.SiteCode() != "SIM1" || s.ExpectedSeconds() != 40 || !s.SiteScoped() {
		t.Fatalf("revised standard = %+v %v", s, ok)
	}
	if s, ok := h.repo.Get("std-0004"); !ok || s.HandlingUnit() != laborstandard.HandlingUnitCase {
		t.Fatalf("handling-unit standard = %+v %v", s, ok)
	}
	if s, ok := h.inForce(t, "SIM1"); !ok || s.ID() != "std-0002" {
		t.Fatalf("SIM1 in force = %+v %v, want the site-specific std-0002 (the CASE one is never used)", s, ok)
	}
	if _, ok := h.inForce(t, "SIM2"); ok {
		t.Fatal("SIM2 has no site-less task-type-only standard: nothing may apply")
	}
}

// A revision opens a new record; the copy keeps both and the read picks the
// latest effective start before the window, whatever the arrival order.
func TestLaborStandardConsumer_RevisionArrivingFirstStillWins(t *testing.T) {
	h := newStandardHarness()
	revised := definedData("std-2", 40)
	revised["effective_from"] = standardFrom.Add(time.Hour).Format(time.RFC3339)
	revised["previous_expected_seconds"] = 45
	h.handle(t,
		standardWire(t, "evt-rev", typeStandardRevised, standardAt.Add(time.Hour), revised),
		standardWire(t, "evt-def", typeStandardDefined, standardAt, definedData("std-1", 45)),
	)
	if h.repo.Len() != 2 {
		t.Fatalf("copy holds %d records, want both facts", h.repo.Len())
	}
	if s, ok := h.inForce(t, "SIM1"); !ok || s.ID() != "std-2" || s.ExpectedSeconds() != 40 {
		t.Fatalf("in force = %+v %v, want the revision", s, ok)
	}
}

// One row per standard id: a redelivery under the SAME CloudEvents id is a
// duplicate (claim), and the same standard id under a NEW event id is claimed
// but changes nothing (the first record of an id is kept).
func TestLaborStandardConsumer_SameEventIDOrSameStandardIDIsANoOp(t *testing.T) {
	h := newStandardHarness()
	h.handle(t,
		standardWire(t, "evt-1", typeStandardDefined, standardAt, definedData("std-1", 45)),
		standardWire(t, "evt-1", typeStandardDefined, standardAt, definedData("std-1", 99)),
		standardWire(t, "evt-2", typeStandardDefined, standardAt.Add(time.Minute), definedData("std-1", 77)),
	)
	if s, _ := h.repo.Get("std-1"); h.repo.Len() != 1 || s.ExpectedSeconds() != 45 || !s.AsOf().Equal(standardAt) {
		t.Fatalf("copy = %d records, std-1 = %+v; want the first record unchanged", h.repo.Len(), s)
	}
	if got := h.flaky.recordCalls(); got != 2 {
		t.Fatalf("Record called %d times, want 2 (the replayed event id never reaches it)", got)
	}
	if !h.processed.Has(laborStandardConsumer, "evt-2") {
		t.Fatal("the same-standard-id event must still be claimed")
	}
}

// Only the two standard types are consumed, matched on the FULL type string.
func TestLaborStandardConsumer_DispatchesOnTheFullTypeAndIgnoresTheRest(t *testing.T) {
	h := newStandardHarness()
	for name, typ := range map[string]string{
		"TaskPerformanceRecorded (same topic)": typeTaskPerformance,
		"right suffix, wrong owner":            "com.warehouse.wes.someone-else.standard.LaborStandardDefined",
		"short name only":                      "LaborStandardDefined",
		"analytics-style suffix":               typeStandardDefined + ".v2",
	} {
		msg := standardWire(t, "evt-"+name, typ, standardAt, definedData("std-1", 45))
		if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
	if h.repo.Len() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("an ignored type wrote %d records / opened %d units of work", h.repo.Len(), h.uow.calls.Load())
	}
}

// Missing required field, expected_seconds <= 0, or anything else a retry
// cannot fix is poison: dead-lettered, never retried, and validated before the
// transaction opens.
func TestLaborStandardConsumer_UnusableMessagesArePoisonAndNeverTouchTheDatabase(t *testing.T) {
	h := newStandardHarness()
	legacyFlat, _ := json.Marshal(map[string]any{"event_id": "e-1", "event_type": "LaborStandardDefined", "payload": definedData("std-1", 45)})
	with := func(mutate func(map[string]any)) map[string]any {
		d := definedData("std-1", 45)
		mutate(d)
		return d
	}
	w := func(name, typ string, at time.Time, data map[string]any) []byte {
		return standardWire(t, "e-"+name, typ, at, data)
	}
	cases := map[string][]byte{
		"not json":                      []byte("this is not json"),
		"empty":                         nil,
		"json but not an event":         []byte(`{"hello":"world"}`),
		"legacy flat envelope":          legacyFlat,
		"no standard_id":                w("a", typeStandardDefined, standardAt, with(func(d map[string]any) { delete(d, "standard_id") })),
		"blank standard_id":             w("b", typeStandardRevised, standardAt, with(func(d map[string]any) { d["standard_id"] = "" })),
		"no task_type":                  w("c", typeStandardDefined, standardAt, with(func(d map[string]any) { delete(d, "task_type") })),
		"no expected_seconds":           w("d", typeStandardDefined, standardAt, with(func(d map[string]any) { delete(d, "expected_seconds") })),
		"zero expected_seconds":         w("e", typeStandardRevised, standardAt, with(func(d map[string]any) { d["expected_seconds"] = 0 })),
		"negative expected_seconds":     w("f", typeStandardDefined, standardAt, with(func(d map[string]any) { d["expected_seconds"] = -5 })),
		"fractional expected_seconds":   w("g", typeStandardDefined, standardAt, with(func(d map[string]any) { d["expected_seconds"] = 4.5 })),
		"no effective_from":             w("h", typeStandardDefined, standardAt, with(func(d map[string]any) { delete(d, "effective_from") })),
		"malformed effective_from":      w("i", typeStandardDefined, standardAt, with(func(d map[string]any) { d["effective_from"] = "soon" })),
		"negative travel component":     w("j", typeStandardDefined, standardAt, with(func(d map[string]any) { d["travel_component_seconds"] = -1 })),
		"unknown handling unit":         w("k", typeStandardDefined, standardAt, with(func(d map[string]any) { d["handling_unit"] = "INNER" })),
		"no event time":                 w("l", typeStandardDefined, time.Time{}, definedData("std-1", 45)),
		"data is not an object":         w("m", typeStandardDefined, standardAt, map[string]any{"standard_id": []string{"x"}}),
		"expected_seconds is a string":  w("n", typeStandardDefined, standardAt, with(func(d map[string]any) { d["expected_seconds"] = "45" })),
		"revised: no expected_seconds":  w("o", typeStandardRevised, standardAt, with(func(d map[string]any) { delete(d, "expected_seconds") })),
		"revised: no effective_from":    w("p", typeStandardRevised, standardAt, with(func(d map[string]any) { delete(d, "effective_from") })),
		"revised: no task_type":         w("q", typeStandardRevised, standardAt, with(func(d map[string]any) { d["task_type"] = "" })),
		"revised: no event time":        w("r", typeStandardRevised, time.Time{}, definedData("std-1", 45)),
		"defined: travel is not number": w("s", typeStandardDefined, standardAt, with(func(d map[string]any) { d["travel_component_seconds"] = "15" })),
	}
	for name, msg := range cases {
		err := h.consumer.HandleMessage(context.Background(), msg)
		if err == nil || !kafkaconsumer.IsPoisonForTest(err) {
			t.Errorf("%s: err = %v, want a poison error (dead-letter, never retry)", name, err)
		}
	}
	if h.repo.Len() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("poison touched the copy: %d records, %d units of work (validation must run before the transaction)", h.repo.Len(), h.uow.calls.Load())
	}
}

func TestLaborStandardConsumer_TransientFailureLeavesNothingAndRetrySucceeds(t *testing.T) {
	h := newStandardHarness()
	ctx := context.Background()
	msg := standardWire(t, "evt-1", typeStandardDefined, standardAt, definedData("std-1", 45))

	h.flaky.failures = 1
	err := h.consumer.HandleMessage(ctx, msg)
	if !errors.Is(err, errInjected) || kafkaconsumer.IsPoisonForTest(err) {
		t.Fatalf("err = %v, want the injected infrastructure error (so the loop retries, not dead-letters)", err)
	}
	if h.repo.Len() != 0 || h.processed.Has(laborStandardConsumer, "evt-1") {
		t.Fatal("a failed handling left a record or a claim behind")
	}
	if err := h.consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, ok := h.repo.Get("std-1"); !ok || !h.processed.Has(laborStandardConsumer, "evt-1") {
		t.Fatal("the retried message never landed")
	}
}

func TestLaborStandardConsumer_RunCommitsAfterSuccessAndDeadLettersPoisonWithoutRetry(t *testing.T) {
	h := newStandardHarness()
	dlq := &fakeDLQWriter{}
	h.consumer.DLQ, h.consumer.DLQTopic = dlq, "warehouse.labor-performance.events.dlq"
	poison := standardWire(t, "evt-bad", typeStandardDefined, standardAt, definedData("std-bad", 0))
	reader := newFakeReader(
		standardWire(t, "evt-1", typeStandardDefined, standardAt, definedData("std-1", 45)),
		poison,
		standardWire(t, "evt-2", typeStandardRevised, standardAt.Add(time.Hour), definedData("std-2", 40)),
	)
	h.consumer.Reader = reader
	h.flaky.failures = 1 // the first message fails once, then succeeds

	if err := runUntilCommits(t, reader, 3, h.consumer.Run); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	want := []string{"fetch:0", "commit:0", "fetch:1", "commit:1", "fetch:2", "commit:2"}
	got := reader.log()
	if len(got) != len(want) {
		t.Fatalf("reader events = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("reader events = %v, want %v", got, want)
		}
	}
	published := dlq.published()
	if len(published) != 1 || string(published[0].Value) != string(poison) || published[0].Topic != "warehouse.labor-performance.events.dlq" {
		t.Fatalf("DLQ = %+v, want exactly the expected_seconds=0 message on the .dlq topic", published)
	}
	if h.repo.Len() != 2 {
		t.Fatalf("copy holds %d records: the messages around the poison one must still be applied", h.repo.Len())
	}
	if got := h.flaky.recordCalls(); got != 3 {
		t.Fatalf("Record attempts = %d, want 3 (one failure + one success on the first message, one on the third)", got)
	}
}

func TestNewLaborStandardConsumer_ReadsTheIntegrationTopicWithItsDLQ(t *testing.T) {
	c := kafkaconsumer.NewLaborStandardConsumer([]string{"127.0.0.1:1"}, "g-labor-standards", &usecases.RecordLaborStandard{}, nil)
	defer func() { _ = c.Close() }()
	if kafkaconsumer.LaborPerformanceTopic != "warehouse.labor-performance.events" {
		t.Fatalf("topic = %s, want labor-performance's INTEGRATION topic (never its analytics topic)", kafkaconsumer.LaborPerformanceTopic)
	}
	if c.DLQTopic != "warehouse.labor-performance.events.dlq" || c.DLQ == nil || c.Logger == nil {
		t.Fatalf("consumer = %+v, want the .dlq writer and a logger", c)
	}
}
