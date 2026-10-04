package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/analytics/report"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

const (
	anPlanID  = "0b7a4c1e-5d52-4f0e-9a39-6c1f2f3a8b10"
	anTypePre = "com.warehouse.wes.warehouse-planning.capacityplan."
)

var (
	anCreatedAt   = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	anPublishedAt = time.Date(2026, 10, 5, 8, 10, 0, 0, time.UTC)
	anRange       = report.Range{From: time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)}
)

// analyticsWire encodes ev exactly as the OLTP service writes it to the
// analytics topic (the real AnalyticsEncoder), under the given CloudEvents id.
func analyticsWire(t *testing.T, id string, ev capacityplan.Event) []byte {
	t.Helper()
	msgs, err := (&outboundkafka.AnalyticsEncoder{NewID: func() string { return id }}).Encode(ev)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("encode: %d msgs, %v", len(msgs), err)
	}
	return msgs[0].Value
}

func anCreated() capacityplan.Event {
	return capacityplan.CapacityPlanCreated{
		Header: capacityplan.Header{PlanID: anPlanID, At: anCreatedAt}, WarehouseID: "WH-1", Location: "SIM1", PathID: "pick-rebin-pack",
		WindowStart: anCreatedAt, WindowEnd: anCreatedAt.Add(8 * time.Hour), AssignedDemand: 12000, PathCapacity: 1000,
		CapacityOverWindow: 8000, Shortage: 4000, BottleneckStep: "REBIN",
	}
}

func anPublished() capacityplan.Event {
	return capacityplan.CapacityPlanPublished{
		Header: capacityplan.Header{PlanID: anPlanID, At: anPublishedAt}, WarehouseID: "WH-1", Location: "SIM1", PathID: "pick-rebin-pack",
		WindowStart: anCreatedAt, WindowEnd: anCreatedAt.Add(8 * time.Hour), AssignedDemand: 12000, PathCapacity: 1000,
		CapacityOverWindow: 8000, Shortage: 4000, BottleneckStep: "REBIN", BottleneckConstraint: "STATION",
	}
}

// anWire builds a hand-made CloudEvent (for payload variants the real encoder
// would never produce).
func anWire(t *testing.T, id, typ, subject string, at time.Time, data any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/warehouse-planning")
	e.SetType(typ)
	e.SetSubject(subject)
	if !at.IsZero() {
		e.SetTime(at)
	}
	if err := e.SetData("application/json", data); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// lockedBuffer is a goroutine-safe log sink.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func newJSONLogger(w io.Writer) *slog.Logger { return slog.New(slog.NewJSONHandler(w, nil)) }

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) lines(substr string) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := 0
	for _, line := range strings.Split(l.b.String(), "\n") {
		if line != "" && strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

type fakeDLQ struct {
	mu   sync.Mutex
	msgs []kafkago.Message
	fail int // fail the next n writes
}

func (f *fakeDLQ) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail > 0 {
		f.fail--
		return errors.New("dlq broker down")
	}
	f.msgs = append(f.msgs, msgs...)
	return nil
}
func (f *fakeDLQ) Close() error { return nil }
func (f *fakeDLQ) written() []kafkago.Message {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]kafkago.Message(nil), f.msgs...)
}

// recordingProjection records every PlanEvent and can fail on demand.
type recordingProjection struct {
	mu     sync.Mutex
	events []report.PlanEvent
	errs   []error // returned in order, one per call, then nil
	calls  int
}

func (r *recordingProjection) Apply(_ context.Context, e report.PlanEvent) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls++
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		if err != nil {
			return false, err
		}
	}
	r.events = append(r.events, e)
	return true, nil
}

func (r *recordingProjection) callCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

type anHarness struct {
	consumer *kafkaconsumer.AnalyticsConsumer
	dlq      *fakeDLQ
	logs     *lockedBuffer
	clock    *time.Time
}

func newAnHarness(p report.Projection) *anHarness {
	logs := &lockedBuffer{}
	clock := time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)
	h := &anHarness{dlq: &fakeDLQ{}, logs: logs, clock: &clock}
	h.consumer = &kafkaconsumer.AnalyticsConsumer{
		Projection: p, DLQ: h.dlq, DLQTopic: "analytics.dlq",
		Logger: newJSONLogger(logs),
		Retry:  kafkaconsumer.RetryPolicy{Initial: time.Millisecond, Max: 2 * time.Millisecond},
		Now:    func() time.Time { return *h.clock },
	}
	return h
}

// run feeds values through the REAL Run loop and waits for all commits.
func (h *anHarness) run(t *testing.T, values ...[]byte) *fakeReader {
	t.Helper()
	reader := newFakeReader(values...)
	h.consumer.Reader = reader
	if err := runUntilCommits(t, reader, len(values), h.consumer.Run); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}
	return reader
}

func TestAnalyticsConsumer_ValidEventsUpdateTheModelAndCommit(t *testing.T) {
	store := analyticsstore.NewMemory()
	h := newAnHarness(store)
	reader := h.run(t, analyticsWire(t, "evt-created", anCreated()), analyticsWire(t, "evt-published", anPublished()))

	counts, _ := store.BottleneckCounts(context.Background(), anRange)
	want := []report.BottleneckCount{{Site: report.Site{WarehouseID: "WH-1", Location: "SIM1"}, BottleneckStep: "REBIN", BindingConstraint: "STATION", Plans: 1}}
	if len(counts) != 1 || counts[0] != want[0] {
		t.Fatalf("bottleneck counts = %+v, want %+v", counts, want)
	}
	lat, _ := store.Latencies(context.Background(), anRange)
	if len(lat) != 1 || lat[0].Plans != 1 || lat[0].MedianSeconds != 600 {
		t.Fatalf("latency = %+v, want one plan at 600s (10 minutes between the two CloudEvents times)", lat)
	}
	if got := reader.log(); len(got) != 4 || got[1] != "commit:0" || got[3] != "commit:1" {
		t.Fatalf("reader events = %v, want fetch/commit per message", got)
	}
	if n := len(h.dlq.written()); n != 0 {
		t.Fatalf("%d messages dead-lettered, want 0", n)
	}
}

func TestAnalyticsConsumer_ReplayOfTheSameIDIsANoOp(t *testing.T) {
	store := analyticsstore.NewMemory()
	h := newAnHarness(store)
	published := analyticsWire(t, "evt-published", anPublished())
	reader := h.run(t, published, published, published)

	days, _ := store.ShortageDays(context.Background(), anRange)
	if len(days) != 1 || days[0].PlansPublished != 1 || days[0].TotalShortage != 4000 {
		t.Fatalf("shortage days = %+v; a redelivered id must not be counted twice", days)
	}
	if len(reader.log()) != 6 {
		t.Fatalf("reader events = %v: every duplicate must still be committed", reader.log())
	}
}

func TestAnalyticsConsumer_UnknownTypeIsIgnoredNotDeadLettered(t *testing.T) {
	proj := &recordingProjection{}
	h := newAnHarness(proj)
	other := anWire(t, "evt-other", anTypePre+"SomethingElse", anPlanID, anCreatedAt, map[string]any{"plan_id": anPlanID})
	foreign := anWire(t, "evt-foreign", "com.warehouse.wes.order-management.order.OrderAllocated", "o-1", anCreatedAt, map[string]any{"order_id": "o-1"})
	h.run(t, other, foreign)
	if proj.callCount() != 0 || len(h.dlq.written()) != 0 {
		t.Fatalf("projection calls = %d, dlq = %d; unknown types are acknowledged untouched", proj.callCount(), len(h.dlq.written()))
	}
}

func TestAnalyticsConsumer_LegacyAndGarbageAreSkippedWithoutFloodingTheLog(t *testing.T) {
	proj := &recordingProjection{}
	h := newAnHarness(proj)
	legacy := []byte(`{"event_id":"e-1","event_type":"CapacityPlanPublished","occurred_at":"2026-10-05T08:00:00Z","payload":{"plan_id":"x"}}`)
	garbage := []byte(`not json at all \x00\x01`)
	values := make([][]byte, 0, 5000)
	for i := 0; i < 2500; i++ {
		values = append(values, legacy, garbage)
	}
	reader := h.run(t, values...)

	if proj.callCount() != 0 || len(h.dlq.written()) != 0 {
		t.Fatalf("projection calls = %d, dlq = %d; legacy/garbage are skipped, not projected or dead-lettered", proj.callCount(), len(h.dlq.written()))
	}
	if commits := len(reader.log()) / 2; commits != 5000 {
		t.Fatalf("committed %d of 5000 skipped messages", commits)
	}
	if warns := h.logs.lines("not a CloudEvents"); warns != 1 {
		t.Fatalf("%d WARN lines for 5000 skipped messages within one interval, want exactly 1 (no flooding)", warns)
	}

	// The next interval logs once more and reports how many were suppressed.
	*h.clock = h.clock.Add(61 * time.Second)
	h.run(t, legacy)
	if warns := h.logs.lines("not a CloudEvents"); warns != 2 {
		t.Fatalf("%d WARN lines after the interval elapsed, want 2", warns)
	}
	if !strings.Contains(h.logs.String(), `"suppressed_since_last_warning":4999`) {
		t.Fatalf("second warning does not report the 4999 suppressed skips: %s", h.logs.String())
	}
}

func TestAnalyticsConsumer_TransientFailuresAreRetriedOnTheSameMessageAndNeverDeadLettered(t *testing.T) {
	proj := &recordingProjection{errs: []error{errors.New("connection reset"), errors.New("connection reset"), errors.New("timeout")}}
	h := newAnHarness(proj)
	reader := h.run(t, analyticsWire(t, "evt-published", anPublished()))

	if proj.callCount() != 4 {
		t.Fatalf("Apply called %d times, want 4 (three transient failures then success) for the SAME message", proj.callCount())
	}
	if got := reader.log(); len(got) != 2 || got[0] != "fetch:0" || got[1] != "commit:0" {
		t.Fatalf("reader events = %v: the offset must be committed once, after success", got)
	}
	if len(h.dlq.written()) != 0 {
		t.Fatal("a transient failure must never reach the DLQ")
	}
}

func TestAnalyticsConsumer_TransientFailureNeverCommitsPastTheMessage(t *testing.T) {
	proj := &recordingProjection{errs: []error{errors.New("db down")}}
	h := newAnHarness(proj)
	err := h.consumer.HandleMessage(context.Background(), kafkago.Message{Value: analyticsWire(t, "evt-1", anPublished())})
	if err == nil || errors.Is(err, report.ErrRejected) {
		t.Fatalf("HandleMessage = %v, want a transient (non-poison) error", err)
	}
}

func TestAnalyticsConsumer_DeterministicPoisonGoesToTheDLQWithTheRawBytesAndIsNotRetried(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"missing warehouse_id", anWire(t, "evt-1", anTypePre+"CapacityPlanPublished", anPlanID, anPublishedAt, map[string]any{"plan_id": anPlanID, "location": "SIM1"})},
		{"missing location", anWire(t, "evt-2", anTypePre+"CapacityPlanCreated", anPlanID, anCreatedAt, map[string]any{"plan_id": anPlanID, "warehouse_id": "WH-1"})},
		{"plan_id is not the subject", anWire(t, "evt-3", anTypePre+"BottleneckDetected", "another-plan", anCreatedAt, map[string]any{"plan_id": anPlanID, "warehouse_id": "WH-1", "location": "SIM1"})},
		{"negative shortage", anWire(t, "evt-4", anTypePre+"CapacityShortageDetected", anPlanID, anCreatedAt, map[string]any{"plan_id": anPlanID, "warehouse_id": "WH-1", "location": "SIM1", "shortage": -5})},
		{"no time", anWire(t, "evt-5", anTypePre+"CapacityPlanCreated", anPlanID, time.Time{}, map[string]any{"plan_id": anPlanID, "warehouse_id": "WH-1", "location": "SIM1"})},
		{"data of the wrong shape", anWire(t, "evt-6", anTypePre+"CapacityPlanCreated", anPlanID, anCreatedAt, map[string]any{"plan_id": 42, "warehouse_id": "WH-1", "location": "SIM1"})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proj := &recordingProjection{}
			h := newAnHarness(proj)
			reader := h.run(t, tc.raw)
			if proj.callCount() != 0 {
				t.Fatalf("Apply called for poison")
			}
			got := h.dlq.written()
			if len(got) != 1 || !bytes.Equal(got[0].Value, tc.raw) {
				t.Fatalf("dlq = %d messages; want exactly the raw poison bytes", len(got))
			}
			headers := map[string]string{}
			for _, hd := range got[0].Headers {
				headers[hd.Key] = string(hd.Value)
			}
			if headers["x-dlq-error"] == "" || headers["x-dlq-failed-at"] == "" || !strings.Contains(strings.Join(reader.log(), ","), "commit:0") {
				t.Fatalf("dlq headers = %v, reader = %v; want error context and the offset committed", headers, reader.log())
			}
		})
	}
}

func TestAnalyticsConsumer_ARejectionByTheStoreIsDeadLetteredAfterASingleAttempt(t *testing.T) {
	proj := &recordingProjection{errs: []error{report.ErrRejected}}
	h := newAnHarness(proj)
	raw := analyticsWire(t, "evt-1", anPublished())
	h.run(t, raw)
	if proj.callCount() != 1 {
		t.Fatalf("Apply called %d times, want 1: a deterministic rejection is not retried", proj.callCount())
	}
	if got := h.dlq.written(); len(got) != 1 || !bytes.Equal(got[0].Value, raw) {
		t.Fatalf("dlq = %+v", got)
	}
}

func TestAnalyticsConsumer_ADLQWriteFailureRetriesTheMessageInsteadOfLosingIt(t *testing.T) {
	proj := &recordingProjection{}
	h := newAnHarness(proj)
	h.dlq.fail = 2
	bad := anWire(t, "evt-bad", anTypePre+"CapacityPlanCreated", anPlanID, anCreatedAt, map[string]any{"plan_id": anPlanID})
	reader := h.run(t, bad)
	if len(h.dlq.written()) != 1 {
		t.Fatalf("dlq = %d, want the poison written once the DLQ recovered", len(h.dlq.written()))
	}
	if got := reader.log(); got[len(got)-1] != "commit:0" || len(got) != 2 {
		t.Fatalf("reader = %v: commit only after the DLQ write succeeded", got)
	}
}

// Every field of every event kind maps exactly as documented in ADR 0005.
func TestAnalyticsConsumer_EventMapping(t *testing.T) {
	proj := &recordingProjection{}
	h := newAnHarness(proj)
	h.run(t,
		analyticsWire(t, "e-created", anCreated()),
		analyticsWire(t, "e-published", anPublished()),
		analyticsWire(t, "e-shortage", capacityplan.CapacityShortageDetected{
			Header: capacityplan.Header{PlanID: anPlanID, At: anPublishedAt}, WarehouseID: "WH-1", Location: "SIM1", Shortage: 4000, BottleneckStep: "REBIN"}),
		analyticsWire(t, "e-bottleneck", capacityplan.BottleneckDetected{
			Header: capacityplan.Header{PlanID: anPlanID, At: anPublishedAt}, WarehouseID: "WH-1", Location: "SIM1", BottleneckStep: "REBIN", PathCapacity: 1000}),
	)
	if len(proj.events) != 4 {
		t.Fatalf("events = %d", len(proj.events))
	}
	step, shortageUnits, station := "REBIN", 4000.0, "STATION"
	site := func(e report.PlanEvent) report.PlanEvent {
		e.PlanID, e.WarehouseID, e.Location = anPlanID, "WH-1", "SIM1"
		return e
	}
	want := []report.PlanEvent{
		site(report.PlanEvent{Kind: report.KindCreated, EventID: "e-created", At: anCreatedAt,
			BottleneckStep: &step, Shortage: &shortageUnits, CreatedAt: &anCreatedAt}),
		site(report.PlanEvent{Kind: report.KindPublished, EventID: "e-published", At: anPublishedAt,
			BottleneckStep: &step, Shortage: &shortageUnits, BindingConstraint: &station, PublishedAt: &anPublishedAt}),
		site(report.PlanEvent{Kind: report.KindShortageDetected, EventID: "e-shortage", At: anPublishedAt,
			BottleneckStep: &step, Shortage: &shortageUnits}),
		site(report.PlanEvent{Kind: report.KindBottleneckDetected, EventID: "e-bottleneck", At: anPublishedAt,
			BottleneckStep: &step}),
	}
	for i := range want {
		if !reflect.DeepEqual(proj.events[i], want[i]) {
			t.Errorf("event %d (%s) =\n%+v\nwant\n%+v", i, want[i].Kind, proj.events[i], want[i])
		}
	}
}

// A Published event of a plan created before binding_constraint existed
// carries an empty one: it is stored as "" (unknown), not dropped.
func TestAnalyticsConsumer_PublishedWithoutAConstraintKeyKeepsItNil(t *testing.T) {
	proj := &recordingProjection{}
	h := newAnHarness(proj)
	h.run(t, anWire(t, "e-old", anTypePre+"CapacityPlanPublished", anPlanID, anPublishedAt,
		map[string]any{"plan_id": anPlanID, "warehouse_id": "WH-1", "location": "SIM1", "shortage": 0, "bottleneck_step": "PACK"}))
	if len(proj.events) != 1 || proj.events[0].BindingConstraint != nil {
		t.Fatalf("events = %+v", proj.events)
	}
}
