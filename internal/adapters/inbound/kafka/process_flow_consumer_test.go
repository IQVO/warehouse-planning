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
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

const (
	typeFlowDefined = "com.warehouse.wes.process-path-management.processflow.ProcessFlowDefined"
	typeFlowRevised = "com.warehouse.wes.process-path-management.processflow.ProcessFlowRevised"
	typeFlowRetired = "com.warehouse.wes.process-path-management.processflow.ProcessFlowRetired"
	typePathCreated = "com.warehouse.wes.process-path-management.processpath.ProcessPathCreated"
	typeCPTChanged  = "com.warehouse.wes.process-path-management.cptschedule.CPTScheduleChanged"
	processFlowName = "process-flow-consumer"
)

var flowAt = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)

// flowWire builds the exact CloudEvents 1.0 structured-mode bytes
// process-path-management publishes on its integration topic (subject = flow id).
func flowWire(t *testing.T, id, typ string, at time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/process-path-management")
	e.SetType(typ)
	if fid, _ := data["flow_id"].(string); fid != "" {
		e.SetSubject(fid)
	} else {
		e.SetSubject("flow-unknown")
	}
	if !at.IsZero() {
		e.SetTime(at)
	}
	e.SetDataSchema("urn:warehouse:process-path-management:events:ProcessFlowDefined:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func flowData(id string, version int, status string, types ...string) map[string]any {
	stages := make([]map[string]any, 0, len(types))
	for _, pt := range types {
		stages = append(stages, map[string]any{"path_id": pt, "process_type": pt})
	}
	return map[string]any{"flow_id": id, "name": "Flow " + id, "stages": stages, "status": status, "version": version}
}

type flakyFlows struct {
	*memory.ProcessFlowRepo
	mu       sync.Mutex
	failures int
	records  int
}

func (f *flakyFlows) Record(ctx context.Context, fl processflow.Flow) (bool, error) {
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
	return f.ProcessFlowRepo.Record(ctx, fl)
}

func (f *flakyFlows) FindActive(ctx context.Context, id string) (*processflow.Flow, error) {
	return f.ProcessFlowRepo.FindActive(ctx, id)
}

func (f *flakyFlows) ListActivePage(ctx context.Context, after *pagecursor.Cursor, limit int) ([]processflow.Flow, error) {
	return f.ProcessFlowRepo.ListActivePage(ctx, after, limit)
}

func (f *flakyFlows) recordCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records
}

type countingFlowMetrics struct {
	mu   sync.Mutex
	seen map[string]int
}

func (m *countingFlowMetrics) ProcessFlowMessage(_ context.Context, outcome string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = map[string]int{}
	}
	m.seen[outcome]++
}

func (m *countingFlowMetrics) count(outcome string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.seen[outcome]
}

type flowHarness struct {
	consumer  *kafkaconsumer.ProcessFlowConsumer
	repo      *memory.ProcessFlowRepo
	flaky     *flakyFlows
	processed *memory.ProcessedEventRepo
	uow       *countingUoW
	metrics   *countingFlowMetrics
}

func newFlowHarness() flowHarness {
	repo, processed := memory.NewProcessFlowRepo(), memory.NewProcessedEventRepo()
	flaky := &flakyFlows{ProcessFlowRepo: repo}
	uow := &countingUoW{inner: memory.NewUnitOfWork(repo, processed)}
	metrics := &countingFlowMetrics{}
	return flowHarness{
		consumer: &kafkaconsumer.ProcessFlowConsumer{
			Record:  &usecases.RecordProcessFlow{UoW: uow, ProcessedEvents: processed, Flows: flaky},
			Logger:  testLogger(),
			Retry:   fastRetry,
			Metrics: metrics,
		},
		repo: repo, flaky: flaky, processed: processed, uow: uow, metrics: metrics,
	}
}

func (h flowHarness) handle(t *testing.T, msgs ...[]byte) {
	t.Helper()
	for _, m := range msgs {
		if err := h.consumer.HandleMessage(context.Background(), m); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
}

func (h flowHarness) version(id string) int64 {
	f, _ := h.repo.Get(id)
	return f.Version()
}

func TestProcessFlowConsumer_AppliesEachTypeWithTheFullState(t *testing.T) {
	h := newFlowHarness()
	h.handle(t,
		flowWire(t, "e1", typeFlowDefined, flowAt, flowData("f1", 1, "ACTIVE", "PICK", "PACK")),
		flowWire(t, "e2", typeFlowRevised, flowAt, flowData("f1", 2, "ACTIVE", "PICK", "REBIN", "PACK")),
	)
	f, ok := h.repo.Get("f1")
	if !ok || f.Version() != 2 || len(f.Steps()) != 3 || f.Steps()[1] != processflow.Rebin || f.Name() != "Flow f1" || !f.UpdatedAt().Equal(flowAt) {
		t.Fatalf("f1 = %+v %v, want the revised v2 with PICK REBIN PACK", f, ok)
	}
	h.handle(t, flowWire(t, "e3", typeFlowRetired, flowAt, flowData("f1", 3, "RETIRED", "PICK", "REBIN", "PACK")))
	if f, _ := h.repo.Get("f1"); f.Active() || f.Version() != 3 {
		t.Fatalf("f1 = %+v, want RETIRED v3", f)
	}
	if !h.processed.Has(processFlowName, "e3") {
		t.Fatal("the event id was not claimed under process-flow-consumer")
	}
	if h.metrics.count("applied") != 3 {
		t.Fatalf("applied counter = %d, want 3", h.metrics.count("applied"))
	}
}

// The version guard: equal or lower versions, in any order, change nothing but are claimed.
func TestProcessFlowConsumer_OnlyAGreaterVersionIsApplied(t *testing.T) {
	h := newFlowHarness()
	h.handle(t,
		flowWire(t, "e2", typeFlowRevised, flowAt, flowData("f1", 2, "ACTIVE", "PICK", "REBIN", "PACK")),
		flowWire(t, "e1", typeFlowDefined, flowAt, flowData("f1", 1, "ACTIVE", "PICK")),          // late, lower
		flowWire(t, "e5", typeFlowRevised, flowAt, flowData("f1", 2, "ACTIVE", "PICK")),          // startup republish, equal
		flowWire(t, "e6", typeFlowRetired, flowAt, flowData("g1", 4, "RETIRED", "PICK", "PACK")), // first message is a retirement
	)
	if f, _ := h.repo.Get("f1"); f.Version() != 2 || len(f.Steps()) != 3 {
		t.Fatalf("f1 = %+v, want the v2 state untouched by the lower and equal versions", f)
	}
	if g, ok := h.repo.Get("g1"); !ok || g.Active() || g.Version() != 4 {
		t.Fatalf("g1 = %+v %v, want a RETIRED v4 row from a first Retired message", g, ok)
	}
	if h.metrics.count("applied") != 2 || h.metrics.count("stale") != 2 {
		t.Fatalf("counters applied=%d stale=%d, want 2 and 2", h.metrics.count("applied"), h.metrics.count("stale"))
	}
	if !h.processed.Has(processFlowName, "e5") {
		t.Fatal("a stale message must still be claimed")
	}
}

func TestProcessFlowConsumer_SameEventIDIsADuplicate(t *testing.T) {
	h := newFlowHarness()
	h.handle(t,
		flowWire(t, "e1", typeFlowDefined, flowAt, flowData("f1", 1, "ACTIVE", "PICK")),
		flowWire(t, "e1", typeFlowRevised, flowAt, flowData("f1", 9, "ACTIVE", "PICK", "PACK")),
	)
	if h.version("f1") != 1 || h.flaky.recordCalls() != 1 || h.metrics.count("duplicate") != 1 {
		t.Fatalf("version %d, %d Record calls, %d duplicates; want 1, 1, 1 (the replayed event id never reaches Record)", h.version("f1"), h.flaky.recordCalls(), h.metrics.count("duplicate"))
	}
}

// Only the three flow types are consumed, matched on the FULL type string.
func TestProcessFlowConsumer_DispatchesOnTheFullTypeAndIgnoresTheRest(t *testing.T) {
	h := newFlowHarness()
	for name, typ := range map[string]string{
		"ProcessPathCreated (same topic)": typePathCreated,
		"CPTScheduleChanged (same topic)": typeCPTChanged,
		"right suffix, wrong owner":       "com.warehouse.wes.someone-else.processflow.ProcessFlowDefined",
		"short name only":                 "ProcessFlowDefined",
		"versioned suffix":                typeFlowDefined + ".v2",
	} {
		if err := h.consumer.HandleMessage(context.Background(), flowWire(t, "evt-"+name, typ, flowAt, flowData("f1", 1, "ACTIVE", "PICK"))); err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
	if h.repo.Len() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("an ignored type wrote %d flows / opened %d units of work", h.repo.Len(), h.uow.calls.Load())
	}
}

// An unknown process_type, a missing field or anything else a retry cannot fix is
// poison: dead-lettered, never retried, never applied, validated before the
// transaction opens, and counted as rejected.
func TestProcessFlowConsumer_UnusableMessagesArePoisonAndNeverTouchTheDatabase(t *testing.T) {
	h := newFlowHarness()
	with := func(mutate func(map[string]any)) map[string]any {
		d := flowData("f1", 1, "ACTIVE", "PICK", "PACK")
		mutate(d)
		return d
	}
	w := func(name, typ string, at time.Time, data map[string]any) []byte {
		return flowWire(t, "e-"+name, typ, at, data)
	}
	cases := map[string][]byte{
		"not json":                []byte("this is not json"),
		"empty":                   nil,
		"json but not an event":   []byte(`{"hello":"world"}`),
		"no flow_id":              w("a", typeFlowDefined, flowAt, with(func(d map[string]any) { delete(d, "flow_id") })),
		"no stages":               w("b", typeFlowDefined, flowAt, with(func(d map[string]any) { delete(d, "stages") })),
		"empty stages":            w("c", typeFlowRevised, flowAt, with(func(d map[string]any) { d["stages"] = []any{} })),
		"unknown process_type":    w("d", typeFlowDefined, flowAt, with(func(d map[string]any) { d["stages"] = []map[string]any{{"path_id": "X", "process_type": "TELEPORT"}} })),
		"lower-case process_type": w("e", typeFlowDefined, flowAt, with(func(d map[string]any) { d["stages"] = []map[string]any{{"path_id": "X", "process_type": "pick"}} })),
		"stage without path_id":   w("f", typeFlowRevised, flowAt, with(func(d map[string]any) { d["stages"] = []map[string]any{{"process_type": "PICK"}} })),
		"unknown status":          w("g", typeFlowRevised, flowAt, with(func(d map[string]any) { d["status"] = "PAUSED" })),
		"no status":               w("h", typeFlowRetired, flowAt, with(func(d map[string]any) { delete(d, "status") })),
		"version zero":            w("i", typeFlowDefined, flowAt, with(func(d map[string]any) { d["version"] = 0 })),
		"no version":              w("j", typeFlowDefined, flowAt, with(func(d map[string]any) { delete(d, "version") })),
		"version is a string":     w("k", typeFlowDefined, flowAt, with(func(d map[string]any) { d["version"] = "2" })),
		"fractional version":      w("l", typeFlowDefined, flowAt, with(func(d map[string]any) { d["version"] = 1.5 })),
		"no event time":           w("m", typeFlowDefined, time.Time{}, flowData("f1", 1, "ACTIVE", "PICK")),
		"data is not an object":   w("n", typeFlowDefined, flowAt, map[string]any{"flow_id": []string{"x"}}),
		"retired: no stages":      w("o", typeFlowRetired, flowAt, with(func(d map[string]any) { delete(d, "stages") })),
		"revised: unknown type":   w("p", typeFlowRevised, flowAt, with(func(d map[string]any) { d["stages"] = []map[string]any{{"path_id": "X", "process_type": "BOGUS"}} })),
		"stages is not an array":  w("q", typeFlowDefined, flowAt, with(func(d map[string]any) { d["stages"] = "PICK,PACK" })),
	}
	for name, msg := range cases {
		err := h.consumer.HandleMessage(context.Background(), msg)
		if err == nil || !kafkaconsumer.IsPoisonForTest(err) {
			t.Errorf("%s: err = %v, want a poison error (dead-letter, never retry)", name, err)
		}
	}
	if h.repo.Len() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("poison touched the copy: %d flows, %d units of work (validation must run before the transaction)", h.repo.Len(), h.uow.calls.Load())
	}
	if got := h.metrics.count("rejected"); got != len(cases) {
		t.Fatalf("rejected counter = %d, want %d", got, len(cases))
	}
}

func TestProcessFlowConsumer_TransientFailureLeavesNothingAndRetrySucceeds(t *testing.T) {
	h := newFlowHarness()
	ctx := context.Background()
	msg := flowWire(t, "e1", typeFlowDefined, flowAt, flowData("f1", 1, "ACTIVE", "PICK"))

	h.flaky.failures = 1
	err := h.consumer.HandleMessage(ctx, msg)
	if !errors.Is(err, errInjected) || kafkaconsumer.IsPoisonForTest(err) {
		t.Fatalf("err = %v, want the injected infrastructure error (so the loop retries, not dead-letters)", err)
	}
	if h.repo.Len() != 0 || h.processed.Has(processFlowName, "e1") {
		t.Fatal("a failed handling left a flow or a claim behind")
	}
	if err := h.consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if _, ok := h.repo.Get("f1"); !ok || !h.processed.Has(processFlowName, "e1") {
		t.Fatal("the retried message never landed")
	}
}

func TestProcessFlowConsumer_RunCommitsAfterSuccessAndDeadLettersPoisonWithoutRetry(t *testing.T) {
	h := newFlowHarness()
	dlq := &fakeDLQWriter{}
	h.consumer.DLQ, h.consumer.DLQTopic = dlq, "warehouse.process-path-management.events.dlq"
	poison := flowWire(t, "e-bad", typeFlowDefined, flowAt, flowData("f-bad", 1, "ACTIVE", "TELEPORT"))
	reader := newFakeReader(
		flowWire(t, "e1", typeFlowDefined, flowAt, flowData("f1", 1, "ACTIVE", "PICK")),
		poison,
		flowWire(t, "e2", typeFlowRevised, flowAt, flowData("f2", 2, "ACTIVE", "PICK", "PACK")),
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
	if len(published) != 1 || string(published[0].Value) != string(poison) || published[0].Topic != "warehouse.process-path-management.events.dlq" {
		t.Fatalf("DLQ = %+v, want exactly the unknown-process_type message on the .dlq topic", published)
	}
	if h.repo.Len() != 2 {
		t.Fatalf("copy holds %d flows: the messages around the poison one must still be applied", h.repo.Len())
	}
}

func TestNewProcessFlowConsumer_ReadsTheIntegrationTopicWithItsDLQ(t *testing.T) {
	c := kafkaconsumer.NewProcessFlowConsumer([]string{"127.0.0.1:1"}, "g-process-flows", &usecases.RecordProcessFlow{}, nil, nil)
	defer func() { _ = c.Close() }()
	if kafkaconsumer.ProcessPathManagementTopic != "warehouse.process-path-management.events" {
		t.Fatalf("topic = %s, want process-path-management's INTEGRATION topic (never its analytics topic)", kafkaconsumer.ProcessPathManagementTopic)
	}
	if c.DLQTopic != "warehouse.process-path-management.events.dlq" || c.DLQ == nil || c.Logger == nil {
		t.Fatalf("consumer = %+v, want the .dlq writer and a logger", c)
	}
}

// A consumer with no Metrics port is a no-op counter, not a panic.
func TestProcessFlowConsumer_NilMetricsIsANoOp(t *testing.T) {
	h := newFlowHarness()
	h.consumer.Metrics = nil
	h.handle(t, flowWire(t, "e1", typeFlowDefined, flowAt, flowData("f1", 1, "ACTIVE", "PICK")))
	var _ ports.ProcessFlowMetrics = (*countingFlowMetrics)(nil)
	if h.repo.Len() != 1 {
		t.Fatal("the message was not applied")
	}
}
