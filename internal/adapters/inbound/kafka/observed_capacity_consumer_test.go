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
	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

const (
	typeTaskCompleted = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
	typeTaskClaimed   = "com.warehouse.wes.fulfillment-execution.task.TaskClaimed"
	observedSite      = "SIM1"
	observedKey       = "observed-capacity-consumer"
)

var (
	doneAt  = time.Date(2026, 9, 30, 15, 0, 20, 0, time.UTC)
	obsFrom = time.Date(2026, 9, 30, 14, 0, 0, 0, time.UTC)
	obsTo   = time.Date(2026, 9, 30, 16, 0, 0, 0, time.UTC)
)

// taskWireEvent builds the exact CloudEvents 1.0 structured-mode bytes
// fulfillment-execution publishes on warehouse.fulfillment.events.
func taskWireEvent(t *testing.T, id, typ, subject string, at time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/fulfillment-execution")
	e.SetType(typ)
	e.SetSubject(subject)
	if !at.IsZero() {
		e.SetTime(at)
	}
	e.SetDataSchema("urn:warehouse:fulfillment-execution:events:TaskCompleted:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func completedData(taskID, taskType string, duration any) map[string]any {
	d := map[string]any{"task_id": taskID, "station_id": "st-1", "work_unit_id": "wu-1"}
	if taskType != "" {
		d["task_type"] = taskType
	}
	if duration != nil {
		d["duration_seconds"] = duration
	}
	return d
}

func completedWire(t *testing.T, eventID, taskID, taskType string, at time.Time, duration any) []byte {
	return taskWireEvent(t, eventID, typeTaskCompleted, taskID, at, completedData(taskID, taskType, duration))
}

type flakyObserved struct {
	*memory.ObservedCapacityRepo
	mu       sync.Mutex
	failures int
	records  int
}

func (f *flakyObserved) Record(ctx context.Context, c observed.Completion) error {
	f.mu.Lock()
	f.records++
	fail := f.failures > 0
	if fail {
		f.failures--
	}
	f.mu.Unlock()
	if fail {
		return errInjected
	}
	return f.ObservedCapacityRepo.Record(ctx, c)
}

type observedHarness struct {
	consumer  *kafkaconsumer.ObservedCapacityConsumer
	repo      *memory.ObservedCapacityRepo
	flaky     *flakyObserved
	processed *memory.ProcessedEventRepo
	uow       *countingUoW
}

func newObservedHarness() observedHarness {
	repo, processed := memory.NewObservedCapacityRepo(), memory.NewProcessedEventRepo()
	flaky := &flakyObserved{ObservedCapacityRepo: repo}
	uow := &countingUoW{inner: memory.NewUnitOfWork(repo, processed)}
	return observedHarness{
		consumer: &kafkaconsumer.ObservedCapacityConsumer{
			Record:   &usecases.RecordObservedCompletion{UoW: uow, ProcessedEvents: processed, Observed: flaky},
			Location: observedSite, Logger: testLogger(), Retry: fastRetry,
		},
		repo: repo, flaky: flaky, processed: processed, uow: uow,
	}
}

func (h observedHarness) throughput(t *testing.T, step string) observed.Throughput {
	t.Helper()
	th, err := h.repo.Throughput(context.Background(), observedSite, step, obsFrom, obsTo)
	if err != nil {
		t.Fatal(err)
	}
	return th
}

func TestObservedCapacityConsumer_TaskCompletedIsCountedPerStepAtTheConfiguredSite(t *testing.T) {
	h := newObservedHarness()
	ctx := context.Background()
	for _, m := range [][]byte{
		completedWire(t, "e1", "t1", "PICK", doneAt, 30),
		completedWire(t, "e2", "t2", "pick", doneAt.Add(time.Minute), 50), // lower case is the same step
		completedWire(t, "e3", "t3", "PACK", doneAt, nil),                 // no duration is still a task
	} {
		if err := h.consumer.HandleMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	pick := h.throughput(t, "PICK")
	if pick.Tasks != 2 || pick.DurationSeconds != 80 || pick.TasksWithDuration != 2 {
		t.Fatalf("PICK = %+v", pick)
	}
	pack := h.throughput(t, "PACK")
	if pack.Tasks != 1 || pack.TasksWithDuration != 0 {
		t.Fatalf("PACK = %+v", pack)
	}
	other, _ := h.repo.Throughput(ctx, "ELSEWHERE", "PICK", obsFrom, obsTo)
	if other.HasData() {
		t.Fatalf("the completions leaked to another site: %+v", other)
	}
}

func TestObservedCapacityConsumer_ReplayOfTheSameEventIDCountsOnce(t *testing.T) {
	h := newObservedHarness()
	ctx := context.Background()
	msg := completedWire(t, "e1", "t1", "PICK", doneAt, 30)
	for i := 0; i < 3; i++ { // the at-least-once redelivery
		if err := h.consumer.HandleMessage(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	if h.repo.Tasks() != 1 {
		t.Fatalf("tasks = %d, want 1 (exactly-once effect)", h.repo.Tasks())
	}
	if h.flaky.records != 1 {
		t.Fatalf("Record called %d times, want 1", h.flaky.records)
	}
}

func TestObservedCapacityConsumer_IgnoresEveryOtherType(t *testing.T) {
	h := newObservedHarness()
	for name, typ := range map[string]string{
		"TaskClaimed":               typeTaskClaimed,
		"right suffix, wrong owner": "com.warehouse.wes.someone-else.task.TaskCompleted",
		"short name only":           "TaskCompleted",
	} {
		if err := h.consumer.HandleMessage(context.Background(), taskWireEvent(t, "e-"+name, typ, "t1", doneAt, completedData("t1", "PICK", 1))); err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
	if h.repo.Tasks() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("an ignored type counted %d tasks / opened %d units of work", h.repo.Tasks(), h.uow.calls.Load())
	}
}

func TestObservedCapacityConsumer_UnusableMessagesArePoison(t *testing.T) {
	h := newObservedHarness()
	cases := map[string][]byte{
		"not json":                        []byte("nope"),
		"json but not an event":           []byte(`{"hello":"world"}`),
		"missing task_id":                 taskWireEvent(t, "e1", typeTaskCompleted, "t1", doneAt, completedData("", "PICK", 1)),
		"subject is not the task id":      taskWireEvent(t, "e2", typeTaskCompleted, "other", doneAt, completedData("t1", "PICK", 1)),
		"no task_type (cannot attribute)": completedWire(t, "e3", "t1", "", doneAt, 1),
		"negative duration":               completedWire(t, "e4", "t1", "PICK", doneAt, -5),
		"duration of the wrong type":      completedWire(t, "e5", "t1", "PICK", doneAt, "slow"),
		"no event time":                   completedWire(t, "e6", "t1", "PICK", time.Time{}, 1),
	}
	for name, msg := range cases {
		err := h.consumer.HandleMessage(context.Background(), msg)
		if err == nil || !kafkaconsumer.IsPoisonForTest(err) {
			t.Errorf("%s: err = %v, want a poison error (dead-letter, never retry)", name, err)
		}
	}
	if h.repo.Tasks() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("poison counted %d tasks / opened %d units of work", h.repo.Tasks(), h.uow.calls.Load())
	}
}

func TestObservedCapacityConsumer_TransientFailureLeavesNothingAndRetrySucceeds(t *testing.T) {
	h := newObservedHarness()
	ctx := context.Background()
	msg := completedWire(t, "e1", "t1", "PICK", doneAt, 30)
	h.flaky.failures = 1
	err := h.consumer.HandleMessage(ctx, msg)
	if !errors.Is(err, errInjected) || kafkaconsumer.IsPoisonForTest(err) {
		t.Fatalf("err = %v, want the injected infrastructure error", err)
	}
	if h.repo.Tasks() != 0 || h.processed.Has(observedKey, "e1") {
		t.Fatal("a failed handling left a count or a claim behind")
	}
	if err := h.consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if h.repo.Tasks() != 1 {
		t.Fatalf("tasks after the retry = %d, want 1", h.repo.Tasks())
	}
}

func TestObservedCapacityConsumer_RunDeadLettersPoisonAndKeepsGoing(t *testing.T) {
	h := newObservedHarness()
	dlq := &fakeDLQWriter{}
	h.consumer.DLQ, h.consumer.DLQTopic = dlq, "warehouse.fulfillment.events.dlq"
	bad := completedWire(t, "bad", "t0", "", doneAt, 1)
	reader := newFakeReader(bad, completedWire(t, "e1", "t1", "PICK", doneAt, 30))
	h.consumer.Reader = reader

	if err := runUntilCommits(t, reader, 2, h.consumer.Run); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	published := dlq.published()
	if len(published) != 1 || string(published[0].Value) != string(bad) || published[0].Topic != "warehouse.fulfillment.events.dlq" {
		t.Fatalf("DLQ = %+v, want exactly the task_type-less completion", published)
	}
	if h.throughput(t, "PICK").Tasks != 1 {
		t.Fatal("the message after the poison one never landed")
	}
}
