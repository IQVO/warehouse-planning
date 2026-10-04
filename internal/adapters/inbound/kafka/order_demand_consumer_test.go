package kafka_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	kafkago "github.com/segmentio/kafka-go"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

const (
	typeAllocated  = "com.warehouse.wes.order-management.order.OrderAllocated"
	typePartially  = "com.warehouse.wes.order-management.order.OrderPartiallyAllocated"
	demandSiteID   = "SIM1"
	orderIDOne     = "ord-7c9e6679-7d5a-4b37-b2f1-93b0c4a1d8f2"
	orderIDTwo     = "ord-1b2f1e0a-5f4f-4a67-9c1e-6e2f3a4b5c6d"
	promiseRFC3339 = "2026-10-05T10:00:00Z"
)

var (
	eventTime  = time.Date(2026, 10, 4, 9, 15, 30, 0, time.UTC)
	promiseAt  = time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)
	windowFrom = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	windowTo   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
)

// orderWireEvent builds the exact CloudEvents 1.0 structured-mode bytes
// order-management publishes on warehouse.order-management.events.
func orderWireEvent(t *testing.T, id, typ, subject string, at time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/order-management")
	e.SetType(typ)
	e.SetSubject(subject)
	if !at.IsZero() {
		e.SetTime(at)
	}
	e.SetDataSchema("urn:warehouse:order-management:events:OrderAllocated:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func allocationData(orderID, promise string, lines int) map[string]any {
	ls := make([]map[string]any, 0, lines)
	for i := 1; i <= lines; i++ {
		ls = append(ls, map[string]any{"line_no": i, "sku": "SKU-1", "path_id": "pick", "gift_wrap": false, "fulfillment_class": "SINGLE"})
	}
	return map[string]any{"order_id": orderID, "promise_date": promise, "lines": ls}
}

// flakyDemandRepo fails the next failUpserts Upsert calls AFTER nothing was
// written, simulating a transient database failure on the write that follows
// the claim.
type flakyDemandRepo struct {
	*memory.OrderDemandRepo
	mu          sync.Mutex
	failUpserts int
	upserts     int
}

func (f *flakyDemandRepo) Upsert(ctx context.Context, o demand.Order) (bool, error) {
	f.mu.Lock()
	f.upserts++
	fail := f.failUpserts > 0
	if fail {
		f.failUpserts--
	}
	f.mu.Unlock()
	if fail {
		return false, errInjected
	}
	return f.OrderDemandRepo.Upsert(ctx, o)
}

func (f *flakyDemandRepo) upsertCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.upserts
}

type orderHarness struct {
	consumer  *kafkaconsumer.OrderDemandConsumer
	repo      *memory.OrderDemandRepo
	flaky     *flakyDemandRepo
	processed *memory.ProcessedEventRepo
	uow       *countingUoW
}

func newOrderHarness() orderHarness {
	repo, processed := memory.NewOrderDemandRepo(), memory.NewProcessedEventRepo()
	flaky := &flakyDemandRepo{OrderDemandRepo: repo}
	uow := &countingUoW{inner: memory.NewUnitOfWork(repo, processed)}
	return orderHarness{
		consumer: &kafkaconsumer.OrderDemandConsumer{
			Record:   &usecases.RecordOrderDemand{UoW: uow, ProcessedEvents: processed, Demand: flaky},
			Location: demandSiteID,
			Logger:   testLogger(),
			Retry:    fastRetry,
		},
		repo: repo, flaky: flaky, processed: processed, uow: uow,
	}
}

func (h orderHarness) expected(t *testing.T) demand.Summary {
	t.Helper()
	s, err := h.repo.Expected(context.Background(), demandSiteID, windowFrom, windowTo)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestOrderDemandConsumer_OrderAllocatedUpdatesTheModel(t *testing.T) {
	h := newOrderHarness()
	msg := orderWireEvent(t, "evt-1", typeAllocated, orderIDOne, eventTime, allocationData(orderIDOne, promiseRFC3339, 2))

	if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	o, ok := h.repo.Get(orderIDOne)
	if !ok {
		t.Fatal("order not in the model")
	}
	if o.Location() != demandSiteID {
		t.Errorf("location = %q, want the CONFIGURED site %q (events carry none)", o.Location(), demandSiteID)
	}
	if !o.PromiseAt().Equal(promiseAt) || o.ReleasedLines() != 2 || !o.AsOf().Equal(eventTime) {
		t.Errorf("order = promise %v lines %d asOf %v", o.PromiseAt(), o.ReleasedLines(), o.AsOf())
	}
	if s := h.expected(t); s.Orders != 1 || s.ReleasedLines != 2 || !s.AsOf.Equal(eventTime) {
		t.Errorf("summary = %+v", s)
	}
}

func TestOrderDemandConsumer_OrderPartiallyAllocatedIsAppliedToo(t *testing.T) {
	h := newOrderHarness()
	msg := orderWireEvent(t, "evt-p", typePartially, orderIDTwo, eventTime, allocationData(orderIDTwo, promiseRFC3339, 1))
	if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if s := h.expected(t); s.Orders != 1 || s.ReleasedLines != 1 {
		t.Fatalf("summary = %+v", s)
	}
}

func TestOrderDemandConsumer_AnOrderWithNoReleasedLinesIsStillAnOrder(t *testing.T) {
	h := newOrderHarness()
	msg := orderWireEvent(t, "evt-0", typeAllocated, orderIDOne, eventTime, allocationData(orderIDOne, promiseRFC3339, 0))
	if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if s := h.expected(t); s.Orders != 1 || s.ReleasedLines != 0 {
		t.Fatalf("summary = %+v", s)
	}
}

func TestOrderDemandConsumer_ReplayOfTheSameEventIDIsANoOp(t *testing.T) {
	h := newOrderHarness()
	ctx := context.Background()
	first := orderWireEvent(t, "evt-1", typeAllocated, orderIDOne, eventTime, allocationData(orderIDOne, promiseRFC3339, 2))
	if err := h.consumer.HandleMessage(ctx, first); err != nil {
		t.Fatal(err)
	}
	// Same id, different payload and a LATER time: a naive consumer that does
	// not claim on the id would apply it.
	replay := orderWireEvent(t, "evt-1", typeAllocated, orderIDOne, eventTime.Add(time.Hour), allocationData(orderIDOne, "2026-10-06T10:00:00Z", 5))
	if err := h.consumer.HandleMessage(ctx, replay); err != nil {
		t.Fatal(err)
	}
	o, _ := h.repo.Get(orderIDOne)
	if !o.PromiseAt().Equal(promiseAt) || o.ReleasedLines() != 2 {
		t.Fatalf("a replayed event id changed the model: %+v", o)
	}
	if got := h.flaky.upsertCalls(); got != 1 {
		t.Fatalf("Upsert called %d times, want 1", got)
	}
}

func TestOrderDemandConsumer_DispatchesOnTheFullTypeAndIgnoresTheRest(t *testing.T) {
	h := newOrderHarness()
	ctx := context.Background()
	for name, typ := range map[string]string{
		"OrderRepromised (no new cutoff instant)": "com.warehouse.wes.order-management.order.OrderRepromised",
		"OrderCancelled (analytics topic only)":   "com.warehouse.wes.order-management.order.OrderCancelled",
		"OrderReleased":                           "com.warehouse.wes.order-management.order.OrderReleased",
		"right suffix, wrong owner":               "com.warehouse.wes.someone-else.order.OrderAllocated",
		"short name only":                         "OrderAllocated",
		"another context entirely":                "com.warehouse.wes.warehouse-planning.capacityplan.CapacityPlanPublished",
	} {
		msg := orderWireEvent(t, "evt-"+name, typ, orderIDOne, eventTime, allocationData(orderIDOne, promiseRFC3339, 1))
		if err := h.consumer.HandleMessage(ctx, msg); err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
	if h.repo.Len() != 0 {
		t.Fatalf("an ignored type wrote %d orders", h.repo.Len())
	}
	if calls := h.uow.calls.Load(); calls != 0 {
		t.Fatalf("an ignored type opened %d units of work", calls)
	}
}

func TestOrderDemandConsumer_GarbageAndLegacyFlatAreSkippedWithoutError(t *testing.T) {
	h := newOrderHarness()
	legacyFlat, _ := json.Marshal(map[string]any{
		"event_id": "e-1", "event_type": "OrderAllocated", "occurred_at": eventTime.Format(time.RFC3339),
		"payload": allocationData(orderIDOne, promiseRFC3339, 1),
	})
	for name, value := range map[string][]byte{
		"not json":              []byte("this is not json"),
		"empty":                 nil,
		"json but not an event": []byte(`{"hello":"world"}`),
		"legacy flat envelope":  legacyFlat,
	} {
		if err := h.consumer.HandleMessage(context.Background(), value); err != nil {
			t.Errorf("%s: err = %v, want nil (never block the partition)", name, err)
		}
	}
	if h.repo.Len() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("garbage touched the model: %d orders, %d units of work", h.repo.Len(), h.uow.calls.Load())
	}
}

func TestOrderDemandConsumer_InvalidPayloadsAreSkippedWithoutTouchingTheDatabase(t *testing.T) {
	h := newOrderHarness()
	cases := map[string][]byte{
		"missing order id": orderWireEvent(t, "e1", typeAllocated, orderIDOne, eventTime, allocationData("", promiseRFC3339, 1)),
		"subject is not the order id": orderWireEvent(t, "e2", typeAllocated, orderIDTwo, eventTime,
			allocationData(orderIDOne, promiseRFC3339, 1)),
		"missing promise_date": orderWireEvent(t, "e3", typeAllocated, orderIDOne, eventTime,
			map[string]any{"order_id": orderIDOne, "lines": []any{}}),
		"malformed promise_date": orderWireEvent(t, "e4", typeAllocated, orderIDOne, eventTime,
			allocationData(orderIDOne, "tomorrow-ish", 1)),
		"promise_date wrong type": orderWireEvent(t, "e5", typeAllocated, orderIDOne, eventTime,
			map[string]any{"order_id": orderIDOne, "promise_date": 12345, "lines": []any{}}),
		"no event time": orderWireEvent(t, "e6", typeAllocated, orderIDOne, time.Time{},
			allocationData(orderIDOne, promiseRFC3339, 1)),
	}
	for name, msg := range cases {
		if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
	if h.repo.Len() != 0 {
		t.Fatalf("invalid payloads wrote %d orders", h.repo.Len())
	}
	if calls := h.uow.calls.Load(); calls != 0 {
		t.Fatalf("invalid payloads opened %d units of work: validation must run before the transaction", calls)
	}
}

// A transient failure after the claim must leave nothing written AND the
// claim unrecorded, so the retry of the same message is processed.
func TestOrderDemandConsumer_TransientFailureLeavesNothingAndRetrySucceeds(t *testing.T) {
	h := newOrderHarness()
	ctx := context.Background()
	msg := orderWireEvent(t, "evt-1", typeAllocated, orderIDOne, eventTime, allocationData(orderIDOne, promiseRFC3339, 2))

	h.flaky.failUpserts = 1
	err := h.consumer.HandleMessage(ctx, msg)
	if !errors.Is(err, errInjected) {
		t.Fatalf("err = %v, want the injected infrastructure error (so the loop retries)", err)
	}
	if h.repo.Len() != 0 {
		t.Fatal("a failed handling left an order behind")
	}
	if h.processed.Has("order-demand-consumer", "evt-1") {
		t.Fatal("the claim survived the failed handling: the redelivery would be skipped as already processed")
	}

	if err := h.consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if s := h.expected(t); s.Orders != 1 {
		t.Fatalf("after the retry the model has %d orders, want 1", s.Orders)
	}
	if !h.processed.Has("order-demand-consumer", "evt-1") {
		t.Fatal("the successful retry did not record its claim")
	}
}

func TestOrderDemandConsumer_LastWriterWinsPerOrderOnEventTime(t *testing.T) {
	h := newOrderHarness()
	ctx := context.Background()
	moved := "2026-10-05T14:30:00Z"
	// The re-allocation (later time) arrives first; the older event arrives
	// afterwards under a new id (redelivery across a DLQ, a rebalance...).
	newer := orderWireEvent(t, "evt-new", typeAllocated, orderIDOne, eventTime.Add(time.Hour), allocationData(orderIDOne, moved, 3))
	older := orderWireEvent(t, "evt-old", typePartially, orderIDOne, eventTime, allocationData(orderIDOne, promiseRFC3339, 1))
	for _, m := range [][]byte{newer, older} {
		if err := h.consumer.HandleMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	o, _ := h.repo.Get(orderIDOne)
	if got, want := o.PromiseAt(), time.Date(2026, 10, 5, 14, 30, 0, 0, time.UTC); !got.Equal(want) || o.ReleasedLines() != 3 {
		t.Fatalf("order = promise %v lines %d; the older event must not overwrite the newer", got, o.ReleasedLines())
	}
	if h.repo.Len() != 1 {
		t.Fatalf("one order id must stay one row, got %d", h.repo.Len())
	}
}

func TestOrderDemandConsumer_AnOrderIsCountedOnceNotPerEvent(t *testing.T) {
	h := newOrderHarness()
	ctx := context.Background()
	for i, typ := range []string{typePartially, typeAllocated} {
		msg := orderWireEvent(t, "evt-"+typ, typ, orderIDOne, eventTime.Add(time.Duration(i)*time.Minute), allocationData(orderIDOne, promiseRFC3339, i+1))
		if err := h.consumer.HandleMessage(ctx, msg); err != nil {
			t.Fatal(err)
		}
	}
	// A second, distinct order.
	if err := h.consumer.HandleMessage(ctx, orderWireEvent(t, "evt-2", typeAllocated, orderIDTwo, eventTime, allocationData(orderIDTwo, promiseRFC3339, 4))); err != nil {
		t.Fatal(err)
	}
	if s := h.expected(t); s.Orders != 2 || s.ReleasedLines != 2+4 {
		t.Fatalf("summary = %+v, want 2 orders (the first counted once, with its latest 2 lines) and 6 lines", s)
	}
}

func TestOrderDemandConsumer_RunCommitsOnlyAfterSuccessAndRetriesTransientFailures(t *testing.T) {
	h := newOrderHarness()
	reader := newFakeReader(
		orderWireEvent(t, "evt-1", typeAllocated, orderIDOne, eventTime, allocationData(orderIDOne, promiseRFC3339, 2)),
		[]byte("garbage that must still be committed past"),
	)
	h.consumer.Reader = reader
	h.flaky.failUpserts = 2 // the first message fails twice, then succeeds

	ctx, cancel := context.WithCancel(context.Background())
	reader.commitFn = func(m kafkago.Message) {
		if m.Offset == 1 {
			cancel() // both messages handled and committed
		}
	}
	done := make(chan error, 1)
	go func() { done <- h.consumer.Run(ctx) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not finish")
	}

	want := []string{"fetch:0", "commit:0", "fetch:1", "commit:1"}
	if got := reader.log(); len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] || got[3] != want[3] {
		t.Fatalf("reader events = %v, want %v (commit only after success, once per message)", got, want)
	}
	if got := h.flaky.upsertCalls(); got != 3 {
		t.Fatalf("Upsert attempts = %d, want 3 (two failures + one success on the SAME message)", got)
	}
	if h.expected(t).Orders != 1 {
		t.Fatal("the retried message never landed")
	}
}
