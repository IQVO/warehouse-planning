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
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

const (
	typeBooked         = "com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentBooked"
	typeCancelled      = "com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentCancelled"
	typeCheckedIn      = "com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentCheckedIn"
	typeASNRegistered  = "com.warehouse.wms.inbound-receiving.asn.ASNRegistered"
	inboundSite        = "SIM1"
	apptID             = "appt-123e4567-e89b-12d3-a456-426614174000"
	inboundConsumerKey = "inbound-demand-consumer"
)

var (
	bookedAt   = time.Date(2026, 10, 8, 12, 5, 0, 0, time.UTC)
	doorStart  = time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)
	doorEnd    = time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)
	inbWinFrom = time.Date(2026, 10, 9, 6, 0, 0, 0, time.UTC)
	inbWinTo   = time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
)

// inboundWireEvent builds the exact CloudEvents 1.0 structured-mode bytes
// inbound-receiving publishes on warehouse.inbound-receiving.events.
func inboundWireEvent(t *testing.T, id, typ, subject string, at time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/inbound-receiving")
	e.SetType(typ)
	e.SetSubject(subject)
	if !at.IsZero() {
		e.SetTime(at)
	}
	e.SetDataSchema("urn:warehouse:inbound-receiving:events:DockAppointment:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func bookedData(id string, start, end time.Time, asns ...string) map[string]any {
	return map[string]any{
		"appointment_id": id, "door_code": "WH1-DOCK-IN-01", "carrier": "ACME Freight",
		"window_start": start.Format(time.RFC3339), "window_end": end.Format(time.RFC3339), "asn_numbers": asns,
	}
}

func bookedWire(t *testing.T, eventID string, at time.Time, asns ...string) []byte {
	return inboundWireEvent(t, eventID, typeBooked, apptID, at, bookedData(apptID, doorStart, doorEnd, asns...))
}

func cancelledWire(t *testing.T, eventID string, at time.Time) []byte {
	return inboundWireEvent(t, eventID, typeCancelled, apptID, at,
		map[string]any{"appointment_id": apptID, "door_code": "WH1-DOCK-IN-01", "reason": "Carrier delayed"})
}

type flakyAppointments struct {
	*memory.InboundAppointmentRepo
	mu       sync.Mutex
	failures int
	applies  int
}

func (f *flakyAppointments) Apply(ctx context.Context, a inbound.Appointment) (bool, error) {
	f.mu.Lock()
	f.applies++
	fail := f.failures > 0
	if fail {
		f.failures--
	}
	f.mu.Unlock()
	if fail {
		return false, errInjected
	}
	return f.InboundAppointmentRepo.Apply(ctx, a)
}

func (f *flakyAppointments) applyCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.applies
}

type inboundHarness struct {
	consumer  *kafkaconsumer.InboundDemandConsumer
	repo      *memory.InboundAppointmentRepo
	flaky     *flakyAppointments
	processed *memory.ProcessedEventRepo
	uow       *countingUoW
}

func newInboundHarness() inboundHarness {
	repo, processed := memory.NewInboundAppointmentRepo(), memory.NewProcessedEventRepo()
	flaky := &flakyAppointments{InboundAppointmentRepo: repo}
	uow := &countingUoW{inner: memory.NewUnitOfWork(repo, processed)}
	return inboundHarness{
		consumer: &kafkaconsumer.InboundDemandConsumer{
			Record:   &usecases.RecordInboundAppointment{UoW: uow, ProcessedEvents: processed, Appointments: flaky},
			Location: inboundSite, Logger: testLogger(), Retry: fastRetry,
		},
		repo: repo, flaky: flaky, processed: processed, uow: uow,
	}
}

func (h inboundHarness) demand(t *testing.T) inbound.Demand {
	t.Helper()
	d, err := h.repo.Demand(context.Background(), inboundSite, inbWinFrom, inbWinTo)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestInboundDemandConsumer_BookedUpdatesTheModelAtTheConfiguredSite(t *testing.T) {
	h := newInboundHarness()
	if err := h.consumer.HandleMessage(context.Background(), bookedWire(t, "evt-1", bookedAt, "ASN-1001", "ASN-1002")); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	a, ok := h.repo.Get(apptID)
	if !ok {
		t.Fatal("appointment not in the model")
	}
	if a.Location() != inboundSite {
		t.Errorf("location = %q, want the CONFIGURED site %q (the event carries none)", a.Location(), inboundSite)
	}
	if !a.WindowStart().Equal(doorStart) || !a.WindowEnd().Equal(doorEnd) || a.ASNCount() != 2 || !a.AsOf().Equal(bookedAt) || a.Status() != inbound.StatusBooked {
		t.Errorf("appointment = %+v", a)
	}
	if d := h.demand(t); d.Appointments != 1 || d.ASNs != 2 || !d.AsOf.Equal(bookedAt) {
		t.Errorf("demand = %+v", d)
	}
}

func TestInboundDemandConsumer_CancelRemovesTheDemand(t *testing.T) {
	h := newInboundHarness()
	ctx := context.Background()
	for _, m := range [][]byte{bookedWire(t, "evt-b", bookedAt, "ASN-1"), cancelledWire(t, "evt-c", bookedAt.Add(time.Hour))} {
		if err := h.consumer.HandleMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if d := h.demand(t); d.Appointments != 0 {
		t.Fatalf("a cancelled appointment still counts: %+v", d)
	}
	if a, _ := h.repo.Get(apptID); a.Status() != inbound.StatusCancelled {
		t.Fatalf("status = %s", a.Status())
	}
}

// The ordering guard: a Cancelled that arrives BEFORE its Booked (a DLQ
// replay, a rebalance across partitions) must still win.
func TestInboundDemandConsumer_CancelledBeforeBookedWins(t *testing.T) {
	h := newInboundHarness()
	ctx := context.Background()
	if err := h.consumer.HandleMessage(ctx, cancelledWire(t, "evt-c", bookedAt.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if a, ok := h.repo.Get(apptID); !ok || a.Status() != inbound.StatusCancelled || a.HasWindow() {
		t.Fatalf("a Cancelled with no booking must be stored as a tombstone: %+v", a)
	}
	if err := h.consumer.HandleMessage(ctx, bookedWire(t, "evt-b", bookedAt, "ASN-1")); err != nil {
		t.Fatal(err)
	}
	if d := h.demand(t); d.Appointments != 0 {
		t.Fatalf("a Booked that arrived after its Cancelled resurrected the demand: %+v", d)
	}
	if a, _ := h.repo.Get(apptID); a.Status() != inbound.StatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", a.Status())
	}
	if !h.processed.Has(inboundConsumerKey, "evt-b") {
		t.Fatal("the stale Booked must still be claimed (it was handled, it changed nothing)")
	}
}

func TestInboundDemandConsumer_ReplayOfTheSameEventIDIsANoOp(t *testing.T) {
	h := newInboundHarness()
	ctx := context.Background()
	if err := h.consumer.HandleMessage(ctx, bookedWire(t, "evt-1", bookedAt, "ASN-1")); err != nil {
		t.Fatal(err)
	}
	// Same id, different payload and a LATER time: a consumer that does not
	// claim on the id would apply it.
	if err := h.consumer.HandleMessage(ctx, bookedWire(t, "evt-1", bookedAt.Add(time.Hour), "ASN-1", "ASN-2", "ASN-3")); err != nil {
		t.Fatal(err)
	}
	if a, _ := h.repo.Get(apptID); a.ASNCount() != 1 {
		t.Fatalf("a replayed event id changed the model: %+v", a)
	}
	if got := h.flaky.applyCalls(); got != 1 {
		t.Fatalf("Apply called %d times, want 1", got)
	}
}

func TestInboundDemandConsumer_RedeliveredCancelIsIdempotent(t *testing.T) {
	h := newInboundHarness()
	ctx := context.Background()
	msgs := [][]byte{
		bookedWire(t, "evt-b", bookedAt, "ASN-1"),
		cancelledWire(t, "evt-c", bookedAt.Add(time.Hour)),
		cancelledWire(t, "evt-c", bookedAt.Add(time.Hour)),  // same id
		cancelledWire(t, "evt-c2", bookedAt.Add(time.Hour)), // same time, a new id
	}
	for _, m := range msgs {
		if err := h.consumer.HandleMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	if h.repo.Len() != 1 || h.demand(t).Appointments != 0 {
		t.Fatalf("len %d demand %+v", h.repo.Len(), h.demand(t))
	}
}

func TestInboundDemandConsumer_DispatchesOnTheFullTypeAndIgnoresTheRest(t *testing.T) {
	h := newInboundHarness()
	ctx := context.Background()
	for name, typ := range map[string]string{
		"DockAppointmentCheckedIn":  typeCheckedIn,
		"ASNRegistered":             typeASNRegistered,
		"right suffix, wrong owner": "com.warehouse.wms.someone-else.dockappointment.DockAppointmentBooked",
		"short name only":           "DockAppointmentBooked",
	} {
		msg := inboundWireEvent(t, "evt-"+name, typ, apptID, bookedAt, bookedData(apptID, doorStart, doorEnd, "ASN-1"))
		if err := h.consumer.HandleMessage(ctx, msg); err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
	if h.repo.Len() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("an ignored type wrote %d appointments / opened %d units of work", h.repo.Len(), h.uow.calls.Load())
	}
}

func TestInboundDemandConsumer_UnusableMessagesArePoisonAndNeverTouchTheDatabase(t *testing.T) {
	h := newInboundHarness()
	legacyFlat, _ := json.Marshal(map[string]any{"event_id": "e-1", "event_type": "DockAppointmentBooked", "payload": bookedData(apptID, doorStart, doorEnd, "ASN-1")})
	cases := map[string][]byte{
		"not json":                           []byte("this is not json"),
		"empty":                              nil,
		"json but not an event":              []byte(`{"hello":"world"}`),
		"legacy flat envelope":               legacyFlat,
		"booked: missing id":                 inboundWireEvent(t, "e1", typeBooked, apptID, bookedAt, bookedData("", doorStart, doorEnd, "ASN-1")),
		"booked: subject is not the id":      inboundWireEvent(t, "e2", typeBooked, "other", bookedAt, bookedData(apptID, doorStart, doorEnd, "ASN-1")),
		"booked: window end not after start": inboundWireEvent(t, "e3", typeBooked, apptID, bookedAt, bookedData(apptID, doorStart, doorStart, "ASN-1")),
		"booked: no ASN":                     inboundWireEvent(t, "e4", typeBooked, apptID, bookedAt, bookedData(apptID, doorStart, doorEnd)),
		"booked: malformed window_start": inboundWireEvent(t, "e5", typeBooked, apptID, bookedAt,
			map[string]any{"appointment_id": apptID, "window_start": "tomorrow-ish", "window_end": doorEnd.Format(time.RFC3339), "asn_numbers": []string{"A"}}),
		"booked: no event time":            inboundWireEvent(t, "e6", typeBooked, apptID, time.Time{}, bookedData(apptID, doorStart, doorEnd, "ASN-1")),
		"cancelled: missing id":            inboundWireEvent(t, "e7", typeCancelled, apptID, bookedAt, map[string]any{"door_code": "D"}),
		"cancelled: subject is not the id": inboundWireEvent(t, "e8", typeCancelled, "other", bookedAt, map[string]any{"appointment_id": apptID}),
		"cancelled: no event time":         inboundWireEvent(t, "e9", typeCancelled, apptID, time.Time{}, map[string]any{"appointment_id": apptID}),
	}
	for name, msg := range cases {
		err := h.consumer.HandleMessage(context.Background(), msg)
		if err == nil || !kafkaconsumer.IsPoisonForTest(err) {
			t.Errorf("%s: err = %v, want a poison error (dead-letter, never retry)", name, err)
		}
	}
	if h.repo.Len() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("poison touched the model: %d appointments, %d units of work (validation must run before the transaction)", h.repo.Len(), h.uow.calls.Load())
	}
}

func TestInboundDemandConsumer_TransientFailureLeavesNothingAndRetrySucceeds(t *testing.T) {
	h := newInboundHarness()
	ctx := context.Background()
	msg := bookedWire(t, "evt-1", bookedAt, "ASN-1")

	h.flaky.failures = 1
	err := h.consumer.HandleMessage(ctx, msg)
	if !errors.Is(err, errInjected) || kafkaconsumer.IsPoisonForTest(err) {
		t.Fatalf("err = %v, want the injected infrastructure error (so the loop retries, not dead-letters)", err)
	}
	if h.repo.Len() != 0 || h.processed.Has(inboundConsumerKey, "evt-1") {
		t.Fatal("a failed handling left a row or a claim behind")
	}
	if err := h.consumer.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if h.demand(t).Appointments != 1 || !h.processed.Has(inboundConsumerKey, "evt-1") {
		t.Fatal("the retried message never landed")
	}
}

func TestInboundDemandConsumer_RunCommitsAfterSuccessAndDeadLettersPoisonWithoutRetry(t *testing.T) {
	h := newInboundHarness()
	dlq := &fakeDLQWriter{}
	h.consumer.DLQ, h.consumer.DLQTopic = dlq, "warehouse.inbound-receiving.events.dlq"
	garbage := []byte("garbage that must be dead-lettered, not retried")
	reader := newFakeReader(bookedWire(t, "evt-1", bookedAt, "ASN-1"), garbage, cancelledWire(t, "evt-2", bookedAt.Add(time.Hour)))
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
	if len(published) != 1 || string(published[0].Value) != string(garbage) || published[0].Topic != "warehouse.inbound-receiving.events.dlq" {
		t.Fatalf("DLQ = %+v, want exactly the garbage message on the .dlq topic", published)
	}
	hdrs := map[string]bool{}
	for _, hd := range published[0].Headers {
		hdrs[hd.Key] = true
	}
	for _, k := range []string{"x-dlq-source-topic", "x-dlq-error", "x-dlq-failed-at"} {
		if !hdrs[k] {
			t.Errorf("DLQ message missing header %q", k)
		}
	}
	if a, _ := h.repo.Get(apptID); a.Status() != inbound.StatusCancelled {
		t.Fatalf("status = %s: the messages around the poison one must still be applied in order", a.Status())
	}
	if got := h.flaky.applyCalls(); got != 3 {
		t.Fatalf("Apply attempts = %d, want 3 (one failure + one success on the first message, one on the third); the poison one never reaches it", got)
	}
}

func TestInboundDemandConsumer_RunWithoutADLQSkipsPoisonInsteadOfRetryingForever(t *testing.T) {
	h := newInboundHarness()
	reader := newFakeReader([]byte("garbage"), bookedWire(t, "evt-1", bookedAt, "ASN-1"))
	h.consumer.Reader = reader
	if err := runUntilCommits(t, reader, 2, h.consumer.Run); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if h.demand(t).Appointments != 1 {
		t.Fatal("the message after the poison one never landed")
	}
}
