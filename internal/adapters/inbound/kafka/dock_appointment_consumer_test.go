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
	typeBooked      = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentBooked"
	typeRescheduled = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentRescheduled"
	typeCancelled   = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentCancelled"
	typeNoShow      = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentNoShow"
	typeCheckedIn   = "com.warehouse.wms.dock-scheduling.dockappointment.DockAppointmentCheckedIn"
	typeClosure     = "com.warehouse.wms.dock-scheduling.dockcalendar.DoorClosureScheduled"
	// The retired source: inbound-receiving's types must never be consumed.
	typeLegacyBooked    = "com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentBooked"
	typeLegacyCancelled = "com.warehouse.wms.inbound-receiving.dockappointment.DockAppointmentCancelled"
	typeASNRegistered   = "com.warehouse.wms.inbound-receiving.asn.ASNRegistered"
	inboundSite         = "SIM1"
	apptID              = "appt-123e4567-e89b-12d3-a456-426614174000"
	inboundConsumerKey  = "dock-appointment-consumer"
)

var (
	bookedAt   = time.Date(2026, 10, 8, 12, 5, 0, 0, time.UTC)
	doorStart  = time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)
	doorEnd    = time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)
	movedStart = time.Date(2026, 10, 9, 20, 0, 0, 0, time.UTC)
	movedEnd   = time.Date(2026, 10, 9, 22, 0, 0, 0, time.UTC)
	inbWinFrom = time.Date(2026, 10, 9, 6, 0, 0, 0, time.UTC)
	inbWinTo   = time.Date(2026, 10, 9, 14, 0, 0, 0, time.UTC)
)

// dockWireEvent builds the exact CloudEvents 1.0 structured-mode bytes
// dock-scheduling publishes on warehouse.dock-scheduling.events (contract:
// type com.warehouse.wms.dock-scheduling.<entity>.<Event>, source
// /warehouse/dock-scheduling, subject = appointment id, snake_case data).
func dockWireEvent(t *testing.T, id, typ, subject string, at time.Time, data map[string]any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource("/warehouse/dock-scheduling")
	e.SetType(typ)
	e.SetSubject(subject)
	if !at.IsZero() {
		e.SetTime(at)
	}
	e.SetDataSchema("urn:warehouse:dock-scheduling:events:DockAppointment:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatalf("SetData: %v", err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// bookedData is a full DockAppointmentBooked `data` (the superset contract).
func bookedData(id, direction string, start, end time.Time, version int64, asns ...string) map[string]any {
	if asns == nil {
		asns = []string{}
	}
	return map[string]any{
		"appointment_id": id, "site_id": inboundSite, "direction": direction, "door_code": "WH1-DOCK-IN-01",
		"carrier": "Example Freight", "carrier_scac": nil,
		"window_start": start.Format(time.RFC3339), "window_end": end.Format(time.RFC3339),
		"asn_numbers": asns, "cpt_id": nil, "shipment_refs": []string{}, "expected_seal_number": nil,
		"provenance": "native", "version": version,
	}
}

func bookedWire(t *testing.T, eventID string, at time.Time, version int64, asns ...string) []byte {
	return dockWireEvent(t, eventID, typeBooked, apptID, at, bookedData(apptID, "INBOUND", doorStart, doorEnd, version, asns...))
}

func rescheduledWire(t *testing.T, eventID string, at time.Time, version int64) []byte {
	return dockWireEvent(t, eventID, typeRescheduled, apptID, at, map[string]any{
		"appointment_id": apptID, "site_id": inboundSite, "door_code": "WH1-DOCK-IN-02",
		"window_start": movedStart.Format(time.RFC3339), "window_end": movedEnd.Format(time.RFC3339),
		"previous_window_start": doorStart.Format(time.RFC3339), "previous_window_end": doorEnd.Format(time.RFC3339), "version": version,
	})
}

func cancelledWire(t *testing.T, eventID string, at time.Time, version int64) []byte {
	return dockWireEvent(t, eventID, typeCancelled, apptID, at, map[string]any{
		"appointment_id": apptID, "site_id": inboundSite, "door_code": "WH1-DOCK-IN-01", "reason": "CARRIER_REQUEST", "version": version,
	})
}

func noShowWire(t *testing.T, eventID string, at time.Time, version int64) []byte {
	return dockWireEvent(t, eventID, typeNoShow, apptID, at, map[string]any{
		"appointment_id": apptID, "site_id": inboundSite, "door_code": "WH1-DOCK-IN-01", "direction": "INBOUND",
		"carrier_scac": "EXFT", "window_end": doorEnd.Format(time.RFC3339), "version": version,
	})
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
	consumer  *kafkaconsumer.DockAppointmentConsumer
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
		consumer: &kafkaconsumer.DockAppointmentConsumer{
			Record: &usecases.RecordInboundAppointment{UoW: uow, ProcessedEvents: processed, Appointments: flaky},
			Logger: testLogger(), Retry: fastRetry,
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

func (h inboundHarness) handle(t *testing.T, msgs ...[]byte) {
	t.Helper()
	for _, m := range msgs {
		if err := h.consumer.HandleMessage(context.Background(), m); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
}

func TestDockAppointmentConsumer_BookedAttributesTheDemandToTheEventsSite(t *testing.T) {
	h := newInboundHarness()
	h.handle(t, bookedWire(t, "evt-1", bookedAt, 1, "ASN-1001", "ASN-1002"))
	a, ok := h.repo.Get(apptID)
	if !ok {
		t.Fatal("appointment not in the model")
	}
	if a.Location() != inboundSite {
		t.Errorf("location = %q, want the event's site_id %q", a.Location(), inboundSite)
	}
	if !a.WindowStart().Equal(doorStart) || !a.WindowEnd().Equal(doorEnd) || a.ASNCount() != 2 || !a.AsOf().Equal(bookedAt) ||
		a.Status() != inbound.StatusBooked || a.Direction() != inbound.DirectionInbound || a.Version() != 1 {
		t.Errorf("appointment = %+v", a)
	}
	if d := h.demand(t); d.Appointments != 1 || d.ASNs != 2 || !d.AsOf.Equal(bookedAt) {
		t.Errorf("demand = %+v", d)
	}
}

// The site comes from the event, not from configuration: two sites keep their
// own demand with no configured site at all.
func TestDockAppointmentConsumer_EachEventCountsAtItsOwnSite(t *testing.T) {
	h := newInboundHarness()
	other := "appt-other"
	data := bookedData(other, "INBOUND", doorStart, doorEnd, 1, "ASN-9")
	data["site_id"] = "SIM2"
	h.handle(t, bookedWire(t, "evt-1", bookedAt, 1, "ASN-1"), dockWireEvent(t, "evt-2", typeBooked, other, bookedAt, data))
	if d := h.demand(t); d.Appointments != 1 || d.ASNs != 1 {
		t.Fatalf("SIM1 demand = %+v, want only its own appointment", d)
	}
	d2, err := h.repo.Demand(context.Background(), "SIM2", inbWinFrom, inbWinTo)
	if err != nil || d2.Appointments != 1 || d2.ASNs != 1 {
		t.Fatalf("SIM2 demand = %+v, %v", d2, err)
	}
}

func TestDockAppointmentConsumer_OutboundAppointmentsAreNotDemand(t *testing.T) {
	h := newInboundHarness()
	h.handle(t, dockWireEvent(t, "evt-1", typeBooked, apptID, bookedAt, bookedData(apptID, "OUTBOUND", doorStart, doorEnd, 1)))
	if a, ok := h.repo.Get(apptID); !ok || a.Direction() != inbound.DirectionOutbound {
		t.Fatalf("the outbound booking must be stored (so later events of the id are understood): %+v %v", a, ok)
	}
	if d := h.demand(t); d.Appointments != 0 || d.ASNs != 0 {
		t.Fatalf("an OUTBOUND appointment counted: %+v", d)
	}
}

func TestDockAppointmentConsumer_RescheduleMovesTheDemandWindow(t *testing.T) {
	h := newInboundHarness()
	h.handle(t, bookedWire(t, "evt-b", bookedAt, 1, "ASN-1"))
	if d := h.demand(t); d.Appointments != 1 {
		t.Fatalf("before the reschedule: %+v", d)
	}
	h.handle(t, rescheduledWire(t, "evt-r", bookedAt.Add(time.Hour), 2))
	if d := h.demand(t); d.Appointments != 0 {
		t.Fatalf("the demand stayed in the old window after the reschedule: %+v", d)
	}
	moved, err := h.repo.Demand(context.Background(), inboundSite, movedStart.Add(-time.Hour), movedEnd.Add(time.Hour))
	if err != nil || moved.Appointments != 1 || moved.ASNs != 1 {
		t.Fatalf("the demand did not follow the new window: %+v %v", moved, err)
	}
	if a, _ := h.repo.Get(apptID); a.Version() != 2 || !a.WindowStart().Equal(movedStart) || a.ASNCount() != 1 {
		t.Fatalf("appointment = %+v, want v2 at the new window keeping its ASNs", a)
	}
}

// Version guard: a Rescheduled that is OLDER than what is stored (a DLQ replay,
// a rebalance) must not move the window back.
func TestDockAppointmentConsumer_AnOlderRescheduleDoesNotMoveTheWindowBack(t *testing.T) {
	h := newInboundHarness()
	h.handle(t, bookedWire(t, "evt-b", bookedAt, 1, "ASN-1"),
		rescheduledWire(t, "evt-r3", bookedAt.Add(2*time.Hour), 3), // the newest
		dockWireEvent(t, "evt-r2", typeRescheduled, apptID, bookedAt.Add(time.Hour), map[string]any{
			"appointment_id": apptID, "site_id": inboundSite, "door_code": "D",
			"window_start": doorStart.Add(time.Hour).Format(time.RFC3339), "window_end": doorEnd.Add(time.Hour).Format(time.RFC3339),
			"previous_window_start": doorStart.Format(time.RFC3339), "previous_window_end": doorEnd.Format(time.RFC3339), "version": 2,
		}))
	if a, _ := h.repo.Get(apptID); a.Version() != 3 || !a.WindowStart().Equal(movedStart) {
		t.Fatalf("appointment = %+v, want v3 at %v", a, movedStart)
	}
}

func TestDockAppointmentConsumer_CancelAndNoShowRemoveTheDemand(t *testing.T) {
	for name, terminal := range map[string]func(*testing.T) []byte{
		"cancelled": func(t *testing.T) []byte { return cancelledWire(t, "evt-x", bookedAt.Add(time.Hour), 2) },
		"no-show":   func(t *testing.T) []byte { return noShowWire(t, "evt-x", bookedAt.Add(time.Hour), 2) },
	} {
		h := newInboundHarness()
		h.handle(t, bookedWire(t, "evt-b", bookedAt, 1, "ASN-1"), terminal(t))
		if d := h.demand(t); d.Appointments != 0 || d.ASNs != 0 {
			t.Errorf("%s: the appointment still counts: %+v", name, d)
		}
	}
	h := newInboundHarness()
	h.handle(t, bookedWire(t, "evt-b", bookedAt, 1, "ASN-1"), noShowWire(t, "evt-n", bookedAt.Add(time.Hour), 2))
	if a, _ := h.repo.Get(apptID); a.Status() != inbound.StatusNoShow {
		t.Fatalf("status = %s, want NO_SHOW", a.Status())
	}
}

// The ordering guard: events that arrive BEFORE the ones they supersede (a DLQ
// replay, a rebalance across partitions) must still win.
func TestDockAppointmentConsumer_TerminalBeforeBookedWins(t *testing.T) {
	for name, terminal := range map[string]func(*testing.T) []byte{
		"cancelled": func(t *testing.T) []byte { return cancelledWire(t, "evt-x", bookedAt.Add(time.Hour), 2) },
		"no-show":   func(t *testing.T) []byte { return noShowWire(t, "evt-x", bookedAt.Add(time.Hour), 2) },
	} {
		h := newInboundHarness()
		h.handle(t, terminal(t))
		if a, ok := h.repo.Get(apptID); !ok || a.HasWindow() {
			t.Fatalf("%s: a terminal event with no booking must be stored as a tombstone: %+v", name, a)
		}
		h.handle(t, bookedWire(t, "evt-b", bookedAt, 1, "ASN-1"))
		if d := h.demand(t); d.Appointments != 0 {
			t.Fatalf("%s: a Booked that arrived after its terminal event resurrected the demand: %+v", name, d)
		}
		if !h.processed.Has(inboundConsumerKey, "evt-b") {
			t.Fatalf("%s: the late Booked must still be claimed", name)
		}
	}
}

func TestDockAppointmentConsumer_RescheduleBeforeBookedKeepsTheNewWindowAndCompletesTheFacts(t *testing.T) {
	h := newInboundHarness()
	h.handle(t, rescheduledWire(t, "evt-r", bookedAt.Add(time.Hour), 2))
	if d, _ := h.repo.Demand(context.Background(), inboundSite, movedStart.Add(-time.Hour), movedEnd.Add(time.Hour)); d.Appointments != 0 {
		t.Fatalf("a partial row (direction unknown) counted: %+v", d)
	}
	h.handle(t, bookedWire(t, "evt-b", bookedAt, 1, "ASN-1", "ASN-2"))
	d, err := h.repo.Demand(context.Background(), inboundSite, movedStart.Add(-time.Hour), movedEnd.Add(time.Hour))
	if err != nil || d.Appointments != 1 || d.ASNs != 2 {
		t.Fatalf("after the Booked the appointment must count once, at the NEWER window: %+v %v", d, err)
	}
	if h.demand(t).Appointments != 0 {
		t.Fatal("the original (older) window must not count")
	}
}

func TestDockAppointmentConsumer_ReplayOfTheSameEventIDIsANoOp(t *testing.T) {
	h := newInboundHarness()
	// Same id, different payload and a HIGHER version: a consumer that does not
	// claim on the id would apply it.
	h.handle(t, bookedWire(t, "evt-1", bookedAt, 1, "ASN-1"), bookedWire(t, "evt-1", bookedAt.Add(time.Hour), 5, "ASN-1", "ASN-2", "ASN-3"))
	if a, _ := h.repo.Get(apptID); a.ASNCount() != 1 || a.Version() != 1 {
		t.Fatalf("a replayed event id changed the model: %+v", a)
	}
	if got := h.flaky.applyCalls(); got != 1 {
		t.Fatalf("Apply called %d times, want 1", got)
	}
}

func TestDockAppointmentConsumer_RedeliveredTerminalEventIsIdempotent(t *testing.T) {
	h := newInboundHarness()
	h.handle(t,
		bookedWire(t, "evt-b", bookedAt, 1, "ASN-1"),
		cancelledWire(t, "evt-c", bookedAt.Add(time.Hour), 2),
		cancelledWire(t, "evt-c", bookedAt.Add(time.Hour), 2),  // same id
		cancelledWire(t, "evt-c2", bookedAt.Add(time.Hour), 2), // same version, a new id
	)
	if h.repo.Len() != 1 || h.demand(t).Appointments != 0 {
		t.Fatalf("len %d demand %+v", h.repo.Len(), h.demand(t))
	}
}

// Only the four appointment types of dock-scheduling are consumed, matched on
// the FULL type string; everything else is ignored -- including the retired
// inbound-receiving appointment types.
func TestDockAppointmentConsumer_DispatchesOnTheFullTypeAndIgnoresTheRest(t *testing.T) {
	h := newInboundHarness()
	for name, typ := range map[string]string{
		"DockAppointmentCheckedIn (analytics only)": typeCheckedIn,
		"DoorClosureScheduled":                      typeClosure,
		"ASNRegistered":                             typeASNRegistered,
		"RETIRED inbound-receiving Booked":          typeLegacyBooked,
		"RETIRED inbound-receiving Cancelled":       typeLegacyCancelled,
		"right suffix, wrong owner":                 "com.warehouse.wms.someone-else.dockappointment.DockAppointmentBooked",
		"short name only":                           "DockAppointmentBooked",
	} {
		msg := dockWireEvent(t, "evt-"+name, typ, apptID, bookedAt, bookedData(apptID, "INBOUND", doorStart, doorEnd, 1, "ASN-1"))
		if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
			t.Errorf("%s: err = %v, want nil", name, err)
		}
	}
	if h.repo.Len() != 0 || h.uow.calls.Load() != 0 {
		t.Fatalf("an ignored type wrote %d appointments / opened %d units of work", h.repo.Len(), h.uow.calls.Load())
	}
}

func TestDockAppointmentConsumer_UnusableMessagesArePoisonAndNeverTouchTheDatabase(t *testing.T) {
	h := newInboundHarness()
	legacyFlat, _ := json.Marshal(map[string]any{"event_id": "e-1", "event_type": "DockAppointmentBooked", "payload": bookedData(apptID, "INBOUND", doorStart, doorEnd, 1, "ASN-1")})
	with := func(mutate func(map[string]any)) map[string]any {
		d := bookedData(apptID, "INBOUND", doorStart, doorEnd, 1, "ASN-1")
		mutate(d)
		return d
	}
	resched := func(mutate func(map[string]any)) map[string]any {
		d := map[string]any{"appointment_id": apptID, "site_id": inboundSite, "window_start": movedStart.Format(time.RFC3339), "window_end": movedEnd.Format(time.RFC3339), "version": 2}
		mutate(d)
		return d
	}
	cancel := func(mutate func(map[string]any)) map[string]any {
		d := map[string]any{"appointment_id": apptID, "site_id": inboundSite, "version": 2}
		mutate(d)
		return d
	}
	noShow := func(mutate func(map[string]any)) map[string]any {
		d := map[string]any{"appointment_id": apptID, "site_id": inboundSite, "direction": "INBOUND", "version": 2}
		mutate(d)
		return d
	}
	w := func(typ, subject string, at time.Time, data map[string]any) []byte {
		return dockWireEvent(t, "e-"+typ+subject, typ, subject, at, data)
	}
	none := func(map[string]any) {}
	cases := map[string][]byte{
		"not json":                           []byte("this is not json"),
		"empty":                              nil,
		"json but not an event":              []byte(`{"hello":"world"}`),
		"legacy flat envelope":               legacyFlat,
		"booked: missing id":                 w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { d["appointment_id"] = "" })),
		"booked: missing site_id":            w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { d["site_id"] = "" })),
		"booked: no site_id field":           w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { delete(d, "site_id") })),
		"booked: subject is not the id":      w(typeBooked, "other", bookedAt, with(none)),
		"booked: no direction":               w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { delete(d, "direction") })),
		"booked: unknown direction":          w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { d["direction"] = "SIDEWAYS" })),
		"booked: window end not after start": w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { d["window_end"] = doorStart.Format(time.RFC3339) })),
		"booked: inbound with no ASN":        w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { d["asn_numbers"] = []string{} })),
		"booked: malformed window_start":     w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { d["window_start"] = "tomorrow-ish" })),
		"booked: no version":                 w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { delete(d, "version") })),
		"booked: version zero":               w(typeBooked, apptID, bookedAt, with(func(d map[string]any) { d["version"] = 0 })),
		"booked: no event time":              w(typeBooked, apptID, time.Time{}, with(none)),
		"rescheduled: missing id":            w(typeRescheduled, apptID, bookedAt, resched(func(d map[string]any) { d["appointment_id"] = "" })),
		"rescheduled: missing site_id":       w(typeRescheduled, apptID, bookedAt, resched(func(d map[string]any) { delete(d, "site_id") })),
		"rescheduled: subject is not the id": w(typeRescheduled, "other", bookedAt, resched(none)),
		"rescheduled: window end not after":  w(typeRescheduled, apptID, bookedAt, resched(func(d map[string]any) { d["window_end"] = d["window_start"] })),
		"rescheduled: no version":            w(typeRescheduled, apptID, bookedAt, resched(func(d map[string]any) { delete(d, "version") })),
		"rescheduled: no event time":         w(typeRescheduled, apptID, time.Time{}, resched(none)),
		"cancelled: missing id":              w(typeCancelled, apptID, bookedAt, cancel(func(d map[string]any) { delete(d, "appointment_id") })),
		"cancelled: missing site_id":         w(typeCancelled, apptID, bookedAt, cancel(func(d map[string]any) { delete(d, "site_id") })),
		"cancelled: subject is not the id":   w(typeCancelled, "other", bookedAt, cancel(none)),
		"cancelled: no version":              w(typeCancelled, apptID, bookedAt, cancel(func(d map[string]any) { delete(d, "version") })),
		"cancelled: no event time":           w(typeCancelled, apptID, time.Time{}, cancel(none)),
		"no-show: missing id":                w(typeNoShow, apptID, bookedAt, noShow(func(d map[string]any) { delete(d, "appointment_id") })),
		"no-show: missing site_id":           w(typeNoShow, apptID, bookedAt, noShow(func(d map[string]any) { delete(d, "site_id") })),
		"no-show: unknown direction":         w(typeNoShow, apptID, bookedAt, noShow(func(d map[string]any) { d["direction"] = "" })),
		"no-show: subject is not the id":     w(typeNoShow, "other", bookedAt, noShow(none)),
		"no-show: no version":                w(typeNoShow, apptID, bookedAt, noShow(func(d map[string]any) { delete(d, "version") })),
		"no-show: no event time":             w(typeNoShow, apptID, time.Time{}, noShow(none)),
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

func TestDockAppointmentConsumer_TransientFailureLeavesNothingAndRetrySucceeds(t *testing.T) {
	h := newInboundHarness()
	ctx := context.Background()
	msg := bookedWire(t, "evt-1", bookedAt, 1, "ASN-1")

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

func TestDockAppointmentConsumer_RunCommitsAfterSuccessAndDeadLettersPoisonWithoutRetry(t *testing.T) {
	h := newInboundHarness()
	dlq := &fakeDLQWriter{}
	h.consumer.DLQ, h.consumer.DLQTopic = dlq, "warehouse.dock-scheduling.events.dlq"
	garbage := []byte("garbage that must be dead-lettered, not retried")
	reader := newFakeReader(bookedWire(t, "evt-1", bookedAt, 1, "ASN-1"), garbage, noShowWire(t, "evt-2", bookedAt.Add(time.Hour), 2))
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
	if len(published) != 1 || string(published[0].Value) != string(garbage) || published[0].Topic != "warehouse.dock-scheduling.events.dlq" {
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
	if a, _ := h.repo.Get(apptID); a.Status() != inbound.StatusNoShow {
		t.Fatalf("status = %s: the messages around the poison one must still be applied in order", a.Status())
	}
	if got := h.flaky.applyCalls(); got != 3 {
		t.Fatalf("Apply attempts = %d, want 3 (one failure + one success on the first message, one on the third); the poison one never reaches it", got)
	}
}

func TestDockAppointmentConsumer_RunWithoutADLQSkipsPoisonInsteadOfRetryingForever(t *testing.T) {
	h := newInboundHarness()
	reader := newFakeReader([]byte("garbage"), bookedWire(t, "evt-1", bookedAt, 1, "ASN-1"))
	h.consumer.Reader = reader
	if err := runUntilCommits(t, reader, 2, h.consumer.Run); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
	if h.demand(t).Appointments != 1 {
		t.Fatal("the message after the poison one never landed")
	}
}
