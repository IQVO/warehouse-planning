//go:build integration

package kafka_test

// Real Kafka and real Postgres, both from testcontainers (the module is
// blank-imported so internal/architecture/fitness_test.go's
// TestKafkaIntegrationTestsUseTestcontainers scan sees it; the broker and
// database lifecycles live in main_integration_test.go and
// atomic_consumers_integration_test.go, shared across this package).

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
	"github.com/claudioed/warehouse-planning/internal/domain/observed"
)

func startDockAppointmentConsumer(t *testing.T, brokers []string, topic string, appts ports.InboundAppointmentRepository, fx pgFixture) {
	t.Helper()
	dlq := kafkaconsumer.NewDomainDLQWriterForTest(brokers)
	c := &kafkaconsumer.DockAppointmentConsumer{
		Reader:   kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("dock-appointments")}),
		Record:   &usecases.RecordInboundAppointment{UoW: fx.uow, ProcessedEvents: fx.processed, Appointments: appts},
		Logger:   testLogger(),
		Retry:    itestRetry,
		DLQ:      dlq,
		DLQTopic: topic + kafkaconsumer.DLQSuffix,
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
}

func waitForInboundDemand(t *testing.T, repo ports.InboundAppointmentRepository, want int) inbound.Demand {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	var last inbound.Demand
	for time.Now().Before(deadline) {
		d, err := repo.Demand(context.Background(), "SIM1", inbWinFrom, inbWinTo)
		if err != nil {
			t.Fatalf("Demand: %v", err)
		}
		if last = d; d.Appointments == want {
			return d
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("the model never reached %d appointments in the window (last %+v)", want, last)
	return last
}

func readDeadLetters(t *testing.T, brokers []string, dlqTopic string, want int) []kafkago.Message {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: dlqTopic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	t.Cleanup(func() { _ = r.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var out []kafkago.Message
	for len(out) < want {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %s after %d messages: %v", dlqTopic, len(out), err)
		}
		out = append(out, m)
	}
	return out
}

// REAL dock-scheduling events on a REAL Kafka topic land in the REAL Postgres
// model, attributed to the events' site_id, with a Cancelled that arrives
// BEFORE its Booked, a Rescheduled that arrives before its Booked, a no-show, an
// OUTBOUND appointment, the retired inbound-receiving type, a redelivery of the
// same event id and an unusable message that is dead-lettered to the real .dlq
// topic.
func TestDockAppointmentConsumer_Integration_RealKafkaAndPostgres_OrderingKindsAndRedelivery(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.dock-scheduling.events")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, topic+kafkaconsumer.DLQSuffix)
	fx := startPgFixture(t)
	repo := postgres.NewInboundAppointmentRepo(fx.pool)
	startDockAppointmentConsumer(t, brokers, topic, repo, fx)

	const (
		kept       = "appt-keep"
		cancelled  = "appt-cancel-first"
		later      = "appt-cancel-later"
		moved      = "appt-moved-before-booked"
		noShow     = "appt-no-show"
		outbound   = "appt-outbound"
		legacyType = "appt-retired-source"
	)
	base := time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	wire := func(id, typ, appt string, at time.Time, data map[string]any) kafkago.Message {
		return kafkago.Message{Key: []byte(appt), Value: dockWireEvent(t, id, typ, appt, at, data)}
	}
	gone := func(appt string, version int) map[string]any {
		return map[string]any{"appointment_id": appt, "site_id": inboundSite, "door_code": "D", "reason": "CARRIER_REQUEST", "version": version}
	}
	resched := func(appt string, version int) map[string]any {
		return map[string]any{
			"appointment_id": appt, "site_id": inboundSite, "door_code": "D",
			"window_start": inbWinFrom.Add(time.Hour).Format(time.RFC3339), "window_end": inbWinFrom.Add(2 * time.Hour).Format(time.RFC3339),
			"previous_window_start": doorStart.Format(time.RFC3339), "previous_window_end": doorEnd.Format(time.RFC3339), "version": version,
		}
	}
	junk := []byte("not a cloud event at all")
	bookedMsg := wire("itest-b-keep", typeBooked, kept, base, bookedData(kept, "INBOUND", doorStart, doorEnd, 1, "ASN-1", "ASN-2"))
	publishMessages(t, brokers, topic,
		// 1. Cancelled BEFORE its Booked (the older Booked arrives afterwards).
		wire("itest-c-first", typeCancelled, cancelled, base.Add(time.Hour), gone(cancelled, 2)),
		wire("itest-b-first", typeBooked, cancelled, base, bookedData(cancelled, "INBOUND", doorStart, doorEnd, 1, "ASN-9")),
		// 2. A normal booking, redelivered with the SAME event id.
		bookedMsg,
		// 3. Noise that must write nothing: unusable message, an ignored type.
		kafkago.Message{Key: []byte("junk"), Value: junk},
		wire("itest-ci", typeCheckedIn, kept, base.Add(time.Minute), map[string]any{"appointment_id": kept, "site_id": inboundSite, "door_code": "D", "version": 9}),
		bookedMsg,
		// 4. Booked then Cancelled in order.
		wire("itest-b-later", typeBooked, later, base, bookedData(later, "INBOUND", doorStart, doorEnd, 1, "ASN-5")),
		wire("itest-c-later", typeCancelled, later, base.Add(time.Hour), gone(later, 2)),
		// 5. Rescheduled BEFORE its Booked: the newer window wins, the Booked
		// only completes the facts.
		wire("itest-r-moved", typeRescheduled, moved, base.Add(time.Hour), resched(moved, 2)),
		wire("itest-b-moved", typeBooked, moved, base, bookedData(moved, "INBOUND", doorStart.Add(24*time.Hour), doorEnd.Add(24*time.Hour), 1, "ASN-7")),
		// 6. Booked then NoShow.
		wire("itest-b-ns", typeBooked, noShow, base, bookedData(noShow, "INBOUND", doorStart, doorEnd, 1, "ASN-8")),
		wire("itest-n-ns", typeNoShow, noShow, base.Add(time.Hour), map[string]any{"appointment_id": noShow, "site_id": inboundSite, "door_code": "D", "direction": "INBOUND", "carrier_scac": "EXFT", "window_end": doorEnd.Format(time.RFC3339), "version": 2}),
		// 7. OUTBOUND is stored but is not demand.
		wire("itest-b-out", typeBooked, outbound, base, bookedData(outbound, "OUTBOUND", doorStart, doorEnd, 1)),
		// 8. The retired source's type: neither stored nor claimed.
		wire("itest-legacy", typeLegacyBooked, legacyType, base, map[string]any{"appointment_id": legacyType, "door_code": "D", "carrier": "C", "window_start": doorStart.Format(time.RFC3339), "window_end": doorEnd.Format(time.RFC3339), "asn_numbers": []string{"A"}}),
		// 9. The last message: lets the test know everything before it was handled.
		wire("itest-last", typeBooked, "appt-last", base, bookedData("appt-last", "OUTBOUND", doorStart, doorEnd, 1)),
	)

	deadline := time.Now().Add(60 * time.Second)
	for fx.processedRows(t, "dock-appointment-consumer", "itest-last") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the last message was never processed")
		}
		time.Sleep(200 * time.Millisecond)
	}
	// Demand of the original window: appt-keep (2 ASNs) and appt-moved (its
	// Rescheduled window starts inside inbWinFrom+1h).
	got := waitForInboundDemand(t, repo, 2)
	if got.ASNs != 3 {
		t.Fatalf("demand = %+v, want 2 appointments covering 3 ASNs (appt-keep 2 + appt-moved 1)", got)
	}

	type row struct{ status, direction string }
	for id, want := range map[string]row{
		cancelled: {"CANCELLED", "INBOUND"}, kept: {"BOOKED", "INBOUND"}, later: {"CANCELLED", "INBOUND"},
		moved: {"BOOKED", "INBOUND"}, noShow: {"NO_SHOW", "INBOUND"}, outbound: {"BOOKED", "OUTBOUND"},
	} {
		if n := fx.count(t, "SELECT count(*) FROM inbound_appointments WHERE appointment_id = $1 AND status = $2 AND direction = $3 AND location = $4", id, want.status, want.direction, inboundSite); n != 1 {
			t.Errorf("%s: want status %s direction %s at %s", id, want.status, want.direction, inboundSite)
		}
	}
	if n := fx.count(t, "SELECT count(*) FROM inbound_appointments WHERE appointment_id = $1 AND version = 2 AND asn_count = 1", moved); n != 1 {
		t.Errorf("%s: want version 2 (the Rescheduled) with the Booked's ASN count", moved)
	}
	if n := fx.count(t, "SELECT count(*) FROM inbound_appointments WHERE appointment_id = $1", legacyType); n != 0 {
		t.Fatalf("the retired inbound-receiving type was stored (%d rows)", n)
	}
	if n := fx.count(t, "SELECT count(*) FROM inbound_appointments"); n != 7 {
		t.Fatalf("inbound_appointments rows = %d, want 7 (the redelivery, the junk, CheckedIn and the retired type wrote nothing)", n)
	}
	if n := fx.processedRows(t, "dock-appointment-consumer", "itest-b-keep"); n != 1 {
		t.Fatalf("processed_events rows for the redelivered id = %d, want 1", n)
	}
	if n := fx.processedRows(t, "dock-appointment-consumer", "itest-b-first"); n != 1 {
		t.Fatalf("the late Booked must still be claimed once, got %d", n)
	}
	if n := fx.processedRows(t, "dock-appointment-consumer", "itest-legacy"); n != 0 {
		t.Fatalf("the retired type was claimed (%d)", n)
	}

	dead := readDeadLetters(t, brokers, topic+kafkaconsumer.DLQSuffix, 1)
	if string(dead[0].Value) != string(junk) {
		t.Fatalf("dead letter = %q, want the unusable message", dead[0].Value)
	}
	headers := map[string]string{}
	for _, h := range dead[0].Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers["x-dlq-source-topic"] != topic || headers["x-dlq-error"] == "" {
		t.Fatalf("x-dlq headers = %v", headers)
	}
}

// failFirstApply fails the first Apply with an infrastructure error.
type failFirstApply struct {
	ports.InboundAppointmentRepository
	calls atomic.Int32
}

func (f *failFirstApply) Apply(ctx context.Context, a inbound.Appointment) (bool, error) {
	if f.calls.Add(1) == 1 {
		return false, errInjected
	}
	return f.InboundAppointmentRepository.Apply(ctx, a)
}

// A failure after the claim on a REAL database rolls the claim back; the
// consumer retries the SAME message (offset not committed) and converges.
func TestDockAppointmentConsumer_Integration_FailureAfterClaimIsRetriedNotLost(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.dock-scheduling.events")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, topic+kafkaconsumer.DLQSuffix)
	fx := startPgFixture(t)
	flaky := &failFirstApply{InboundAppointmentRepository: postgres.NewInboundAppointmentRepo(fx.pool)}
	startDockAppointmentConsumer(t, brokers, topic, flaky, fx)

	evtID := fmt.Sprintf("itest-retry-%d", time.Now().UnixNano())
	publishMessages(t, brokers, topic, kafkago.Message{Key: []byte(apptID), Value: bookedWire(t, evtID, bookedAt, 1, "ASN-1")})

	waitForInboundDemand(t, flaky.InboundAppointmentRepository, 1)
	if got := flaky.calls.Load(); got < 2 {
		t.Fatalf("Apply attempts = %d, want >= 2: the message must be retried after the injected failure", got)
	}
	if n := fx.processedRows(t, "dock-appointment-consumer", evtID); n != 1 {
		t.Fatalf("processed_events rows = %d, want exactly 1 once the retry succeeded", n)
	}
}

// --- observed capacity -------------------------------------------------------

func startObservedCapacityConsumer(t *testing.T, brokers []string, topic string, fx pgFixture) {
	t.Helper()
	c := &kafkaconsumer.ObservedCapacityConsumer{
		Reader:   kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("observed-capacity")}),
		Record:   &usecases.RecordObservedCompletion{UoW: fx.uow, ProcessedEvents: fx.processed, Observed: postgres.NewObservedCapacityRepo(fx.pool)},
		Location: "SIM1",
		Logger:   testLogger(),
		Retry:    itestRetry,
		DLQ:      kafkaconsumer.NewDomainDLQWriterForTest(brokers),
		DLQTopic: topic + kafkaconsumer.DLQSuffix,
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
}

func waitForObservedTasks(t *testing.T, repo ports.ObservedCapacityRepository, step string, want int64) observed.Throughput {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	var last observed.Throughput
	for time.Now().Before(deadline) {
		th, err := repo.Throughput(context.Background(), "SIM1", step, obsFrom, obsTo)
		if err != nil {
			t.Fatalf("Throughput: %v", err)
		}
		if last = th; th.Tasks == want {
			return th
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("%s never reached %d tasks (last %+v)", step, want, last)
	return last
}

// REAL TaskCompleted events on a REAL Kafka topic are aggregated in the REAL
// Postgres model: a redelivery counts once, another type and another step are
// separated, and a completion with no task_type is dead-lettered to the real
// .dlq topic.
func TestObservedCapacityConsumer_Integration_RealKafkaAndPostgres_RedeliveryCountsOnce(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.fulfillment.events")
	createTopic(t, brokers, topic)
	createTopic(t, brokers, topic+kafkaconsumer.DLQSuffix)
	fx := startPgFixture(t)
	repo := postgres.NewObservedCapacityRepo(fx.pool)
	startObservedCapacityConsumer(t, brokers, topic, fx)

	msg := func(id, taskID, taskType string, at time.Time, dur any) kafkago.Message {
		return kafkago.Message{Key: []byte(taskID), Value: completedWire(t, id, taskID, taskType, at, dur)}
	}
	dup := msg("itest-1", "t1", "PICK", doneAt, 30)
	bad := msg("itest-bad", "t0", "", doneAt, 1)
	publishMessages(t, brokers, topic,
		dup,
		msg("itest-2", "t2", "PICK", doneAt.Add(time.Minute), 50),
		dup, // at-least-once redelivery of the same event id
		msg("itest-3", "t3", "PACK", doneAt, nil),
		kafkago.Message{Key: []byte("t9"), Value: taskWireEvent(t, "itest-claimed", typeTaskClaimed, "t9", doneAt, map[string]any{"task_id": "t9", "station_id": "s"})},
		bad,
		dup,
	)

	pick := waitForObservedTasks(t, repo, "PICK", 2)
	if pick.DurationSeconds != 80 || pick.TasksWithDuration != 2 {
		t.Fatalf("PICK = %+v", pick)
	}
	waitForObservedTasks(t, repo, "PACK", 1)
	deadline := time.Now().Add(45 * time.Second)
	for fx.processedRows(t, "observed-capacity-consumer", "itest-3") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the PACK message was never processed")
		}
		time.Sleep(200 * time.Millisecond)
	}
	if n := fx.count(t, "SELECT coalesce(sum(tasks_completed), 0) FROM observed_capacity"); n != 3 {
		t.Fatalf("tasks counted = %d, want 3 (the two redeliveries, TaskClaimed and the unusable one counted nothing)", n)
	}
	if n := fx.processedRows(t, "observed-capacity-consumer", "itest-1"); n != 1 {
		t.Fatalf("processed_events rows for the redelivered id = %d, want 1", n)
	}
	dead := readDeadLetters(t, brokers, topic+kafkaconsumer.DLQSuffix, 1)
	if string(dead[0].Value) != string(bad.Value) {
		t.Fatalf("dead letter = %q, want the task_type-less completion", dead[0].Value)
	}
}
