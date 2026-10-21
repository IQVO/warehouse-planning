//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/jackc/pgx/v5/pgxpool"
	kafkago "github.com/segmentio/kafka-go"
	"github.com/testcontainers/testcontainers-go"
	tckafka "github.com/testcontainers/testcontainers-go/modules/kafka"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// The end-to-end proof that the shipped binary is really wired: the test boots
// the REAL entry point (run, in-process) against testcontainers Postgres and
// Kafka with nothing but the environment a deployment sets, and drives it only
// through REST and Kafka. It feeds the three consumers (labor capacity,
// storage capacity, order demand) through the input topics, creates and
// publishes a capacity plan over REST, reads the effects from Postgres and
// from the output topics (the integration topic and the analytics topic),
// restarts the service, and finally takes the dependencies away to watch
// /readyz and the exit-on-dead-worker behaviour. Every Kafka read is bounded
// by a deadline; run it with -timeout 300s.

const (
	topicLabor     = "warehouse.workforce.events"
	topicFacility  = "warehouse.facility.events"
	topicOrders    = "warehouse.order-management.events"
	topicInbound   = "warehouse.inbound-receiving.events"
	topicFulfil    = "warehouse.fulfillment.events"
	topicEvents    = "warehouse.warehouse-planning.events"
	topicAnalytics = "warehouse.warehouse-planning.analytics"

	source     = "/warehouse/warehouse-planning"
	typePrefix = "com.warehouse.wes.warehouse-planning."
	siteID     = "WH1"

	windowStart = "2026-10-05T08:00:00Z"
	windowEnd   = "2026-10-05T16:00:00Z"
)

// consumedEvents is how many distinct CloudEvents ids the five consumers
// claim in the whole test: 4 in phaseConsumers, 4 dock-appointment events and 5
// task completions in phaseInboundObserved (duplicates excluded).
const consumedEvents = 13

// containerStartTimeout bounds a testcontainers start.
const containerStartTimeout = 120 * time.Second

func startPostgres(t *testing.T) (*tcpostgres.PostgresContainer, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), containerStartTimeout)
	defer cancel()
	c, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("warehouse_planning_e2e"), tcpostgres.WithUsername("e2e"), tcpostgres.WithPassword("e2e"),
		testcontainers.WithWaitStrategy(wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(60*time.Second)))
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	url, err := c.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("postgres dsn: %v", err)
	}
	return c, url
}

func startKafka(t *testing.T) (*tckafka.KafkaContainer, []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), containerStartTimeout)
	defer cancel()
	c, err := tckafka.Run(ctx, "confluentinc/confluent-local:7.6.1", tckafka.WithClusterID("warehouse-planning-e2e"))
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })
	brokers, err := c.Brokers(ctx)
	if err != nil {
		t.Fatalf("kafka brokers: %v", err)
	}
	return c, brokers
}

// topicExists asks the broker for the topic through a metadata request that
// cannot create it (kafka-go's Client.Metadata never auto-creates).
func topicExists(t *testing.T, brokers []string, topic string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := (&kafkago.Client{Addr: kafkago.TCP(brokers...)}).Metadata(ctx, &kafkago.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	for _, tp := range resp.Topics {
		if tp.Name == topic && tp.Error == nil {
			return true
		}
	}
	return false
}

// createTopic creates a one-partition topic and waits for its leader.
func createTopic(t *testing.T, brokers []string, topic string) {
	t.Helper()
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		t.Fatalf("dial broker: %v", err)
	}
	defer conn.Close()
	if err := conn.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}); err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if parts, err := conn.ReadPartitions(topic); err == nil && len(parts) == 1 && parts[0].Leader.ID != 0 {
			return
		}
	}
	t.Fatalf("topic %s never got a leader", topic)
}

// produce writes already-encoded messages to topic with RequireAll acks.
func produce(t *testing.T, brokers []string, topic string, msgs ...kafkago.Message) {
	t.Helper()
	transport := &kafkago.Transport{}
	defer transport.CloseIdleConnections()
	w := &kafkago.Writer{Addr: kafkago.TCP(brokers...), Topic: topic, Balancer: &kafkago.Hash{}, RequiredAcks: kafkago.RequireAll, BatchTimeout: 10 * time.Millisecond, Transport: transport}
	defer w.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.WriteMessages(ctx, msgs...); err != nil {
		t.Fatalf("produce to %s: %v", topic, err)
	}
}

// inputEvent builds a CloudEvent as a sibling context publishes it.
func inputEvent(t *testing.T, id, src, typ, subject string, at time.Time, data any) []byte {
	t.Helper()
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource(src)
	e.SetType(typ)
	e.SetSubject(subject)
	e.SetTime(at)
	e.SetDataSchema("urn:warehouse:e2e:events:X:v1")
	if err := e.SetData("application/json", data); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// consumeAll reads exactly n messages of the one-partition topic from the
// start, bounded by a deadline.
func consumeAll(t *testing.T, brokers []string, topic string, n int) []kafkago.Message {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := r.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatal(err)
	}
	var out []kafkago.Message
	for len(out) < n {
		m, err := r.ReadMessage(ctx)
		if err != nil {
			t.Fatalf("read %s: got %d of %d messages: %v", topic, len(out), n, err)
		}
		out = append(out, m)
	}
	return out
}

// noMoreMessages fails if the topic holds more than want messages after a
// settling period (nothing was duplicated).
func noMoreMessages(t *testing.T, brokers []string, topic string, want int) {
	t.Helper()
	r := kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 200 * time.Millisecond})
	defer r.Close()
	if err := r.SetOffset(kafkago.FirstOffset); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	got := 0
	for {
		if _, err := r.ReadMessage(ctx); err != nil {
			break
		}
		got++
	}
	if got != want {
		t.Fatalf("%s holds %d messages, want exactly %d", topic, got, want)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// service is one running instance of the real entry point.
type service struct {
	t      *testing.T
	base   string
	cancel context.CancelFunc
	done   chan error
}

// env is the environment a deployment sets.
type env struct {
	dbURL   string
	brokers []string
	addr    string
}

func (e env) apply(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", e.dbURL)
	t.Setenv("MIGRATIONS_DATABASE_URL", "")
	t.Setenv("MIGRATIONS_PATH", "../../internal/adapters/outbound/postgres/migrations")
	t.Setenv("KAFKA_BROKERS", strings.Join(e.brokers, ","))
	t.Setenv("EVENT_PUBLISHER", "kafka")
	t.Setenv("LABOR_CAPACITY_CONSUMER_GROUP", "warehouse-planning-labor-capacity-e2e")
	t.Setenv("STORAGE_CAPACITY_CONSUMER_GROUP", "warehouse-planning-storage-capacity-e2e")
	t.Setenv("DEMAND_CONSUMER_GROUP", "warehouse-planning-demand-e2e")
	t.Setenv("INBOUND_DEMAND_CONSUMER_GROUP", "warehouse-planning-inbound-demand-e2e")
	t.Setenv("OBSERVED_CAPACITY_CONSUMER_GROUP", "warehouse-planning-observed-capacity-e2e")
	t.Setenv("DEMAND_SITE_ID", siteID)
	t.Setenv("HTTP_ADDR", e.addr)
	t.Setenv("SHUTDOWN_DRAIN_DELAY", "0")
	t.Setenv("OUTBOX_RELAY_INTERVAL", "200ms")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:1")
	t.Setenv("LOG_LEVEL", "warn")
}

// start boots run in-process and waits until the listener answers /healthz.
func (e env) start(t *testing.T) *service {
	t.Helper()
	e.apply(t)
	ctx, cancel := context.WithCancel(context.Background())
	s := &service{t: t, base: "http://" + e.addr, cancel: cancel, done: make(chan error, 1)}
	go func() { s.done <- run(ctx) }()
	t.Cleanup(s.stopQuietly)
	waitFor(t, 90*time.Second, "the service to listen on "+e.addr, func() bool {
		select {
		case err := <-s.done:
			t.Fatalf("run returned while booting: %v", err)
		default:
		}
		status, _ := s.try(http.MethodGet, "/healthz", "")
		return status == http.StatusOK
	})
	return s
}

// stop cancels run and requires a clean shutdown within a bound.
func (s *service) stop() {
	s.t.Helper()
	s.cancel()
	select {
	case err := <-s.done:
		if err != nil {
			s.t.Fatalf("shutdown returned %v", err)
		}
	case <-time.After(60 * time.Second):
		s.t.Fatal("run did not return after the shutdown")
	}
	s.done <- nil // let the cleanup find it stopped
}

func (s *service) stopQuietly() {
	s.cancel()
	select {
	case <-s.done:
	case <-time.After(60 * time.Second):
	}
}

// expectExit waits for run to return on its own (nobody cancelled it) and
// returns its error.
func (s *service) expectExit() error {
	s.t.Helper()
	select {
	case err := <-s.done:
		s.done <- nil // let the cleanup find it stopped
		return err
	case <-time.After(120 * time.Second):
		s.t.Fatal("run did not return although Kafka is gone")
		return nil
	}
}

// try performs a request and returns -1 when the connection fails.
func (s *service) try(method, path, body string) (int, string) {
	req, err := http.NewRequestWithContext(context.Background(), method, s.base+path, strings.NewReader(body))
	if err != nil {
		return -1, err.Error()
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return -1, err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func (s *service) call(method, path, body string, want int) string {
	s.t.Helper()
	status, raw := s.try(method, path, body)
	if status != want {
		s.t.Fatalf("%s %s = %d %s, want %d", method, path, status, raw, want)
	}
	return raw
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s", timeout, what)
}

// pgView reads the service's own database.
type pgView struct {
	t    *testing.T
	pool *pgxpool.Pool
}

func newPGView(t *testing.T, url string) pgView {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return pgView{t: t, pool: pool}
}

func (v pgView) count(query string, args ...any) int {
	v.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var n int
	if err := v.pool.QueryRow(ctx, query, args...).Scan(&n); err != nil {
		v.t.Fatalf("%s: %v", query, err)
	}
	return n
}

// The CloudEvents context attributes of a published message, plus its data.
type envelope struct {
	SpecVersion     string          `json:"specversion"`
	ID              string          `json:"id"`
	Source          string          `json:"source"`
	Type            string          `json:"type"`
	Subject         string          `json:"subject"`
	Time            string          `json:"time"`
	DataContentType string          `json:"datacontenttype"`
	DataSchema      string          `json:"dataschema"`
	Data            json.RawMessage `json:"data"`
}

// assertEnvelope checks every context attribute, the Kafka key (the aggregate
// id, equal to the subject) and the content-type header of a message of this
// service, and returns the decoded envelope.
func assertEnvelope(t *testing.T, m kafkago.Message, stream string) envelope {
	t.Helper()
	var e envelope
	if err := json.Unmarshal(m.Value, &e); err != nil {
		t.Fatalf("not a CloudEvent: %v: %s", err, m.Value)
	}
	name := e.Type[strings.LastIndex(e.Type, ".")+1:]
	schema := fmt.Sprintf("urn:warehouse:warehouse-planning:%s:%s:v1", stream, name)
	if e.SpecVersion != "1.0" || !strings.HasPrefix(e.Type, typePrefix) || e.Source != source || e.DataSchema != schema ||
		e.DataContentType != "application/json" || e.Subject == "" || e.ID == "" {
		t.Fatalf("envelope = %+v, want 1.0 %s* from %s with dataschema %s", e, typePrefix, source, schema)
	}
	if _, err := time.Parse(time.RFC3339Nano, e.Time); err != nil {
		t.Fatalf("time %q is not RFC 3339: %v", e.Time, err)
	}
	if string(m.Key) != e.Subject {
		t.Fatalf("kafka key = %q, want the subject %q", m.Key, e.Subject)
	}
	var ct string
	for _, h := range m.Headers {
		if h.Key == "content-type" {
			ct = string(h.Value)
		}
	}
	if ct != "application/cloudevents+json; charset=UTF-8" {
		t.Fatalf("content-type header = %q", ct)
	}
	return e
}

func mustDecode(t *testing.T, raw string, dest any) {
	t.Helper()
	if err := json.Unmarshal([]byte(raw), dest); err != nil {
		t.Fatalf("decode %s: %v", raw, err)
	}
}

func assertProblem(t *testing.T, body, slug string) {
	t.Helper()
	if !strings.Contains(body, "/"+slug+`"`) {
		t.Fatalf("body %s is not the %s problem", body, slug)
	}
}

func waitReadyzStatus(t *testing.T, s *service, want int, what string) string {
	t.Helper()
	var body string
	waitFor(t, 60*time.Second, what, func() bool {
		var status int
		status, body = s.try(http.MethodGet, "/readyz", "")
		return status == want
	})
	return body
}

// world is what the phases share.
type world struct {
	e       env
	pg      *tcpostgres.PostgresContainer
	kc      *tckafka.KafkaContainer
	db      pgView
	brokers []string
	planID  string
}

// Phase 1: the output topics do not exist yet (a fresh cluster before
// anything published). Readiness needs Postgres plus a broker answering
// metadata, NOT topics, so the service is ready at once, and it creates no
// output topic itself.
func phaseTopicsMissing(t *testing.T, s *service, brokers []string) {
	t.Helper()
	body := waitReadyzStatus(t, s, http.StatusOK, "/readyz to be 200 although no output topic exists")
	if !strings.Contains(body, "ready") {
		t.Fatalf("readyz = %s", body)
	}
	for _, topic := range []string{topicEvents, topicAnalytics} {
		if topicExists(t, brokers, topic) {
			t.Fatalf("topic %s exists: the service must never create its output topics at boot", topic)
		}
	}
}

func orderData(orderID, promise string) map[string]any {
	return map[string]any{"order_id": orderID, "promise_date": promise, "lines": []map[string]any{
		{"line_no": 1, "sku": "SKU-1", "path_id": "pick", "gift_wrap": false, "fulfillment_class": "SINGLE"},
	}}
}

// Phase 2: the three consumers are fed through Kafka only. A duplicate
// delivery (same CloudEvents id) is applied once.
func phaseConsumers(t *testing.T, w *world) {
	t.Helper()
	at := time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	// 1 head x 0.1 units/hour for 8 hours: far less than two orders of demand.
	produce(t, w.brokers, topicLabor, kafkago.Message{Key: []byte("WH1/SHIFT-1"), Value: inputEvent(t, "lab-1", "/warehouse/workforce-management",
		"com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted", "WH1/SHIFT-1", at,
		map[string]any{"building_id": siteID, "shift_id": "SHIFT-1", "path_id": "pick", "planned_heads": 1, "planned_rate": 0.1, "planned_hours": 8.0})})
	produce(t, w.brokers, topicFacility, kafkago.Message{Key: []byte("WH1-A-01"), Value: inputEvent(t, "loc-1", "/warehouse/facility-layout",
		"com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered", "WH1-A-01", at,
		map[string]any{"locationCode": "WH1-A-01", "zoneId": "ZONE-A", "locationType": "BULK", "role": "Storage"})})
	const orderType = "com.warehouse.wes.order-management.order."
	o1 := inputEvent(t, "ord-e1", "/warehouse/order-management", orderType+"OrderAllocated", "ord-1", at, orderData("ord-1", "2026-10-05T10:00:00Z"))
	o2 := inputEvent(t, "ord-e2", "/warehouse/order-management", orderType+"OrderPartiallyAllocated", "ord-2", at.Add(time.Second), orderData("ord-2", "2026-10-05T12:00:00Z"))
	produce(t, w.brokers, topicOrders, kafkago.Message{Key: []byte("ord-1"), Value: o1}, kafkago.Message{Key: []byte("ord-1"), Value: o1}, kafkago.Message{Key: []byte("ord-2"), Value: o2})

	waitFor(t, 90*time.Second, "the three consumers to be fed from Kafka", func() bool {
		return w.db.count(`SELECT count(*) FROM process_capacity`) == 1 && w.db.count(`SELECT count(*) FROM location_slot_registration`) == 1 &&
			w.db.count(`SELECT count(*) FROM order_demand`) == 2
	})
	waitFor(t, 30*time.Second, "every message to be claimed", func() bool {
		return w.db.count(`SELECT count(*) FROM processed_events`) == 4
	})
	if n := w.db.count(`SELECT count(*) FROM process_capacity_constraint WHERE constraint_type = 'LABOR'`); n != 1 {
		t.Fatalf("LABOR constraints = %d, want 1", n)
	}
}

func dockData(id, start, end string, asns ...string) map[string]any {
	return map[string]any{"appointment_id": id, "door_code": "WH1-DOCK-IN-01", "carrier": "ACME", "window_start": start, "window_end": end, "asn_numbers": asns}
}

func taskData(id, taskType string) map[string]any {
	return map[string]any{"task_id": id, "task_type": taskType, "duration_seconds": 60}
}

// Phase 2b (ADR 0017): the dock-appointment and task-completion consumers are
// fed through Kafka only. A Cancelled that arrives BEFORE its Booked wins, a
// redelivered id is applied once, an appointment on the window end is not in
// the window, and the labour comes only from the declared standard: until one
// is declared the appointment is counted, unpriced and reported missing.
func phaseInboundObserved(t *testing.T, s *service, w *world) {
	t.Helper()
	const dockType = "com.warehouse.wms.inbound-receiving.dockappointment."
	const taskType = "com.warehouse.wes.fulfillment-execution.task.TaskCompleted"
	at := time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)
	ev := func(id, typ, subject string, when time.Time, data any) kafkago.Message {
		return kafkago.Message{Key: []byte(subject), Value: inputEvent(t, id, "/warehouse/e2e", typ, subject, when, data)}
	}
	booked2 := ev("dock-e3", dockType+"DockAppointmentBooked", "appt-2", at.Add(5*time.Minute), dockData("appt-2", "2026-10-05T09:00:00Z", "2026-10-05T10:30:00Z", "ASN-2"))
	produce(t, w.brokers, topicInbound,
		ev("dock-e1", dockType+"DockAppointmentCancelled", "appt-1", at.Add(time.Hour), map[string]any{"appointment_id": "appt-1", "door_code": "WH1-DOCK-IN-01"}),
		ev("dock-e2", dockType+"DockAppointmentBooked", "appt-1", at, dockData("appt-1", "2026-10-05T08:30:00Z", "2026-10-05T09:30:00Z", "ASN-1", "ASN-1b")),
		booked2, booked2,
		ev("dock-e4", dockType+"DockAppointmentBooked", "appt-3", at.Add(10*time.Minute), dockData("appt-3", windowEnd, "2026-10-05T17:00:00Z", "ASN-3")))
	done := func(id, kind string, when string) kafkago.Message {
		ts, err := time.Parse(time.RFC3339, when)
		if err != nil {
			t.Fatal(err)
		}
		return ev("task-e-"+id, taskType, "task-"+id, ts, taskData("task-"+id, kind))
	}
	first := done("1", "PICK", "2026-10-05T09:00:00Z")
	produce(t, w.brokers, topicFulfil, first, first,
		done("2", "PICK", "2026-10-05T09:00:30Z"), done("3", "PICK", "2026-10-05T15:59:00Z"),
		done("4", "PACK", "2026-10-05T09:00:00Z"), done("5", "PICK", "2026-10-05T07:59:00Z"))

	waitFor(t, 90*time.Second, "the dock-appointment and task-completion consumers to be fed from Kafka", func() bool {
		return w.db.count(`SELECT count(*) FROM inbound_appointments`) == 3 && w.db.count(`SELECT count(*) FROM processed_events`) == consumedEvents
	})
	if n := w.db.count(`SELECT count(*) FROM inbound_appointments WHERE status = 'BOOKED'`); n != 2 {
		t.Fatalf("BOOKED appointments = %d, want 2 (appt-1 was cancelled before it was booked)", n)
	}
	if n := w.db.count(`SELECT COALESCE(sum(tasks_completed), 0)::bigint FROM observed_capacity`); n != 5 {
		t.Fatalf("observed tasks = %d, want 5 (the duplicate id counted once)", n)
	}

	query := "/inbound-demand?location=" + siteID + "&path_id=pick-only&window_start=" + windowStart + "&window_end=" + windowEnd
	var dem struct {
		Appointments    int     `json:"appointments"`
		ASNs            int     `json:"asns"`
		LabourMinutes   float64 `json:"labour_minutes"`
		StandardMissing bool    `json:"standard_missing"`
	}
	mustDecode(t, s.call(http.MethodGet, query, "", http.StatusOK), &dem)
	if dem.Appointments != 1 || dem.ASNs != 1 || dem.LabourMinutes != 0 || !dem.StandardMissing {
		t.Fatalf("GET /inbound-demand before any standard = %+v, want 1 appointment, 1 ASN, 0 minutes and the standard reported missing", dem)
	}
	s.call(http.MethodPut, "/inbound-labour-standards/"+siteID+"/pick-only", `{"minutes_per_appointment":30,"minutes_per_asn":10}`, http.StatusCreated)
	mustDecode(t, s.call(http.MethodGet, query, "", http.StatusOK), &dem)
	if dem.Appointments != 1 || dem.LabourMinutes != 40 || dem.StandardMissing {
		t.Fatalf("GET /inbound-demand with the standard = %+v, want 1 appointment and 40 labour minutes", dem)
	}
}

// Phase 3b (ADR 0017): the advisory variance of the published plan. Only PICK
// completions inside the plan window count (3 of the 5 fed), the read changes
// no plan and queues no event.
func phaseVariance(t *testing.T, s *service, w *world) {
	t.Helper()
	outboxBefore := w.db.count(`SELECT count(*) FROM outbox_events`)
	var v struct {
		Step     string `json:"step"`
		Observed struct {
			HasData bool  `json:"has_data"`
			Tasks   int64 `json:"tasks"`
		} `json:"observed"`
		Comparison struct {
			Comparable     bool     `json:"comparable"`
			ObservedOrders *float64 `json:"observed_orders"`
		} `json:"comparison"`
		Advisory bool `json:"advisory"`
	}
	mustDecode(t, s.call(http.MethodGet, "/capacity-plans/"+w.planID+"/variance?tasks_per_order=1", "", http.StatusOK), &v)
	if v.Step != "PICK" || !v.Observed.HasData || v.Observed.Tasks != 3 || !v.Comparison.Comparable || v.Comparison.ObservedOrders == nil || *v.Comparison.ObservedOrders != 3 || !v.Advisory {
		t.Fatalf("variance = %+v, want the 3 PICK completions inside the window compared at 1 task per order, advisory", v)
	}
	var noFactor struct {
		Comparison struct {
			Comparable bool `json:"comparable"`
		} `json:"comparison"`
	}
	mustDecode(t, s.call(http.MethodGet, "/capacity-plans/"+w.planID+"/variance", "", http.StatusOK), &noFactor)
	if noFactor.Comparison.Comparable {
		t.Fatalf("variance without tasks_per_order is comparable: no variance may be invented")
	}
	assertProblem(t, s.call(http.MethodGet, "/capacity-plans/"+w.planID+"/variance?tasks_per_order=0", "", http.StatusUnprocessableEntity), "non-positive-tasks-per-order")
	if after := w.db.count(`SELECT count(*) FROM outbox_events`); after != outboxBefore {
		t.Fatalf("outbox rows %d -> %d: the advisory variance read queued something", outboxBefore, after)
	}
}

// Phase 3: REST only: register a path, read the expected demand the order
// consumer built, create a plan from it (a shortage, since the labor capacity
// is tiny), publish it.
func phaseWorkflow(t *testing.T, s *service, w *world) {
	t.Helper()
	s.call(http.MethodPost, "/process-paths", `{"id":"pick-only","name":"Pick","steps":["PICK"]}`, http.StatusCreated)
	var dem struct {
		Orders        int `json:"orders"`
		ReleasedLines int `json:"released_lines"`
	}
	mustDecode(t, s.call(http.MethodGet, "/demand?location="+siteID+"&window_start="+windowStart+"&window_end="+windowEnd, "", http.StatusOK), &dem)
	if dem.Orders != 2 {
		t.Fatalf("GET /demand = %+v, want the 2 orders fed through Kafka", dem)
	}
	body := fmt.Sprintf(`{"warehouse_id":"WH-1","site_id":%q,"location":%q,"path_id":"pick-only","window_start":%q,"window_end":%q,"units_per_order":1,"packages_per_order":1}`,
		siteID, siteID, windowStart, windowEnd)
	var plan struct {
		ID             string  `json:"id"`
		Status         string  `json:"status"`
		AssignedDemand float64 `json:"assigned_demand"`
		DemandSource   string  `json:"demand_source"`
		Shortage       float64 `json:"shortage"`

		InboundAppointments    int     `json:"inbound_appointments"`
		InboundLabourMinutes   float64 `json:"inbound_labour_minutes"`
		InboundStandardMissing bool    `json:"inbound_standard_missing"`
	}
	mustDecode(t, s.call(http.MethodPost, "/capacity-plans", body, http.StatusCreated), &plan)
	if plan.Status != "DRAFT" || plan.AssignedDemand != 2 || plan.DemandSource != "orders" || plan.Shortage <= 0 {
		t.Fatalf("created plan = %+v, want a DRAFT with demand 2 from orders and a shortage", plan)
	}
	// ADR 0017: the plan carries the one dock appointment of its window (the
	// cancelled one is not demand) priced with the standard phaseInboundObserved
	// declared (30 + 1 ASN x 10 minutes), next to, not inside, the order demand.
	if plan.InboundAppointments != 1 || plan.InboundLabourMinutes != 40 || plan.InboundStandardMissing {
		t.Fatalf("created plan inbound = %+v, want 1 appointment, 40 labour minutes and the standard present", plan)
	}
	w.planID = plan.ID
	s.call(http.MethodPost, "/capacity-plans/"+plan.ID+"/publish", "", http.StatusOK)
	var got struct {
		Status string `json:"status"`
	}
	mustDecode(t, s.call(http.MethodGet, "/capacity-plans/"+plan.ID, "", http.StatusOK), &got)
	if got.Status != "PUBLISHED" {
		t.Fatalf("GET /capacity-plans/%s = %+v, want PUBLISHED", plan.ID, got)
	}
}

// Phase 4: a NUL is a uniform client error on the real runtime (the guard's
// slugs malformed-request-body / resource-not-found / malformed-request-query),
// never a Postgres 22021 -> 500, and nothing is written.
func phaseNUL(t *testing.T, s *service, w *world) {
	t.Helper()
	before := w.db.count(`SELECT count(*) FROM process_paths`)
	assertProblem(t, s.call(http.MethodPost, "/process-paths", `{"id":"p-nul","name":"a\u0000b","steps":["PICK"]}`, http.StatusBadRequest), "malformed-request-body")
	assertProblem(t, s.call(http.MethodGet, "/capacity-plans/pl%00an", "", http.StatusNotFound), "resource-not-found")
	assertProblem(t, s.call(http.MethodGet, "/storage-capacity?location=WH%001", "", http.StatusBadRequest), "malformed-request-query")
	if after := w.db.count(`SELECT count(*) FROM process_paths`); after != before {
		t.Fatalf("process_paths %d -> %d: a NUL request wrote a row", before, after)
	}
}

// Phase 5: the rows are in Postgres, and the relay drained every outbox row.
func phaseRows(t *testing.T, w *world) (events, analytics int) {
	t.Helper()
	db := w.db
	if n := db.count(`SELECT count(*) FROM capacity_plans WHERE id = $1 AND status = 'PUBLISHED'`, w.planID); n != 1 {
		t.Fatalf("plan %s is not PUBLISHED in Postgres", w.planID)
	}
	waitFor(t, 60*time.Second, "the relay to publish every outbox row", func() bool {
		return db.count(`SELECT count(*) FROM outbox_events`) > 0 && db.count(`SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	events = db.count(`SELECT count(*) FROM outbox_events WHERE topic = $1`, topicEvents)
	analytics = db.count(`SELECT count(*) FROM outbox_events WHERE topic = $1`, topicAnalytics)
	if events+analytics != db.count(`SELECT count(*) FROM outbox_events`) {
		t.Fatalf("outbox rows for unknown topics: events=%d analytics=%d", events, analytics)
	}
	return events, analytics
}

// typesOf decodes and checks every message of a topic and returns the sorted
// distinct event names.
func typesOf(t *testing.T, msgs []kafkago.Message, stream string) []string {
	t.Helper()
	seen := map[string]bool{}
	for _, m := range msgs {
		e := assertEnvelope(t, m, stream)
		seen[strings.TrimPrefix(e.Type, typePrefix)] = true
	}
	var out []string
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func requireTypes(t *testing.T, topic string, got []string, want ...string) {
	t.Helper()
	have := strings.Join(got, ",")
	for _, w := range want {
		if !strings.Contains(have, w) {
			t.Fatalf("%s carries %v, want %s among them", topic, got, w)
		}
	}
}

// Phase 6: the CloudEvents on both output topics: exact envelope, key and
// stream; ProcessCapacity events are integration-only.
func phaseTopics(t *testing.T, w *world, events, analytics int) {
	t.Helper()
	ev := typesOf(t, consumeAll(t, w.brokers, topicEvents, events), "events")
	requireTypes(t, topicEvents, ev, "processcapacity.ProcessCapacityRegistered", "capacityplan.CapacityPlanCreated",
		"capacityplan.CapacityShortageDetected", "capacityplan.CapacityPlanPublished")
	an := typesOf(t, consumeAll(t, w.brokers, topicAnalytics, analytics), "analytics")
	requireTypes(t, topicAnalytics, an, "capacityplan.CapacityPlanCreated", "capacityplan.CapacityPlanPublished")
	if strings.Contains(strings.Join(an, ","), "processcapacity.") {
		t.Fatalf("%s carries ProcessCapacity events %v: the analytics stream is plan facts only", topicAnalytics, an)
	}
}

// Phase 7: restart the real entry point in-process on the same database and
// broker: nothing is lost, nothing is published or consumed twice.
func phaseRestart(t *testing.T, w *world, s *service, events, analytics int) *service {
	t.Helper()
	s.stop()
	s2 := w.e.start(t)
	var got struct {
		Status string `json:"status"`
	}
	mustDecode(t, s2.call(http.MethodGet, "/capacity-plans/"+w.planID, "", http.StatusOK), &got)
	if got.Status != "PUBLISHED" {
		t.Fatalf("after the restart the plan is %q, want PUBLISHED", got.Status)
	}
	// Give the restarted consumers time to (wrongly) re-apply and the relay to
	// (wrongly) re-publish; neither may happen.
	time.Sleep(3 * time.Second)
	if n := w.db.count(`SELECT count(*) FROM processed_events`); n != consumedEvents {
		t.Fatalf("processed_events after the restart = %d, want still %d", n, consumedEvents)
	}
	noMoreMessages(t, w.brokers, topicEvents, events)
	noMoreMessages(t, w.brokers, topicAnalytics, analytics)
	return s2
}

// Phase 8: /readyz follows Postgres going away (Kafka still up): the 503
// problem names only postgres, and /healthz stays liveness (200). Then Kafka
// goes away: the consumers and the relay lose their broker and the service
// EXITS non-zero naming the dead worker, so the orchestrator restarts it
// instead of leaving a pod alive with consumers that will never come back.
func phaseDependenciesGoAway(t *testing.T, s *service, w *world) {
	t.Helper()
	stopTimeout := 10 * time.Second
	if err := w.pg.Stop(context.Background(), &stopTimeout); err != nil {
		t.Fatalf("stop postgres: %v", err)
	}
	body := waitReadyzStatus(t, s, http.StatusServiceUnavailable, "/readyz to turn 503 once Postgres is gone")
	if !strings.Contains(body, "dependency-unavailable") || !strings.Contains(body, "not ready: postgres") || strings.Contains(body, "kafka") {
		t.Fatalf("readyz with Postgres down = %s, want dependency-unavailable naming only postgres", body)
	}
	if status, body := s.try(http.MethodGet, "/healthz", ""); status != http.StatusOK {
		t.Fatalf("healthz with Postgres down = %d %s, want 200 (liveness only)", status, body)
	}
	if err := w.kc.Stop(context.Background(), &stopTimeout); err != nil {
		t.Fatalf("stop kafka: %v", err)
	}
	err := s.expectExit()
	if err == nil || !strings.Contains(err.Error(), "stopped unexpectedly") {
		t.Fatalf("run returned %v after Kafka went away, want a 'stopped unexpectedly' error naming the dead worker", err)
	}
}

func TestRunOnRealPostgresAndKafka(t *testing.T) {
	pg, dbURL := startPostgres(t)
	kc, brokers := startKafka(t)
	// The input topics are infra's: create them the way the cluster has them.
	for _, topic := range []string{topicLabor, topicFacility, topicOrders, topicInbound, topicFulfil} {
		createTopic(t, brokers, topic)
	}
	w := &world{e: env{dbURL: dbURL, brokers: brokers, addr: freeAddr(t)}, pg: pg, kc: kc, db: newPGView(t, dbURL), brokers: brokers}

	s := w.e.start(t)
	phaseTopicsMissing(t, s, brokers)
	phaseConsumers(t, w)
	phaseInboundObserved(t, s, w)
	phaseWorkflow(t, s, w)
	phaseVariance(t, s, w)
	phaseNUL(t, s, w)
	events, analytics := phaseRows(t, w)
	phaseTopics(t, w, events, analytics)
	s = phaseRestart(t, w, s, events, analytics)
	phaseDependenciesGoAway(t, s, w)
}
