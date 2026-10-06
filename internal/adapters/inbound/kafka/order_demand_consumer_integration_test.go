//go:build integration

package kafka_test

// Blank-importing the testcontainers Kafka module satisfies
// internal/architecture/fitness_test.go's TestKafkaIntegrationTestsUseTestcontainers
// scan; the broker and Postgres lifecycles live in main_integration_test.go
// and atomic_consumers_integration_test.go, shared across this package.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

func realOrderEvent(t *testing.T, id, typ, orderID string, at time.Time, promise string, lines int) []byte {
	t.Helper()
	return orderWireEvent(t, id, typ, orderID, at, allocationData(orderID, promise, lines))
}

func startOrderDemandConsumer(t *testing.T, brokers []string, topic string, record *usecases.RecordOrderDemand) *kafkaconsumer.OrderDemandConsumer {
	t.Helper()
	c := &kafkaconsumer.OrderDemandConsumer{
		Reader:   kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: uniqueGroupID("order-demand")}),
		Record:   record,
		Location: "SIM1",
		Logger:   testLogger(),
		Retry:    itestRetry,
	}
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = c.Run(ctx) }()
	return c
}

func httpDo(t *testing.T, method, url string, body map[string]any) (int, map[string]any) {
	t.Helper()
	var reader *strings.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		reader = strings.NewReader(string(b))
	} else {
		reader = strings.NewReader("")
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func waitForOrders(t *testing.T, repo ports.OrderDemandRepository, want int) demand.Summary {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		s, err := repo.Expected(context.Background(), "SIM1", windowFrom, windowTo)
		if err != nil {
			t.Fatalf("Expected: %v", err)
		}
		if s.Orders == want {
			return s
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("the model never reached %d orders in the window", want)
	return demand.Summary{}
}

// A REAL order event on a REAL Kafka topic lands in the REAL Postgres model,
// and a plan created without assigned_demand through the real HTTP surface
// picks it up.
func TestOrderDemandConsumer_Integration_RealKafkaAndPostgres_PlanPicksUpTheDemand(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.order-management.events")
	createTopic(t, brokers, topic)
	fx := startPgFixture(t)

	demandRepo := postgres.NewOrderDemandRepo(fx.pool)
	record := &usecases.RecordOrderDemand{UoW: fx.uow, ProcessedEvents: fx.processed, Demand: demandRepo}
	startOrderDemandConsumer(t, brokers, topic, record)

	base := time.Now().UTC().Truncate(time.Second)
	dup := realOrderEvent(t, "itest-evt-1", typeAllocated, orderIDOne, base, promiseRFC3339, 2)
	publishMessages(t, brokers, topic,
		kafkago.Message{Key: []byte(orderIDOne), Value: dup},
		kafkago.Message{Key: []byte("junk"), Value: []byte("not a cloud event at all")},
		kafkago.Message{Key: []byte(orderIDOne), Value: orderWireEvent(t, "itest-evt-r", "com.warehouse.wes.order-management.order.OrderRepromised", orderIDOne, base.Add(time.Second),
			map[string]any{"order_id": orderIDOne, "cpt_id_old": "a", "cpt_id_new": "b", "reason": "TaskCPTMissed"})},
		kafkago.Message{Key: []byte(orderIDTwo), Value: realOrderEvent(t, "itest-evt-2", typePartially, orderIDTwo, base.Add(2*time.Second), "2026-10-05T12:00:00Z", 1)},
		kafkago.Message{Key: []byte(orderIDOne), Value: dup}, // at-least-once redelivery of the same event id
	)

	summary := waitForOrders(t, demandRepo, 2)
	if summary.ReleasedLines != 3 || !summary.AsOf.Equal(base.Add(2*time.Second)) {
		t.Fatalf("summary = %+v", summary)
	}
	if n := fx.count(t, "SELECT count(*) FROM order_demand"); n != 2 {
		t.Fatalf("order_demand rows = %d, want 2 (replay and Repromised/junk wrote nothing)", n)
	}
	if n := fx.processedRows(t, "order-demand-consumer", "itest-evt-1"); n != 1 {
		t.Fatalf("processed_events rows for the replayed id = %d, want 1", n)
	}

	// Real HTTP surface over the same Postgres model.
	pcs, paths, plans, ob := fx.pcs, postgres.NewProcessPathRepo(fx.pool), postgres.NewCapacityPlanRepo(fx.pool), postgres.NewOutboxRepo(fx.pool)
	expected := &usecases.GetExpectedDemand{Demand: demandRepo}
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: postgres.NewStationStandardRepo(fx.pool), Tally: fx.tally}
	enc := outboundkafka.NewEncoder()
	server := httptest.NewServer(inboundhttp.NewRouter(&inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: paths},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan:                &usecases.CreateCapacityPlan{PathCapacity: pathCapacity, Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: fx.uow, Demand: expected},
		PublishCapacityPlan:               &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: fx.uow},
		CapacityPlans:                     plans,
		GetExpectedDemand:                 expected,
	}))
	defer server.Close()

	status, got := httpDo(t, http.MethodGet, server.URL+"/demand?location=SIM1&window_start=2026-10-05T08:00:00Z&window_end=2026-10-05T16:00:00Z", nil)
	if status != http.StatusOK || got["orders"] != float64(2) || got["released_lines"] != float64(3) {
		t.Fatalf("GET /demand = %d %v", status, got)
	}

	for _, reg := range []map[string]any{
		{"process_type": "PICK", "location": "SIM1", "window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z", "constraint_type": "LABOR", "quantity": 1, "unit": "UNIT", "period_seconds": 3600},
	} {
		if status, body := httpDo(t, http.MethodPost, server.URL+"/process-capacities", reg); status != http.StatusCreated {
			t.Fatalf("register capacity: %d %v", status, body)
		}
	}
	if status, body := httpDo(t, http.MethodPost, server.URL+"/process-paths", map[string]any{"id": "pick-only", "name": "Pick", "steps": []string{"PICK"}}); status != http.StatusCreated {
		t.Fatalf("register path: %d %v", status, body)
	}

	planBody := map[string]any{
		"warehouse_id": "WH-1", "site_id": "SIM1", "location": "SIM1", "path_id": "pick-only",
		"window_start": "2026-10-05T08:00:00Z", "window_end": "2026-10-05T16:00:00Z",
		"units_per_order": 1, "packages_per_order": 1,
	}
	status, plan := httpDo(t, http.MethodPost, server.URL+"/capacity-plans", planBody)
	if status != http.StatusCreated {
		t.Fatalf("create plan: %d %v", status, plan)
	}
	// 1 UNIT/h over 8h = 8 orders of capacity; 2 expected orders: no shortage.
	if plan["assigned_demand"] != float64(2) || plan["demand_source"] != "orders" || plan["shortage"] != float64(0) || plan["capacity_over_window"] != float64(8) {
		t.Fatalf("plan = %v", plan)
	}
	id, _ := plan["id"].(string)
	if n := fx.count(t, "SELECT count(*) FROM capacity_plans WHERE id = '"+id+"' AND demand_source = 'orders' AND assigned_demand = 2"); n != 1 {
		t.Fatal("the plan row does not record demand 2 from orders")
	}

	planBody["assigned_demand"] = 100
	status, explicit := httpDo(t, http.MethodPost, server.URL+"/capacity-plans", planBody)
	if status != http.StatusCreated || explicit["assigned_demand"] != float64(100) || explicit["demand_source"] != "request" || explicit["shortage"] != float64(92) {
		t.Fatalf("explicit plan = %d %v", status, explicit)
	}
}

// failFirstUpsert fails the first Upsert with an infrastructure error and
// delegates afterwards; it counts how often Upsert was attempted.
type failFirstUpsert struct {
	ports.OrderDemandRepository
	calls atomic.Int32
}

func (f *failFirstUpsert) Upsert(ctx context.Context, o demand.Order) (bool, error) {
	if f.calls.Add(1) == 1 {
		return false, errInjected
	}
	return f.OrderDemandRepository.Upsert(ctx, o)
}

// A failure after the claim on a REAL database rolls the claim back; the
// consumer retries the SAME message (offset not committed) and converges.
func TestOrderDemandConsumer_Integration_FailureAfterClaimIsRetriedNotLost(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.order-management.events")
	createTopic(t, brokers, topic)
	fx := startPgFixture(t)

	flaky := &failFirstUpsert{OrderDemandRepository: postgres.NewOrderDemandRepo(fx.pool)}
	record := &usecases.RecordOrderDemand{UoW: fx.uow, ProcessedEvents: fx.processed, Demand: flaky}
	startOrderDemandConsumer(t, brokers, topic, record)

	evtID := fmt.Sprintf("itest-retry-%d", time.Now().UnixNano())
	publishMessages(t, brokers, topic, kafkago.Message{
		Key: []byte(orderIDOne), Value: realOrderEvent(t, evtID, typeAllocated, orderIDOne, time.Now().UTC(), promiseRFC3339, 2),
	})

	waitForOrders(t, flaky.OrderDemandRepository, 1)
	if got := flaky.calls.Load(); got < 2 {
		t.Fatalf("Upsert attempts = %d, want >= 2: the message must be retried after the injected failure", got)
	}
	if n := fx.processedRows(t, "order-demand-consumer", evtID); n != 1 {
		t.Fatalf("processed_events rows = %d, want exactly 1 once the retry succeeded", n)
	}
	if n := fx.count(t, "SELECT count(*) FROM order_demand"); n != 1 {
		t.Fatalf("order_demand rows = %d, want 1", n)
	}
}
