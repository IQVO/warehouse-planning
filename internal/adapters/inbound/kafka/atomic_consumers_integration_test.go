//go:build integration

package kafka_test

// Integration proofs (real Postgres + real Kafka, both via testcontainers;
// never skip-gated, never a hardcoded broker address) that Phase 3's
// consumers are atomic and at-least-once:
//   - a failure injected AFTER the tally step rolls back real Postgres rows
//     (tally, registration, constraint AND the processed_events claim);
//   - the retried message heals tally/constraint consistently and the
//     processed_events row exists exactly once;
//   - a message that fails mid-handling is NOT lost: it is re-handled in
//     process, its offset is committed only after success, and when the
//     consumer dies without ever succeeding the offset stays uncommitted so
//     the next consumer in the group is redelivered the message.
// The blank import below satisfies TestKafkaIntegrationTestsUseTestcontainers.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"
	_ "github.com/testcontainers/testcontainers-go/modules/kafka"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/jackc/pgx/v5/pgxpool"
)

// itestRetry keeps retries fast while still exercising the backoff path.
var itestRetry = kafkaconsumer.RetryPolicy{Initial: 50 * time.Millisecond, Max: 200 * time.Millisecond}

type pgFixture struct {
	pool      *pgxpool.Pool
	pcs       *postgres.ProcessCapacityRepo
	processed *postgres.ProcessedEventRepo
	tally     *postgres.StorageTallyRepo
	uow       *postgres.UnitOfWork
}

func startPgFixture(t *testing.T) pgFixture {
	t.Helper()
	_, pool := startPostgresPool(t)
	return pgFixture{
		pool:      pool,
		pcs:       postgres.NewProcessCapacityRepo(pool),
		processed: postgres.NewProcessedEventRepo(pool),
		tally:     postgres.NewStorageTallyRepo(pool),
		uow:       postgres.NewUnitOfWork(pool),
	}
}

func (f pgFixture) count(t *testing.T, query string, args ...any) int {
	t.Helper()
	var n int
	if err := f.pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (f pgFixture) processedRows(t *testing.T, consumer, eventID string) int {
	return f.count(t, "SELECT count(*) FROM processed_events WHERE consumer=$1 AND event_id=$2", consumer, eventID)
}

func (f pgFixture) tallyCount(t *testing.T, zone, typ, key string) int {
	t.Helper()
	var n int
	err := f.pool.QueryRow(context.Background(),
		"SELECT coalesce(max(count),0) FROM location_slot_tally WHERE zone_id=$1 AND tally_type=$2 AND tally_key=$3", zone, typ, key).Scan(&n)
	if err != nil {
		t.Fatalf("tally count: %v", err)
	}
	return n
}

func (f pgFixture) registrations(t *testing.T) int {
	return f.count(t, "SELECT count(*) FROM location_slot_registration")
}

// processCapacityRows counts every ProcessCapacity aggregate in Postgres. The
// facility consumer is a pure tally maintainer now: it must never create one.
func (f pgFixture) processCapacityRows(t *testing.T) int {
	return f.count(t, "SELECT count(*) FROM process_capacity")
}

// failingSaveRepo wraps the REAL Postgres ProcessCapacityRepository and
// fails Save with an injected error for the first `remaining` calls. It is
// the second step of a message (the tally step already ran, in the same
// real transaction), so this is a genuine mid-handling failure.
type failingSaveRepo struct {
	ports.ProcessCapacityRepository
	mu        sync.Mutex
	remaining int
	calls     int
}

func (r *failingSaveRepo) Save(ctx context.Context, pc *processcapacity.ProcessCapacity) error {
	r.mu.Lock()
	r.calls++
	fail := r.remaining > 0
	if fail {
		r.remaining--
	}
	r.mu.Unlock()
	if fail {
		return errInjected
	}
	return r.ProcessCapacityRepository.Save(ctx, pc)
}

func (r *failingSaveRepo) saveCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

// failingTallyRepo wraps the REAL Postgres tally and, for the first
// `remaining` RegisterSlot calls, APPLIES the real mutation inside the unit of
// work's transaction and then fails with an injected error: a genuine
// mid-handling failure after the tally step already ran.
type failingTallyRepo struct {
	ports.StorageTallyRepository
	mu        sync.Mutex
	remaining int
	calls     int
}

func (r *failingTallyRepo) RegisterSlot(ctx context.Context, code, zone, typ string, keys []string) ([]tally.Update, error) {
	updates, err := r.StorageTallyRepository.RegisterSlot(ctx, code, zone, typ, keys)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	r.calls++
	fail := r.remaining > 0
	if fail {
		r.remaining--
	}
	r.mu.Unlock()
	if fail {
		return nil, errInjected
	}
	return updates, nil
}

func (r *failingTallyRepo) registerCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.calls
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for: %s", timeout, what)
}

// committedOffset returns the group's committed offset for partition 0 of
// topic, or -1 if none (or the coordinator is not ready yet).
func committedOffset(brokers []string, groupID, topic string) int64 {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := &kafkago.Client{Addr: kafkago.TCP(brokers...)}
	resp, err := client.OffsetFetch(ctx, &kafkago.OffsetFetchRequest{GroupID: groupID, Topics: map[string][]int{topic: {0}}})
	if err != nil {
		return -1
	}
	for _, p := range resp.Topics[topic] {
		if p.Partition == 0 && p.Error == nil {
			return p.CommittedOffset
		}
	}
	return -1
}

func newStorageConsumerOnTopic(brokers []string, topic, groupID string, tallyRepo ports.StorageTallyRepository, fx pgFixture) *kafkaconsumer.StorageCapacityConsumer {
	return &kafkaconsumer.StorageCapacityConsumer{
		// Same reader configuration as production's readerConfig: GroupID,
		// and CommitInterval left unset (explicit commits only).
		Reader:          kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: groupID}),
		Tally:           tallyRepo,
		ProcessedEvents: fx.processed,
		UoW:             fx.uow,
		Logger:          testLogger(),
		Retry:           itestRetry,
	}
}

// startRun runs the consumer and returns a stop func that cancels it and
// waits for Run to return.
func startRun(t *testing.T, run func(context.Context) error) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Error("consumer Run did not return after cancel")
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func storageRegisteredMsg(t *testing.T, id, code string) kafkago.Message {
	return kafkago.Message{
		Key: []byte(code),
		Value: locationSlotWireEvent(t, id, "LocationSlotRegistered", time.Now().UTC(), map[string]any{
			"locationCode": code, "zoneId": "ZONE-A", "locationType": "BULK",
		}),
	}
}

// Postgres only: the mid-handling failure rolls back REAL rows (tally,
// registration AND the processed_events claim), the retry heals them, the
// processed_events row ends up exactly once -- and no ProcessCapacity row is
// ever written: the facility consumer only mutates the tally.
func TestStorageConsumer_Integration_MidHandlingFailureRollsBackRealRows_RetryHeals(t *testing.T) {
	fx := startPgFixture(t)
	failing := &failingTallyRepo{StorageTallyRepository: fx.tally, remaining: 1}
	c := &kafkaconsumer.StorageCapacityConsumer{ // no Reader: HandleMessage only
		Tally:           failing,
		ProcessedEvents: fx.processed,
		UoW:             fx.uow,
		Logger:          testLogger(),
	}
	ctx := context.Background()
	msg := storageRegisteredMsg(t, "itest-evt-1", "WH1-A-01").Value

	if err := c.HandleMessage(ctx, msg); err == nil {
		t.Fatal("HandleMessage must return the injected infrastructure error")
	}
	// The tally step really ran inside the tx; after the failure NONE of it
	// may be visible.
	if n := fx.tallyCount(t, "ZONE-A", "LOCATION", "BULK"); n != 0 {
		t.Errorf("tally = %d after failed message, want 0", n)
	}
	if n := fx.registrations(t); n != 0 {
		t.Errorf("location_slot_registration rows = %d, want 0 (otherwise the retry hits 'already registered' and never heals)", n)
	}
	if n := fx.processedRows(t, "storage-capacity-consumer", "itest-evt-1"); n != 0 {
		t.Errorf("processed_events rows = %d after failure, want 0 (un-claimed)", n)
	}

	if err := c.HandleMessage(ctx, msg); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if n := fx.tallyCount(t, "ZONE-A", "LOCATION", "BULK"); n != 1 {
		t.Errorf("tally = %d after retry, want 1", n)
	}
	if n := fx.processedRows(t, "storage-capacity-consumer", "itest-evt-1"); n != 1 {
		t.Errorf("processed_events rows = %d after retry, want exactly 1", n)
	}
	if n := fx.processCapacityRows(t); n != 0 {
		t.Errorf("process_capacity rows = %d, want 0: the facility consumer must only mutate the tally", n)
	}
	// Redelivery after success is a no-op.
	if err := c.HandleMessage(ctx, msg); err != nil {
		t.Fatal(err)
	}
	if n := fx.tallyCount(t, "ZONE-A", "LOCATION", "BULK"); n != 1 {
		t.Errorf("tally = %d after redelivery, want still 1", n)
	}
}

// Kafka + Postgres: a transient failure after the tally step is retried
// (the message is handled twice), ends consistent, and the offset is
// committed only after the success.
func TestStorageConsumer_Integration_TransientFailure_RetriedAndCommittedAfterSuccess(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.facility.events")
	createTopic(t, brokers, topic)
	groupID := uniqueGroupID("storage-capacity")
	fx := startPgFixture(t)
	failing := &failingTallyRepo{StorageTallyRepository: fx.tally, remaining: 1}

	c := newStorageConsumerOnTopic(brokers, topic, groupID, failing, fx)
	defer func() { _ = c.Close() }()
	publishMessages(t, brokers, topic, storageRegisteredMsg(t, "itest-evt-retry", "WH1-A-01"))
	startRun(t, c.Run)

	eventually(t, 60*time.Second, "tally incremented after the retry", func() bool {
		return fx.tallyCount(t, "ZONE-A", "LOCATION", "BULK") == 1
	})
	if calls := failing.registerCalls(); calls != 2 {
		t.Errorf("RegisterSlot called %d times, want 2 (the message was handled, failed, then handled again)", calls)
	}
	if n := fx.processedRows(t, "storage-capacity-consumer", "itest-evt-retry"); n != 1 {
		t.Errorf("processed_events rows = %d, want exactly 1", n)
	}
	if n := fx.processCapacityRows(t); n != 0 {
		t.Errorf("process_capacity rows = %d, want 0", n)
	}
	eventually(t, 30*time.Second, "offset committed past the message", func() bool {
		return committedOffset(brokers, groupID, topic) == 1
	})
}

// Kafka + Postgres: the message is NOT lost. consumer 1 can never succeed;
// it must retry in process and must NOT commit the offset. When it stops,
// consumer 2 (same group) is redelivered the message by Kafka, applies it,
// and only then is the offset committed. With the old ReadMessage
// auto-commit the offset would already have moved and consumer 2 would
// receive nothing.
func TestStorageConsumer_Integration_UncommittedOnFailure_RedeliveredToNextConsumer(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.facility.events")
	createTopic(t, brokers, topic)
	groupID := uniqueGroupID("storage-capacity")
	fx := startPgFixture(t)

	publishMessages(t, brokers, topic, storageRegisteredMsg(t, "itest-evt-redeliver", "WH1-A-01"))

	// Consumer 1: always fails after the tally step.
	broken := &failingTallyRepo{StorageTallyRepository: fx.tally, remaining: 1 << 30}
	c1 := newStorageConsumerOnTopic(brokers, topic, groupID, broken, fx)
	stop1 := startRun(t, c1.Run)
	eventually(t, 60*time.Second, "consumer 1 to retry the same message at least 3 times", func() bool {
		return broken.registerCalls() >= 3
	})
	if off := committedOffset(brokers, groupID, topic); off > 0 {
		t.Fatalf("offset %d committed although the message never succeeded (it would be lost)", off)
	}
	stop1()
	_ = c1.Close()

	// Nothing of the failed attempts is visible.
	if fx.tallyCount(t, "ZONE-A", "LOCATION", "BULK") != 0 || fx.registrations(t) != 0 ||
		fx.processedRows(t, "storage-capacity-consumer", "itest-evt-redeliver") != 0 {
		t.Fatal("failed attempts leaked state into Postgres")
	}

	// Consumer 2, same group, healthy: Kafka redelivers the message.
	c2 := newStorageConsumerOnTopic(brokers, topic, groupID, fx.tally, fx)
	defer func() { _ = c2.Close() }()
	startRun(t, c2.Run)
	eventually(t, 90*time.Second, "consumer 2 to be redelivered and apply the message", func() bool {
		return fx.tallyCount(t, "ZONE-A", "LOCATION", "BULK") == 1
	})
	if n := fx.processedRows(t, "storage-capacity-consumer", "itest-evt-redeliver"); n != 1 {
		t.Errorf("processed_events rows = %d, want exactly 1", n)
	}
	eventually(t, 30*time.Second, "offset committed after consumer 2's success", func() bool {
		return committedOffset(brokers, groupID, topic) == 1
	})
}

// Labor: same transient-failure story through the labor consumer (claim +
// upsert in one transaction).
func TestLaborConsumer_Integration_TransientFailure_RetriedAndCommittedAfterSuccess(t *testing.T) {
	brokers := startKafkaBroker(t)
	topic := uniqueTopic("warehouse.workforce.events")
	createTopic(t, brokers, topic)
	groupID := uniqueGroupID("labor-capacity")
	fx := startPgFixture(t)
	failing := &failingSaveRepo{ProcessCapacityRepository: fx.pcs, remaining: 1}

	occurredAt := time.Date(2026, 10, 5, 6, 0, 0, 0, time.UTC)
	eventID := fmt.Sprintf("itest-labor-retry-%d", time.Now().UnixNano())
	publishMessages(t, brokers, topic, kafkago.Message{
		Key: []byte("WH1/SHIFT-1"),
		Value: shiftPlanCommittedWireEvent(t, eventID, occurredAt, map[string]any{
			"building_id": "WH1", "shift_id": "SHIFT-1", "path_id": "pick",
			"planned_heads": 10, "planned_rate": 40.0, "planned_hours": 8.0,
		}),
	})

	c := &kafkaconsumer.LaborCapacityConsumer{
		Reader:          kafkago.NewReader(kafkago.ReaderConfig{Brokers: brokers, Topic: topic, GroupID: groupID}),
		Register:        &usecases.RegisterProcessCapacityConstraint{Repo: failing},
		ProcessedEvents: fx.processed,
		UoW:             fx.uow,
		Logger:          testLogger(),
		Retry:           itestRetry,
	}
	defer func() { _ = c.Close() }()
	startRun(t, c.Run)

	eventually(t, 60*time.Second, "LABOR constraint registered after the retry", func() bool {
		pc, err := fx.pcs.FindByProcessLocationWindow(context.Background(), "PICK", "WH1", occurredAt, occurredAt.Add(8*time.Hour))
		if err != nil || pc == nil {
			return false
		}
		rate, _, err := pc.EffectiveRate()
		return err == nil && rate.Quantity() == 400
	})
	if calls := failing.saveCalls(); calls != 2 {
		t.Errorf("Save called %d times, want 2", calls)
	}
	if n := fx.processedRows(t, "labor-capacity-consumer", eventID); n != 1 {
		t.Errorf("processed_events rows = %d, want exactly 1", n)
	}
	eventually(t, 30*time.Second, "offset committed past the message", func() bool {
		return committedOffset(brokers, groupID, topic) == 1
	})
}
