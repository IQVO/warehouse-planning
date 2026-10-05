package kafka_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	kafkaconsumer "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// testLogger returns a discard-everything logger so tests' expected WARN
// logs (malformed/unknown/untracked messages) don't spam `go test -v`
// output.
func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeProcessedEvents lets a test force
// ports.ProcessedEventRepository.Claim to fail, proving a consumer
// propagates the error instead of masking it.
type fakeProcessedEvents struct {
	err error
}

func (f fakeProcessedEvents) Claim(context.Context, string, string) (bool, error) {
	return false, f.err
}

// fakeFailingTally lets a test force ports.StorageTallyRepository to
// fail, proving StorageCapacityConsumer propagates the error instead of
// masking it.
type fakeFailingTally struct {
	err error
}

func (f fakeFailingTally) RegisterSlot(context.Context, string, string, string, []string) ([]tally.Update, error) {
	return nil, f.err
}

func (f fakeFailingTally) DecommissionSlot(context.Context, string) ([]tally.Update, bool, error) {
	return nil, false, f.err
}

// ---------------------------------------------------------------------
// Atomicity / at-least-once test doubles.
// ---------------------------------------------------------------------

var errInjected = errors.New("injected transient database failure")

// flakyPCRepo wraps the in-memory ProcessCapacityRepository and injects an
// infrastructure error into Save and/or Find, to simulate a transient DB
// failure on the SECOND step of a message (after the tally step already
// ran). passSaves lets that many Save calls succeed first; the next
// failSaves calls then fail; later ones succeed again. The embedded repo is
// still a memory.Snapshotter so the UnitOfWork can roll it back.
type flakyPCRepo struct {
	*memory.ProcessCapacityRepo
	mu        sync.Mutex
	passSaves int
	failSaves int
	failFinds int
	saveCalls int
}

func (f *flakyPCRepo) Save(ctx context.Context, pc *processcapacity.ProcessCapacity) error {
	f.mu.Lock()
	f.saveCalls++
	fail := false
	switch {
	case f.passSaves > 0:
		f.passSaves--
	case f.failSaves > 0:
		f.failSaves--
		fail = true
	}
	f.mu.Unlock()
	if fail {
		return errInjected
	}
	return f.ProcessCapacityRepo.Save(ctx, pc)
}

func (f *flakyPCRepo) FindByProcessLocationWindow(ctx context.Context, pt processcapacity.ProcessType, loc string, s, e time.Time) (*processcapacity.ProcessCapacity, error) {
	f.mu.Lock()
	fail := f.failFinds > 0
	if fail {
		f.failFinds--
	}
	f.mu.Unlock()
	if fail {
		return nil, errInjected
	}
	return f.ProcessCapacityRepo.FindByProcessLocationWindow(ctx, pt, loc, s, e)
}

func (f *flakyPCRepo) failSaveAfter(pass, fail int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.passSaves, f.failSaves = pass, fail
}

// countingUoW counts how often a unit of work was opened, proving a
// deterministic bad message never even touches the database.
type countingUoW struct {
	inner ports.UnitOfWork
	calls atomic.Int32
}

func (u *countingUoW) Do(ctx context.Context, fn func(context.Context) error) error {
	u.calls.Add(1)
	return u.inner.Do(ctx, fn)
}

// flakyTally wraps the in-memory tally and injects an infrastructure error
// AFTER the real mutation has been applied (failAfterMutation upcoming
// mutating calls apply their change and then return errInjected), simulating a
// transient database failure part-way through a message -- e.g. the second
// bucket's UPDATE of a work-center slot. The embedded repo is still a
// memory.Snapshotter so the UnitOfWork can roll the half-applied change back.
type flakyTally struct {
	*memory.StorageTallyRepo
	mu                sync.Mutex
	failAfterMutation int
	calls             int
}

// callCount is how many mutating calls (RegisterSlot/DecommissionSlot) were
// attempted, successful or not.
func (f *flakyTally) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *flakyTally) failNext(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failAfterMutation = n
}

func (f *flakyTally) shouldFail() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failAfterMutation > 0 {
		f.failAfterMutation--
		return true
	}
	return false
}

func (f *flakyTally) RegisterSlot(ctx context.Context, code, zone, typ string, keys []string) ([]tally.Update, error) {
	updates, err := f.StorageTallyRepo.RegisterSlot(ctx, code, zone, typ, keys)
	if err != nil {
		return nil, err
	}
	if f.shouldFail() {
		return nil, errInjected
	}
	return updates, nil
}

func (f *flakyTally) DecommissionSlot(ctx context.Context, code string) ([]tally.Update, bool, error) {
	updates, found, err := f.StorageTallyRepo.DecommissionSlot(ctx, code)
	if err != nil {
		return nil, false, err
	}
	if f.shouldFail() {
		return nil, false, errInjected
	}
	return updates, found, nil
}

type storageHarness struct {
	consumer  *kafkaconsumer.StorageCapacityConsumer
	processed *memory.ProcessedEventRepo
	tally     *memory.StorageTallyRepo // the real store behind flaky
	flaky     *flakyTally
	uow       *countingUoW
}

// newStorageHarness wires a StorageCapacityConsumer -- a pure tally
// maintainer -- over transactional in-memory doubles: the UnitOfWork
// snapshots and, on error, restores the tally AND the processed-event table.
func newStorageHarness() storageHarness {
	processed, tallyRepo := memory.NewProcessedEventRepo(), memory.NewStorageTallyRepo()
	flaky := &flakyTally{StorageTallyRepo: tallyRepo}
	uow := &countingUoW{inner: memory.NewUnitOfWork(tallyRepo, processed)}
	return storageHarness{
		consumer: &kafkaconsumer.StorageCapacityConsumer{
			Tally:           flaky,
			ProcessedEvents: processed,
			UoW:             uow,
			Logger:          testLogger(),
			Retry:           fastRetry,
		},
		processed: processed, tally: tallyRepo, flaky: flaky, uow: uow,
	}
}

type laborHarness struct {
	consumer  *kafkaconsumer.LaborCapacityConsumer
	pcs       *memory.ProcessCapacityRepo
	flaky     *flakyPCRepo
	processed *memory.ProcessedEventRepo
	uow       *countingUoW
}

func newLaborHarness() laborHarness {
	pcs, processed := memory.NewProcessCapacityRepo(), memory.NewProcessedEventRepo()
	flaky := &flakyPCRepo{ProcessCapacityRepo: pcs}
	uow := &countingUoW{inner: memory.NewUnitOfWork(pcs, processed)}
	return laborHarness{
		consumer: &kafkaconsumer.LaborCapacityConsumer{
			Register:        &usecases.RegisterProcessCapacityConstraint{Repo: flaky},
			ProcessedEvents: processed,
			UoW:             uow,
			Logger:          testLogger(),
			Retry:           fastRetry,
		},
		pcs: pcs, flaky: flaky, processed: processed, uow: uow,
	}
}

// fastRetry keeps Run-loop tests quick while still exercising backoff.
var fastRetry = kafkaconsumer.RetryPolicy{Initial: time.Millisecond, Max: 4 * time.Millisecond}

// fakeReader is a scripted kafkaconsumer.Reader. It records every
// FetchMessage/CommitMessages in order so a test can assert that the
// offset was committed only after the handler succeeded, and exactly once.
// After the scripted messages it blocks until ctx is cancelled, like a
// real reader on an idle topic.
type fakeReader struct {
	mu       sync.Mutex
	msgs     []kafkago.Message
	next     int
	events   []string
	commitFn func(kafkago.Message) // called (unlocked) after each commit
}

func newFakeReader(values ...[]byte) *fakeReader {
	r := &fakeReader{}
	for i, v := range values {
		r.msgs = append(r.msgs, kafkago.Message{Partition: 0, Offset: int64(i), Value: v})
	}
	return r
}

func (r *fakeReader) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	r.mu.Lock()
	if r.next < len(r.msgs) {
		m := r.msgs[r.next]
		r.next++
		r.events = append(r.events, fmt.Sprintf("fetch:%d", m.Offset))
		r.mu.Unlock()
		return m, nil
	}
	r.mu.Unlock()
	<-ctx.Done()
	return kafkago.Message{}, ctx.Err()
}

func (r *fakeReader) CommitMessages(_ context.Context, msgs ...kafkago.Message) error {
	r.mu.Lock()
	for _, m := range msgs {
		r.events = append(r.events, fmt.Sprintf("commit:%d", m.Offset))
	}
	fn := r.commitFn
	r.mu.Unlock()
	if fn != nil {
		for _, m := range msgs {
			fn(m)
		}
	}
	return nil
}

func (r *fakeReader) Close() error { return nil }

func (r *fakeReader) log() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.events...)
}

// fakeDLQWriter records every message WriteMessages publishes; errs[0]
// is consumed (and popped) on each call before falling through to a nil
// return, so a test can script a transient DLQ-write failure followed by
// success.
type fakeDLQWriter struct {
	mu     sync.Mutex
	msgs   []kafkago.Message
	errs   []error
	closed bool
}

func (w *fakeDLQWriter) WriteMessages(_ context.Context, msgs ...kafkago.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if len(w.errs) > 0 {
		err, w.errs = w.errs[0], w.errs[1:]
	}
	if err != nil {
		return err
	}
	w.msgs = append(w.msgs, msgs...)
	return nil
}

func (w *fakeDLQWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	return nil
}

func (w *fakeDLQWriter) published() []kafkago.Message {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]kafkago.Message(nil), w.msgs...)
}
