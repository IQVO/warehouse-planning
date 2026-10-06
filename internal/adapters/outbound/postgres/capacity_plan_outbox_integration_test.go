//go:build integration

package postgres_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

var (
	planStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	planEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)

	typePrefix    = "com.warehouse.wes.warehouse-planning.capacityplan."
	typeCreated   = typePrefix + "CapacityPlanCreated"
	typePublished = typePrefix + "CapacityPlanPublished"
	typeShortage  = typePrefix + "CapacityShortageDetected"
	typeBottle    = typePrefix + "BottleneckDetected"
)

// planStack is the Phase 4 use cases wired over the REAL Postgres
// adapters (one pool, one UnitOfWork).
type planStack struct {
	pool    *pgxpool.Pool
	plans   *postgres.CapacityPlanRepo
	outbox  *postgres.OutboxRepo
	create  *usecases.CreateCapacityPlan
	publish *usecases.PublishCapacityPlan
}

func newPlanStack(t *testing.T) *planStack {
	t.Helper()
	pool := newMigratedPool(t)
	pcs, paths := postgres.NewProcessCapacityRepo(pool), memory.NewProcessPathRepo()
	plans, ob := postgres.NewCapacityPlanRepo(pool), postgres.NewOutboxRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	enc := outboundkafka.NewEncoder()

	register := &usecases.RegisterProcessCapacityConstraint{Repo: pcs}
	for _, reg := range []struct {
		process processcapacity.ProcessType
		qty     float64
		unit    processcapacity.CapacityUnit
	}{{"PICK", 4000, processcapacity.UnitUnit}, {"REBIN", 2500, processcapacity.UnitUnit}, {"PACK", 1800, processcapacity.UnitPackage}} {
		if _, err := register.Handle(context.Background(), usecases.RegisterProcessCapacityConstraintCommand{
			ProcessType: reg.process, Location: "PATH-ZONE-A", WindowStart: planStart, WindowEnd: planEnd,
			ConstraintType: processcapacity.ConstraintLabor, Quantity: reg.qty, Unit: reg.unit, Period: time.Hour,
		}); err != nil {
			t.Fatalf("register %s: %v", reg.process, err)
		}
	}
	if _, err := (&usecases.RegisterProcessPath{Repo: paths}).Handle(context.Background(), usecases.RegisterProcessPathCommand{
		ID: "pick-rebin-pack", Name: "Pick-Rebin-Pack", Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"},
	}); err != nil {
		t.Fatalf("register path: %v", err)
	}

	s := &planStack{pool: pool, plans: plans, outbox: ob}
	s.create = &usecases.CreateCapacityPlan{
		PathCapacity: &usecases.GetProcessPathCapacity{ProcessPaths: paths, ProcessCapacities: pcs, StationStandards: postgres.NewStationStandardRepo(pool), Tally: postgres.NewStorageTallyRepo(pool)},
		Plans:        plans, Outbox: ob, Encoder: enc, UnitOfWork: uow,
	}
	s.publish = &usecases.PublishCapacityPlan{Plans: plans, Outbox: ob, Encoder: enc, UnitOfWork: uow}
	return s
}

func planCmd(demand float64) usecases.CreateCapacityPlanCommand {
	upo, ppo := 2.5, 1.0
	return usecases.CreateCapacityPlanCommand{
		WarehouseID: "WH-1", SiteID: "SIM1", Location: "PATH-ZONE-A", WindowStart: planStart, WindowEnd: planEnd,
		ProcessPathID: "pick-rebin-pack", AssignedDemand: demand, UnitsPerOrder: &upo, PackagesPerOrder: &ppo,
	}
}

func count(t *testing.T, pool *pgxpool.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	return n
}

func eventTypes(t *testing.T, pool *pgxpool.Pool) []string {
	t.Helper()
	rows, err := pool.Query(context.Background(), `SELECT event_type FROM outbox_events ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCapacityPlanRepo_Postgres_RoundTrip(t *testing.T) {
	s := newPlanStack(t)
	ctx := context.Background()

	if got, err := s.plans.FindByID(ctx, "missing"); got != nil || err != nil {
		t.Fatalf("miss = %v, %v; want nil, nil", got, err)
	}

	plan, err := s.create.Handle(ctx, planCmd(12000))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	got, err := s.plans.FindByID(ctx, plan.ID())
	if err != nil || got == nil {
		t.Fatalf("Find = %v, %v", got, err)
	}
	if got.ID() != plan.ID() || got.WarehouseID() != "WH-1" || got.SiteID() != "SIM1" || got.Location() != "PATH-ZONE-A" || got.ProcessPathID() != "pick-rebin-pack" ||
		got.AssignedDemand() != 12000 || got.PathCapacity() != 1000 || got.BottleneckStep() != "REBIN" ||
		got.CapacityOverWindow() != 8000 || got.Shortage() != 4000 || got.Status() != capacityplan.StatusDraft {
		t.Errorf("round trip lost state: %+v", got)
	}
	if !got.Window().Start().Equal(planStart) || !got.Window().End().Equal(planEnd) || got.Window().Start().Location() != time.UTC {
		t.Errorf("window = %v..%v", got.Window().Start(), got.Window().End())
	}
	if !got.PublishedAt().IsZero() {
		t.Errorf("DRAFT plan has PublishedAt %v", got.PublishedAt())
	}

	published, err := s.publish.Handle(ctx, plan.ID())
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	got, _ = s.plans.FindByID(ctx, plan.ID())
	if got.Status() != capacityplan.StatusPublished || !got.PublishedAt().Equal(published.PublishedAt()) || got.PublishedAt().IsZero() {
		t.Errorf("published state = %s at %v", got.Status(), got.PublishedAt())
	}
}

// The section-43 shortage plan: create + publish queue exactly the four
// events, with the FULL CloudEvents type in event_type, in order.
func TestCapacityPlan_Postgres_CreateAndPublishQueueFourEvents(t *testing.T) {
	s := newPlanStack(t)
	ctx := context.Background()
	plan, err := s.create.Handle(ctx, planCmd(12000))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if got := eventTypes(t, s.pool); !equalStrings(got, []string{typeCreated}) {
		t.Fatalf("after create: %v", got)
	}
	if _, err := s.publish.Handle(ctx, plan.ID()); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if got := eventTypes(t, s.pool); !equalStrings(got, []string{typeCreated, typePublished, typeShortage, typeBottle}) {
		t.Fatalf("after publish: %v", got)
	}
	if n := count(t, s.pool, `SELECT count(*) FROM outbox_events WHERE subject = $1 AND published_at IS NULL AND topic = 'warehouse.warehouse-planning.events'`, plan.ID()); n != 4 {
		t.Errorf("unpublished rows for the plan = %d, want 4", n)
	}
	if _, err := s.publish.Handle(ctx, plan.ID()); !errors.Is(err, capacityplan.ErrAlreadyPublished) {
		t.Fatalf("second publish err = %v", err)
	}
	if n := count(t, s.pool, `SELECT count(*) FROM outbox_events`); n != 4 {
		t.Errorf("a rejected second publish left %d outbox rows, want 4", n)
	}

	// Within capacity: only Created + Published.
	s2 := newPlanStack(t)
	p2, _ := s2.create.Handle(ctx, planCmd(6000))
	if _, err := s2.publish.Handle(ctx, p2.ID()); err != nil {
		t.Fatal(err)
	}
	if got := eventTypes(t, s2.pool); !equalStrings(got, []string{typeCreated, typePublished}) {
		t.Fatalf("within capacity: %v", got)
	}
}

// savesThenFails performs the REAL write and then reports failure, so the
// tests prove the write is rolled back with the rest of the unit of work
// instead of merely never having happened.
type plansThenFail struct {
	ports.CapacityPlanRepository
	err error
}

func (p plansThenFail) Save(ctx context.Context, plan *capacityplan.CapacityPlan) error {
	if err := p.CapacityPlanRepository.Save(ctx, plan); err != nil {
		return err
	}
	return p.err
}

type outboxThenFail struct {
	ports.OutboxRepository
	err error
}

func (o outboxThenFail) Insert(ctx context.Context, msgs ...outbox.Message) error {
	if err := o.OutboxRepository.Insert(ctx, msgs...); err != nil {
		return err
	}
	return o.err
}

// TestAtomicity proves the plan and its outbox rows commit or roll back
// together on the real Postgres UnitOfWork, in both directions.
func TestAtomicity_PlanAndOutboxCommitOrRollBackTogether(t *testing.T) {
	boom := errors.New("injected failure after the real write")
	ctx := context.Background()

	t.Run("create: plan save fails -> no outbox row", func(t *testing.T) {
		s := newPlanStack(t)
		s.create.Plans = plansThenFail{s.plans, boom}
		if _, err := s.create.Handle(ctx, planCmd(12000)); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if p, o := count(t, s.pool, `SELECT count(*) FROM capacity_plans`), count(t, s.pool, `SELECT count(*) FROM outbox_events`); p != 0 || o != 0 {
			t.Errorf("plans = %d, outbox = %d; want 0, 0", p, o)
		}
	})

	t.Run("create: outbox insert fails -> no plan row", func(t *testing.T) {
		s := newPlanStack(t)
		s.create.Outbox = outboxThenFail{s.outbox, boom}
		if _, err := s.create.Handle(ctx, planCmd(12000)); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		if p, o := count(t, s.pool, `SELECT count(*) FROM capacity_plans`), count(t, s.pool, `SELECT count(*) FROM outbox_events`); p != 0 || o != 0 {
			t.Errorf("plans = %d, outbox = %d; want 0, 0", p, o)
		}
	})

	t.Run("publish: outbox insert fails -> plan stays DRAFT, no event rows", func(t *testing.T) {
		s := newPlanStack(t)
		plan, err := s.create.Handle(ctx, planCmd(12000))
		if err != nil {
			t.Fatal(err)
		}
		s.publish.Outbox = outboxThenFail{s.outbox, boom}
		if _, err := s.publish.Handle(ctx, plan.ID()); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		got, _ := s.plans.FindByID(ctx, plan.ID())
		if got.Status() != capacityplan.StatusDraft || !got.PublishedAt().IsZero() {
			t.Errorf("status = %s (published at %v); want DRAFT", got.Status(), got.PublishedAt())
		}
		if types := eventTypes(t, s.pool); !equalStrings(types, []string{typeCreated}) {
			t.Errorf("outbox = %v, want only the Created row", types)
		}
	})

	t.Run("publish: plan save fails -> no event rows", func(t *testing.T) {
		s := newPlanStack(t)
		plan, _ := s.create.Handle(ctx, planCmd(12000))
		s.publish.Plans = plansThenFail{s.plans, boom}
		if _, err := s.publish.Handle(ctx, plan.ID()); !errors.Is(err, boom) {
			t.Fatalf("err = %v", err)
		}
		got, _ := s.plans.FindByID(ctx, plan.ID())
		if got.Status() != capacityplan.StatusDraft {
			t.Errorf("status = %s, want DRAFT", got.Status())
		}
		if n := count(t, s.pool, `SELECT count(*) FROM outbox_events`); n != 1 {
			t.Errorf("outbox rows = %d, want 1", n)
		}
	})
}

// pausesAfterFind lets a test hold one publish transaction open right after
// it read the plan, so a second publish provably overlaps it.
type pausesAfterFind struct {
	ports.CapacityPlanRepository
	found   chan struct{}
	release chan struct{}
}

func (p pausesAfterFind) FindByID(ctx context.Context, id string) (*capacityplan.CapacityPlan, error) {
	plan, err := p.CapacityPlanRepository.FindByID(ctx, id)
	close(p.found)
	<-p.release
	return plan, err
}

// Two overlapping publishes of the same DRAFT plan: publish A reads the
// plan and is held open; publish B starts meanwhile. FindByID's FOR UPDATE
// makes B wait for A's commit and then see PUBLISHED, so exactly one wins
// and the events are queued once. (Without the row lock both would read
// DRAFT and both would publish.)
func TestPublish_Postgres_OverlappingPublishesPublishOnce(t *testing.T) {
	s := newPlanStack(t)
	ctx := context.Background()
	plan, err := s.create.Handle(ctx, planCmd(12000))
	if err != nil {
		t.Fatal(err)
	}

	pause := pausesAfterFind{CapacityPlanRepository: s.plans, found: make(chan struct{}), release: make(chan struct{})}
	publishA := *s.publish
	publishA.Plans = pause

	var wg sync.WaitGroup
	var errA, errB error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errA = publishA.Handle(ctx, plan.ID())
	}()
	<-pause.found // A holds its transaction open after reading DRAFT

	wg.Add(1)
	go func() {
		defer wg.Done()
		_, errB = s.publish.Handle(ctx, plan.ID())
	}()
	time.Sleep(500 * time.Millisecond) // B is now blocked on the row lock (or, without it, has already published)
	close(pause.release)
	wg.Wait()

	if errA != nil {
		t.Errorf("publish A (first to read) err = %v, want success", errA)
	}
	if !errors.Is(errB, capacityplan.ErrAlreadyPublished) {
		t.Errorf("publish B err = %v, want ErrAlreadyPublished", errB)
	}
	if got := eventTypes(t, s.pool); !equalStrings(got, []string{typeCreated, typePublished, typeShortage, typeBottle}) {
		t.Fatalf("outbox = %v, want each event exactly once", got)
	}
}

func TestOutbox_Postgres_InsertRoundTripAndDrain(t *testing.T) {
	pool := newMigratedPool(t)
	ob := postgres.NewOutboxRepo(pool)
	ctx := context.Background()

	msgs := []outbox.Message{
		{EventID: "e1", Topic: "t", EventType: typeCreated, Subject: "p1", Key: []byte("p1"), DataSchema: "urn:a", Value: []byte(`{"n":1}`),
			Headers: []outbox.Header{{Key: "content-type", Value: "application/cloudevents+json; charset=UTF-8"}}},
		{EventID: "e2", Topic: "t", EventType: typePublished, Subject: "p1", Key: []byte("p1"), DataSchema: "urn:b", Value: []byte(`{"n":2}`)},
		{EventID: "e3", Topic: "t", EventType: typeShortage, Subject: "p1", Key: []byte("p1"), DataSchema: "urn:c", Value: []byte(`{"n":3}`)},
	}
	if err := ob.Insert(ctx); err != nil {
		t.Fatalf("empty insert: %v", err)
	}
	if err := ob.Insert(ctx, msgs...); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// A duplicate event_id is rejected (the id is the dedupe key).
	if err := ob.Insert(ctx, msgs[0]); err == nil {
		t.Fatal("duplicate event_id accepted")
	}

	// Pass 1: e1 sends, e2 fails -> e1 is marked published, e2 records the error, e3 untouched.
	boom := errors.New("broker down")
	var sent []string
	n, err := ob.Drain(ctx, 10, func(_ context.Context, m outbox.Message) error {
		if m.EventID == "e2" {
			return boom
		}
		sent = append(sent, m.EventID)
		return nil
	})
	if !errors.Is(err, boom) || n != 1 || !equalStrings(sent, []string{"e1"}) {
		t.Fatalf("pass 1 = %d, %v, sent %v", n, err, sent)
	}
	var lastErr *string
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT last_error, attempts FROM outbox_events WHERE event_id = 'e2'`).Scan(&lastErr, &attempts); err != nil {
		t.Fatal(err)
	}
	if lastErr == nil || attempts != 1 {
		t.Errorf("e2 last_error = %v attempts = %d", lastErr, attempts)
	}

	// Pass 2 (broker back): e2 is republished with the same id and content, then e3.
	var second []outbox.Message
	n, err = ob.Drain(ctx, 10, func(_ context.Context, m outbox.Message) error {
		second = append(second, m)
		return nil
	})
	if err != nil || n != 2 || second[0].EventID != "e2" || second[1].EventID != "e3" {
		t.Fatalf("pass 2 = %d, %v, %+v", n, err, second)
	}
	if string(second[0].Value) != `{"n":2}` || second[0].Topic != "t" || second[0].EventType != typePublished || second[0].Subject != "p1" || second[0].DataSchema != "urn:b" || string(second[0].Key) != "p1" {
		t.Errorf("e2 changed on retry: %+v", second[0])
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); n != 0 {
		t.Errorf("%d rows still unpublished", n)
	}

	// Headers survive the JSONB round trip.
	n, err = ob.Drain(ctx, 10, func(context.Context, outbox.Message) error { t.Error("published rows were drained again"); return nil })
	if err != nil || n != 0 {
		t.Fatalf("empty drain = %d, %v", n, err)
	}
	var headers []byte
	if err := pool.QueryRow(ctx, `SELECT headers FROM outbox_events WHERE event_id = 'e1'`).Scan(&headers); err != nil {
		t.Fatal(err)
	}
	if string(headers) != `[{"key": "content-type", "value": "application/cloudevents+json; charset=UTF-8"}]` {
		t.Errorf("headers = %s", headers)
	}
}

func TestOutbox_Postgres_DrainRespectsBatchLimitAndSkipsLockedRows(t *testing.T) {
	pool := newMigratedPool(t)
	ob := postgres.NewOutboxRepo(pool)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d"} {
		if err := ob.Insert(ctx, outbox.Message{EventID: id, Topic: "t", EventType: "T", Subject: "s", DataSchema: "u", Value: []byte("{}")}); err != nil {
			t.Fatal(err)
		}
	}

	// A first relay is mid-pass (holding the claimed rows). A second relay
	// must not re-claim them (SKIP LOCKED): it gets nothing, not a duplicate.
	holding, release := make(chan struct{}), make(chan struct{})
	done := make(chan []string, 1)
	go func() {
		var ids []string
		_, _ = ob.Drain(ctx, 2, func(_ context.Context, m outbox.Message) error {
			ids = append(ids, m.EventID)
			if len(ids) == 1 {
				close(holding)
				<-release
			}
			return nil
		})
		done <- ids
	}()
	<-holding
	var other []string
	n, err := ob.Drain(ctx, 10, func(_ context.Context, m outbox.Message) error {
		other = append(other, m.EventID)
		return nil
	})
	if err != nil || n != 2 || !equalStrings(other, []string{"c", "d"}) {
		t.Fatalf("concurrent relay got %d %v (%v); want the 2 rows the first relay did not claim", n, other, err)
	}
	close(release)
	if first := <-done; !equalStrings(first, []string{"a", "b"}) {
		t.Fatalf("first relay sent %v, want [a b] (batch limit 2)", first)
	}
	if n := count(t, pool, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); n != 0 {
		t.Errorf("%d unpublished rows left", n)
	}
}
