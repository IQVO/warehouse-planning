//go:build integration

// Real-router proofs of the replay-class rule over real Postgres: a RETRYABLE
// refusal is NOT stored (its key row is absent), the retry under the same key
// executes, and its success is then stored and replayed. Each proof drives a
// genuine problem type of a keyed route through the production router:
//
//   - version-conflict        (POST /capacity-plans/{id}/publications)
//   - process-path-not-found  (POST /capacity-plans)
//   - missing-assigned-demand (POST /capacity-plans)
//
// version-conflict is driven two ways: through a Plans decorator that returns
// ports.ErrVersionConflict from Save (the typed mapping of the
// capacity_plans_one_published_per_key unique violation) without aborting the
// statement's transaction, and through one that loses the race FOR REAL -- a
// conflicting PUBLISHED row inserted on the joined transaction, so the
// statement fails 23505 and ABORTS it, exactly what a concurrent publish
// committing first does. The use case's internal rerun must then execute on a
// fresh statement context (the joined unit of work rolls the failed attempt
// back to its entry savepoint, healing the shared transaction), so a second
// loss surfaces as the 409 version-conflict refusal -- never a 25P02-driven
// 500 -- unstored and with the key released.
package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres/pgtx"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// replayProofStack is the production wiring of newPostgresRouterWithPaths plus
// the handles the proofs need: the seeded flow repo, the pool (key-row counts,
// order_demand seeding, published-plan seeding) and the Plans repo decorator.
type replayProofStack struct {
	srv   *httptest.Server
	flows *memory.ProcessFlowRepo
	pool  *pgxpool.Pool
	plans raceLoser
}

// raceLoser is the behaviour both Save decorators share: arm the next n Save
// calls to lose the one-published-per-key race.
type raceLoser interface {
	ports.CapacityPlanRepository
	arm(n int)
}

// newReplayProofStack wires the stack with the typed-error conflict decorator.
func newReplayProofStack(t *testing.T) *replayProofStack {
	t.Helper()
	return newProofStack(t, func(raw ports.CapacityPlanRepository) raceLoser {
		return newConflictLosingPlans(raw)
	})
}

// newPoisonProofStack wires the stack with the decorator that loses the race
// FOR REAL (a genuine 23505 aborting the joined transaction).
func newPoisonProofStack(t *testing.T) *replayProofStack {
	t.Helper()
	return newProofStack(t, func(raw ports.CapacityPlanRepository) raceLoser {
		return newRacePoisoningPlans(raw)
	})
}

// newProofStack wires the REAL router over one private migrated database, with
// the capacity-plan repository the use cases see wrapped by wrap (a race
// decorator, disabled until armed; reads keep the raw repo).
func newProofStack(t *testing.T, wrap func(ports.CapacityPlanRepository) raceLoser) *replayProofStack {
	t.Helper()
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, httpMigratedDB(t))
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)

	pcs, flows := postgres.NewProcessCapacityRepo(pool), memory.NewProcessFlowRepo()
	standards, tally := postgres.NewStationStandardRepo(pool), postgres.NewStorageTallyRepo(pool)
	rawPlans := postgres.NewCapacityPlanRepo(pool)
	plans := wrap(rawPlans)
	ob := postgres.NewOutboxRepo(pool)
	uow := postgres.NewUnitOfWork(pool)
	encoder := outboundkafka.NewFanoutEncoder()
	pathCapacity := &usecases.GetProcessPathCapacity{
		ProcessPaths: &usecases.FlowPathFinder{Flows: flows}, ProcessCapacities: pcs, StationStandards: standards, Tally: tally,
	}
	expectedDemand := &usecases.GetExpectedDemand{Demand: postgres.NewOrderDemandRepo(pool)}
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: pcs},
		ProcessCapacities:                 pcs,
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: plans, Outbox: ob,
			Encoder: encoder, UnitOfWork: uow, Demand: expectedDemand,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{
			Plans: plans, Outbox: ob, Encoder: encoder, UnitOfWork: uow,
		},
		CapacityPlans:          rawPlans,
		DeclareStationStandard: &usecases.DeclareStationStandard{Repo: standards},
		StationStandards:       standards,
		GetStorageCapacity:     &usecases.GetStorageCapacity{Tally: tally},
		GetExpectedDemand:      expectedDemand,
		ListProcessPaths:       &usecases.ListProcessPaths{Paths: &usecases.FlowPathLister{Flows: flows}},
		ListCapacityPlans:      &usecases.ListCapacityPlans{Plans: rawPlans},

		UnitOfWork:  uow,
		Idempotency: postgres.NewIdempotencyStore(pool),
	}
	srv := httptest.NewServer(inboundhttp.NewRouter(s))
	t.Cleanup(srv.Close)
	return &replayProofStack{srv: srv, flows: flows, pool: pool, plans: plans}
}

// conflictLosingPlans wraps ports.CapacityPlanRepository and fails the next N
// Save calls with ports.ErrVersionConflict (the typed mapping of the
// capacity_plans_one_published_per_key unique violation) without touching the
// transaction, so the joined unit of work stays alive and the 409 path runs.
type conflictLosingPlans struct {
	ports.CapacityPlanRepository
	losses atomic.Int32
}

func newConflictLosingPlans(inner ports.CapacityPlanRepository) *conflictLosingPlans {
	return &conflictLosingPlans{CapacityPlanRepository: inner}
}

// arm makes the next n Save calls lose the race.
func (p *conflictLosingPlans) arm(n int) { p.losses.Store(int32(n)) }

func (p *conflictLosingPlans) Save(ctx context.Context, plan *capacityplan.CapacityPlan) error {
	if p.losses.Add(-1) >= 0 {
		return ports.ErrVersionConflict
	}
	return p.CapacityPlanRepository.Save(ctx, plan)
}

// racePoisoningPlans wraps ports.CapacityPlanRepository and, while armed,
// makes Save lose the one-published-per-key race FOR REAL: it inserts a
// conflicting PUBLISHED clone of the plan's own row on the transaction the
// ctx carries, so the statement fails 23505 on
// capacity_plans_one_published_per_key and ABORTS the joined unit of work --
// exactly what a concurrent publish of the key committing first does (the
// conflictLosingPlans decorator returns the typed error without the abort).
// The poison row rolls back with the unit of work; a committed PUBLISHED plan
// of the key must exist (seedPublishedClone), like the race's winner.
type racePoisoningPlans struct {
	ports.CapacityPlanRepository
	losses atomic.Int32
}

func newRacePoisoningPlans(inner ports.CapacityPlanRepository) *racePoisoningPlans {
	return &racePoisoningPlans{CapacityPlanRepository: inner}
}

// arm makes the next n Save calls lose the race.
func (p *racePoisoningPlans) arm(n int) { p.losses.Store(int32(n)) }

func (p *racePoisoningPlans) Save(ctx context.Context, plan *capacityplan.CapacityPlan) error {
	if p.losses.Add(-1) >= 0 {
		tx, ok := pgtx.From(ctx)
		if !ok {
			return errors.New("racePoisoningPlans: Save outside a unit of work")
		}
		if _, err := tx.Exec(ctx, cloneAsPublishedSQL, "poison-"+plan.ID(), plan.ID()); err == nil {
			return errors.New("racePoisoningPlans: the poison insert succeeded, no PUBLISHED plan of the key")
		}
		return ports.ErrVersionConflict
	}
	return p.CapacityPlanRepository.Save(ctx, plan)
}

// cloneAsPublishedSQL inserts a PUBLISHED clone of the plan $2 under id $1 --
// the race winner's row seeded outside the request, and the poison row the
// armed decorator inserts on the joined transaction (genuine 23505).
const cloneAsPublishedSQL = `
	INSERT INTO capacity_plans (
		id, warehouse_id, site_id, location, window_start, window_end, process_path_id,
		assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage,
		status, created_at, published_at, bottleneck_constraint, warnings, demand_source,
		inbound_appointments, inbound_labour_minutes, inbound_standard_missing, labour_requirement,
		forecast_path_id, scenario, superseded_by)
	SELECT $1, warehouse_id, site_id, location, window_start, window_end, process_path_id,
		assigned_demand, path_capacity, bottleneck_step, capacity_over_window, shortage,
		'PUBLISHED', created_at, now(), bottleneck_constraint, warnings, demand_source,
		inbound_appointments, inbound_labour_minutes, inbound_standard_missing, labour_requirement,
		forecast_path_id, scenario, NULL
	FROM capacity_plans WHERE id = $2`

// seedPublishedClone stores a PUBLISHED copy of the plan under a new id: the
// winner of the one-published-per-key race, committed before the request.
func seedPublishedClone(t *testing.T, pool *pgxpool.Pool, planID, winnerID string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(), cloneAsPublishedSQL, winnerID, planID); err != nil {
		t.Fatalf("seed published clone: %v", err)
	}
}

// planStatus reads one plan's stored status from outside any request.
func planStatus(t *testing.T, pool *pgxpool.Pool, planID string) string {
	t.Helper()
	var status string
	if err := pool.QueryRow(context.Background(), `SELECT status FROM capacity_plans WHERE id = $1`, planID).Scan(&status); err != nil {
		t.Fatalf("read status of %s: %v", planID, err)
	}
	return status
}

// seedActiveFlow registers an ACTIVE single-step flow under id (a
// process-path-management event this service consumed, ADR 0025).
func seedActiveFlow(t *testing.T, flows *memory.ProcessFlowRepo, id string) {
	t.Helper()
	f, err := processflow.New(processflow.Params{
		FlowID: id, Name: "proof-" + id,
		Stages: []processflow.Stage{{PathID: "PICK", ProcessType: processflow.Pick}},
		Status: processflow.StatusActive, Version: 1, UpdatedAt: time.Unix(0, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flows.Record(context.Background(), f); err != nil {
		t.Fatal(err)
	}
}

// registerStepCapacity registers a UNIT/HOUR LABOR constraint for process at
// location over [ws, we) through the real keyed route.
func registerStepCapacity(t *testing.T, srv *httptest.Server, key, process, location, ws, we string) {
	t.Helper()
	body := fmt.Sprintf(`{"process_type":%q,"location":%q,"window_start":%q,"window_end":%q,"constraint_type":"LABOR","quantity":120,"unit":"UNIT","period_seconds":3600}`,
		process, location, ws, we)
	status, _, resp := proofPost(t, srv, "/process-capacities", key, body)
	if status != http.StatusCreated {
		t.Fatalf("register capacity: %d %s", status, resp)
	}
}

// proofPost sends one keyed POST and returns status, headers and body.
func proofPost(t *testing.T, srv *httptest.Server, path, key, body string) (int, http.Header, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", key)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, resp.Header, string(raw)
}

// keyRows counts idempotency_keys rows from outside any request.
func keyRows(t *testing.T, pool *pgxpool.Pool, key string) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM idempotency_keys WHERE key = $1`, key).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// TestVersionConflictRefusalIsNotStoredSoItsKeyRetriesTheWork: the publish
// route loses the one-published-per-key race twice (the use case retries once
// internally), answers 409 version-conflict, and NOTHING is stored under the
// key; the retry under the same key executes the real publish, which is then
// stored and replayed.
func TestVersionConflictRefusalIsNotStoredSoItsKeyRetriesTheWork(t *testing.T) {
	st := newReplayProofStack(t)
	seedActiveFlow(t, st.flows, "CONFLICT-PROOF")
	registerStepCapacity(t, st.srv, "proof-cap-1", "PICK", "CONFLICTLOC", "2027-03-01T08:00:00Z", "2027-03-01T16:00:00Z")

	create := `{"warehouse_id":"WH-1","site_id":"SIM1","location":"CONFLICTLOC","window_start":"2027-03-01T08:00:00Z","window_end":"2027-03-01T16:00:00Z","path_id":"CONFLICT-PROOF","assigned_demand":100,"units_per_order":1}`
	status, _, body := proofPost(t, st.srv, "/capacity-plans", "proof-create-1", create)
	if status != http.StatusCreated {
		t.Fatalf("create plan: %d %s", status, body)
	}
	var plan struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &plan); err != nil || plan.ID == "" {
		t.Fatalf("create response: %v %s", err, body)
	}

	st.plans.arm(2) // both publish attempts lose the one-published-per-key race
	refused, _, rbody := proofPost(t, st.srv, "/capacity-plans/"+plan.ID+"/publications", "proof-pub-1", "{}")
	if refused != http.StatusConflict || !strings.Contains(rbody, "/version-conflict") {
		t.Fatalf("first publish = %d %s, want 409 version-conflict", refused, rbody)
	}
	if n := keyRows(t, st.pool, "proof-pub-1"); n != 0 {
		t.Fatalf("key rows after the RETRYABLE 409 = %d, want 0: the key must be released", n)
	}

	retry, _, rbody2 := proofPost(t, st.srv, "/capacity-plans/"+plan.ID+"/publications", "proof-pub-1", "{}")
	if retry != http.StatusOK {
		t.Fatalf("retry under the same key = %d %s, want the real publish to execute (200)", retry, rbody2)
	}
	if n := keyRows(t, st.pool, "proof-pub-1"); n != 1 {
		t.Fatalf("key rows after the successful retry = %d, want 1: a 2xx is stored", n)
	}
	replay, h, rbody3 := proofPost(t, st.srv, "/capacity-plans/"+plan.ID+"/publications", "proof-pub-1", "{}")
	if replay != http.StatusOK || h.Get("Idempotency-Replayed") != "true" || rbody3 != rbody2 {
		t.Fatalf("replay = %d replayed=%q, want the stored 200 replayed", replay, h.Get("Idempotency-Replayed"))
	}
}

// TestGenuineUniqueIndexLossAnswersTheVersionConflictRefusal: the publish
// route loses the one-published-per-key race FOR REAL twice -- a committed
// winner holds the key's PUBLISHED slot and each armed Save inserts a
// conflicting PUBLISHED row on the joined transaction, a genuine 23505 that
// ABORTS it. The use case's internal rerun must execute on a fresh statement
// context, not the aborted transaction, so the second loss surfaces as the
// 409 version-conflict refusal (a 500 here is the rerun dying 25P02 on the
// aborted transaction): nothing is stored under the key, nothing of the
// aborted attempts commits, and the retry under the same key executes the
// real publish (superseding the winner), which is stored and replayed.
func TestGenuineUniqueIndexLossAnswersTheVersionConflictRefusal(t *testing.T) {
	st := newPoisonProofStack(t)
	seedActiveFlow(t, st.flows, "POISON-PROOF")
	registerStepCapacity(t, st.srv, "proof-pcap-1", "PICK", "POISONLOC", "2027-04-01T08:00:00Z", "2027-04-01T16:00:00Z")

	create := `{"warehouse_id":"WH-1","site_id":"SIM1","location":"POISONLOC","window_start":"2027-04-01T08:00:00Z","window_end":"2027-04-01T16:00:00Z","path_id":"POISON-PROOF","assigned_demand":100,"units_per_order":1}`
	status, _, body := proofPost(t, st.srv, "/capacity-plans", "proof-pcreate-1", create)
	if status != http.StatusCreated {
		t.Fatalf("create plan: %d %s", status, body)
	}
	var plan struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &plan); err != nil || plan.ID == "" {
		t.Fatalf("create response: %v %s", err, body)
	}
	winnerID := "winner-" + plan.ID
	seedPublishedClone(t, st.pool, plan.ID, winnerID)

	st.plans.arm(2) // both publish attempts lose the one-published-per-key race FOR REAL
	refused, _, rbody := proofPost(t, st.srv, "/capacity-plans/"+plan.ID+"/publications", "proof-ppub-1", "{}")
	if refused != http.StatusConflict || !strings.Contains(rbody, "/version-conflict") {
		t.Fatalf("first publish = %d %s, want 409 version-conflict (a 500 is the rerun dying 25P02 on the aborted transaction)", refused, rbody)
	}
	if n := keyRows(t, st.pool, "proof-ppub-1"); n != 0 {
		t.Fatalf("key rows after the RETRYABLE 409 = %d, want 0: the key must be released", n)
	}
	if got := planStatus(t, st.pool, plan.ID); got != "DRAFT" {
		t.Fatalf("plan after the refusal = %s, want DRAFT: nothing of the aborted attempts may commit", got)
	}
	if got := planStatus(t, st.pool, winnerID); got != "PUBLISHED" {
		t.Fatalf("winner after the refusal = %s, want still PUBLISHED", got)
	}

	retry, _, rbody2 := proofPost(t, st.srv, "/capacity-plans/"+plan.ID+"/publications", "proof-ppub-1", "{}")
	if retry != http.StatusOK {
		t.Fatalf("retry under the same key = %d %s, want the real publish to execute (200)", retry, rbody2)
	}
	if n := keyRows(t, st.pool, "proof-ppub-1"); n != 1 {
		t.Fatalf("key rows after the successful retry = %d, want 1: a 2xx is stored", n)
	}
	if got := planStatus(t, st.pool, plan.ID); got != "PUBLISHED" {
		t.Fatalf("plan after the retry = %s, want PUBLISHED", got)
	}
	if got := planStatus(t, st.pool, winnerID); got != "SUPERSEDED" {
		t.Fatalf("winner after the retry = %s, want SUPERSEDED by the retried publish", got)
	}
	replay, h, rbody3 := proofPost(t, st.srv, "/capacity-plans/"+plan.ID+"/publications", "proof-ppub-1", "{}")
	if replay != http.StatusOK || h.Get("Idempotency-Replayed") != "true" || rbody3 != rbody2 {
		t.Fatalf("replay = %d replayed=%q, want the stored 200 replayed", replay, h.Get("Idempotency-Replayed"))
	}
}

// TestProcessPathNotFoundRefusalIsNotStoredSoItsKeyRetriesTheWork: a plan for a
// path whose flow has not arrived yet is refused 404 process-path-not-found and
// nothing is stored; once the event is consumed, the same key and body execute
// and the 201 is stored and replayed.
func TestProcessPathNotFoundRefusalIsNotStoredSoItsKeyRetriesTheWork(t *testing.T) {
	st := newReplayProofStack(t)
	body := `{"warehouse_id":"WH-1","site_id":"SIM1","location":"PATHLOC","window_start":"2027-03-01T08:00:00Z","window_end":"2027-03-01T16:00:00Z","path_id":"LATE-PROOF","assigned_demand":100,"units_per_order":1}`

	refused, _, rbody := proofPost(t, st.srv, "/capacity-plans", "proof-path-1", body)
	if refused != http.StatusNotFound || !strings.Contains(rbody, "/process-path-not-found") {
		t.Fatalf("first create = %d %s, want 404 process-path-not-found", refused, rbody)
	}
	if n := keyRows(t, st.pool, "proof-path-1"); n != 0 {
		t.Fatalf("key rows after the RETRYABLE 404 = %d, want 0: the key must be released", n)
	}

	seedActiveFlow(t, st.flows, "LATE-PROOF")
	registerStepCapacity(t, st.srv, "proof-cap-2", "PICK", "PATHLOC", "2027-03-01T08:00:00Z", "2027-03-01T16:00:00Z")

	retry, _, rbody2 := proofPost(t, st.srv, "/capacity-plans", "proof-path-1", body)
	if retry != http.StatusCreated {
		t.Fatalf("retry under the same key = %d %s, want the real create to execute (201)", retry, rbody2)
	}
	if n := keyRows(t, st.pool, "proof-path-1"); n != 1 {
		t.Fatalf("key rows after the successful retry = %d, want 1", n)
	}
	replay, h, rbody3 := proofPost(t, st.srv, "/capacity-plans", "proof-path-1", body)
	if replay != http.StatusCreated || h.Get("Idempotency-Replayed") != "true" || rbody3 != rbody2 {
		t.Fatalf("replay = %d replayed=%q, want the stored 201 replayed", replay, h.Get("Idempotency-Replayed"))
	}
}

// TestMissingAssignedDemandRefusalIsNotStoredSoItsKeyRetriesTheWork: a plan
// with no assigned_demand and no order data yet is refused 422
// missing-assigned-demand and nothing is stored; once an order event lands in
// the read model, the same key and body execute and the 201 is stored.
func TestMissingAssignedDemandRefusalIsNotStoredSoItsKeyRetriesTheWork(t *testing.T) {
	st := newReplayProofStack(t)
	seedActiveFlow(t, st.flows, "DEMAND-PROOF")
	registerStepCapacity(t, st.srv, "proof-cap-3", "PICK", "DEMANDLOC", "2027-03-01T08:00:00Z", "2027-03-01T16:00:00Z")
	body := `{"warehouse_id":"WH-1","site_id":"SIM1","location":"DEMANDLOC","window_start":"2027-03-01T08:00:00Z","window_end":"2027-03-01T16:00:00Z","path_id":"DEMAND-PROOF","units_per_order":1}`

	refused, _, rbody := proofPost(t, st.srv, "/capacity-plans", "proof-demand-1", body)
	if refused != http.StatusUnprocessableEntity || !strings.Contains(rbody, "/missing-assigned-demand") {
		t.Fatalf("first create = %d %s, want 422 missing-assigned-demand", refused, rbody)
	}
	if n := keyRows(t, st.pool, "proof-demand-1"); n != 0 {
		t.Fatalf("key rows after the RETRYABLE 422 = %d, want 0: the key must be released", n)
	}

	// The order-management event lands: the read model now expects one order in the window.
	if _, err := st.pool.Exec(context.Background(),
		`INSERT INTO order_demand (order_id, location, promise_at, released_lines, as_of) VALUES ($1, $2, $3, 5, $4)`,
		"proof-o-1", "DEMANDLOC", "2027-03-01T10:00:00Z", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	retry, _, rbody2 := proofPost(t, st.srv, "/capacity-plans", "proof-demand-1", body)
	if retry != http.StatusCreated {
		t.Fatalf("retry under the same key = %d %s, want the real create to execute (201)", retry, rbody2)
	}
	if n := keyRows(t, st.pool, "proof-demand-1"); n != 1 {
		t.Fatalf("key rows after the successful retry = %d, want 1", n)
	}
	replay, h, rbody3 := proofPost(t, st.srv, "/capacity-plans", "proof-demand-1", body)
	if replay != http.StatusCreated || h.Get("Idempotency-Replayed") != "true" || rbody3 != rbody2 {
		t.Fatalf("replay = %d replayed=%q, want the stored 201 replayed", replay, h.Get("Idempotency-Replayed"))
	}
}
