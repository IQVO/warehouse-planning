// Package main_test hosts the godog (Cucumber for Go) acceptance suite. It
// drives the REAL chi router over HTTP -- the same wiring the service uses
// in production, but with the in-memory outbound adapter -- so every
// scenario in features/*.feature is a true black-box test of the REST API.
// Mirrors inventory-storage's root-level features_test.go shape.
package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	outboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/outbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// TestFeatures runs every Gherkin feature under features/ against a
// freshly wired HTTP server.
func TestFeatures(t *testing.T) {
	suite := godog.TestSuite{
		ScenarioInitializer: InitializeScenario,
		Options: &godog.Options{
			Format:   "pretty",
			Paths:    []string{"features"},
			Strict:   true,
			TestingT: t,
		},
	}
	if suite.Run() != 0 {
		t.Fatal("non-zero status returned, failed to run feature tests")
	}
}

// world is the per-scenario state: a running server over a fresh in-memory
// repository, plus whatever the last HTTP call returned.
type world struct {
	server *httptest.Server

	status int
	body   []byte

	// outbox is the in-memory transactional outbox behind the server, so
	// scenarios can assert which integration events were queued.
	outbox *memory.OutboxRepo
	// planID is the id of the capacity plan the last successful create
	// returned; the publish/get steps act on it.
	planID string

	// facility is the REAL facility-layout consumer over the in-memory tally:
	// the Given steps feed it LocationSlotRegistered CloudEvents through
	// HandleMessage, exactly the bytes the Kafka loop would hand it. slotSeq
	// keeps every slot's locationCode and event id unique.
	facility *inboundkafka.StorageCapacityConsumer
	slotSeq  int

	// orders is the REAL order-demand consumer over the in-memory expected-
	// demand read model: the Given steps feed it order-management CloudEvents
	// through HandleMessage, exactly the bytes the Kafka loop would hand it.
	// orderSeq keeps every event id unique and increases the event time.
	orders   *inboundkafka.OrderDemandConsumer
	orderSeq int

	// planClock is the creation time of the last capacity plan; see
	// nextPlanTime.
	planClock time.Time
}

// start builds the composition root the way cmd/ would, but with the
// in-memory repo.
func (w *world) start() {
	repo := memory.NewProcessCapacityRepo()
	pathRepo := memory.NewProcessPathRepo()
	planRepo, outboxRepo := memory.NewCapacityPlanRepo(), memory.NewOutboxRepo()
	standards, tallyRepo, processed := memory.NewStationStandardRepo(), memory.NewStorageTallyRepo(), memory.NewProcessedEventRepo()
	uow := memory.NewUnitOfWork(repo, planRepo, outboxRepo)
	encoder := outboundkafka.NewEncoder()
	pathCapacity := &usecases.GetProcessPathCapacity{ProcessPaths: pathRepo, ProcessCapacities: repo, StationStandards: standards, Tally: tallyRepo}
	w.facility = &inboundkafka.StorageCapacityConsumer{
		Tally: tallyRepo, ProcessedEvents: processed, UoW: memory.NewUnitOfWork(tallyRepo, processed),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	w.slotSeq = 0
	demandRepo, demandProcessed := memory.NewOrderDemandRepo(), memory.NewProcessedEventRepo()
	expectedDemand := &usecases.GetExpectedDemand{Demand: demandRepo}
	w.orders = &inboundkafka.OrderDemandConsumer{
		Record: &usecases.RecordOrderDemand{
			UoW: memory.NewUnitOfWork(demandRepo, demandProcessed), ProcessedEvents: demandProcessed, Demand: demandRepo,
		},
		Location: "SIM1",
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	w.orderSeq = 0
	w.planClock = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)
	s := &inboundhttp.Server{
		RegisterProcessCapacityConstraint: &usecases.RegisterProcessCapacityConstraint{Repo: repo},
		ProcessCapacities:                 repo,
		RegisterProcessPath:               &usecases.RegisterProcessPath{Repo: pathRepo},
		GetProcessPathCapacity:            pathCapacity,
		CreateCapacityPlan: &usecases.CreateCapacityPlan{
			PathCapacity: pathCapacity, Plans: planRepo, Outbox: outboxRepo, Encoder: encoder, UnitOfWork: uow,
			Demand: expectedDemand,
			// Each plan is created one minute after the previous one, so
			// "newest first" in the list scenarios never hangs on a clock tie.
			Now: w.nextPlanTime,
		},
		PublishCapacityPlan: &usecases.PublishCapacityPlan{Plans: planRepo, Outbox: outboxRepo, Encoder: encoder, UnitOfWork: uow},
		CapacityPlans:       planRepo,
		ListProcessPaths:    &usecases.ListProcessPaths{Paths: pathRepo},
		ListCapacityPlans:   &usecases.ListCapacityPlans{Plans: planRepo},

		DeclareStationStandard: &usecases.DeclareStationStandard{Repo: standards},
		StationStandards:       standards,
		GetStorageCapacity:     &usecases.GetStorageCapacity{Tally: tallyRepo},
		GetExpectedDemand:      expectedDemand,
	}
	w.server = httptest.NewServer(inboundhttp.NewRouter(s))
	w.outbox = outboxRepo
	w.planID = ""
	w.status = 0
	w.body = nil
}

func (w *world) stop() {
	if w.server != nil {
		w.server.Close()
		w.server = nil
	}
}

// record issues a request and remembers the response the Then steps assert
// against.
func (w *world) record(ctx context.Context, method, path string, payload any) error {
	var reader io.Reader = http.NoBody
	if payload != nil {
		encoded, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		reader = strings.NewReader(string(encoded))
	}

	req, err := http.NewRequestWithContext(ctx, method, w.server.URL+path, reader)
	if err != nil {
		return err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := w.server.Client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.status, w.body = resp.StatusCode, body
	return nil
}

func (w *world) decode(dest any) error {
	if err := json.Unmarshal(w.body, dest); err != nil {
		return fmt.Errorf("response body is not valid JSON (%w): %s", err, string(w.body))
	}
	return nil
}

// ----------------------------------------------------------------- When ----

// unitFor maps a Gherkin unit word ("UNIT", "PACKAGE", ...) straight
// through -- the domain's CapacityUnit values are already upper-case
// words, so no translation table is needed.
func (w *world) iRegisterAConstraint(ctx context.Context, constraintType string, quantity float64, unit, processType, location, windowStart, windowEnd string) error {
	return w.record(ctx, http.MethodPost, "/process-capacities", map[string]any{
		"process_type":    processType,
		"location":        location,
		"window_start":    windowStart,
		"window_end":      windowEnd,
		"constraint_type": constraintType,
		"quantity":        quantity,
		"unit":            unit,
		"period_seconds":  3600,
	})
}

func (w *world) iLookUpTheEffectiveCapacity(ctx context.Context, processType, location, windowStart, windowEnd string) error {
	path := fmt.Sprintf("/process-capacities?process_type=%s&location=%s&window_start=%s&window_end=%s", processType, location, windowStart, windowEnd)
	return w.record(ctx, http.MethodGet, path, nil)
}

// iRegisterAProcessPath handles "I register a process path "<id>" named
// "<name>" with steps PICK, REBIN, PACK" -- stepsRaw is the raw
// comma-separated step list as it appears in the Gherkin step text.
func (w *world) iRegisterAProcessPath(ctx context.Context, id, name, stepsRaw string) error {
	rawSteps := strings.Split(stepsRaw, ",")
	steps := make([]string, 0, len(rawSteps))
	for _, s := range rawSteps {
		steps = append(steps, strings.TrimSpace(s))
	}
	return w.record(ctx, http.MethodPost, "/process-paths", map[string]any{
		"id":    id,
		"name":  name,
		"steps": steps,
	})
}

func (w *world) iLookUpTheProcessPathCapacity(ctx context.Context, pathID, location, windowStart, windowEnd string, unitsPerOrder, packagesPerOrder float64) error {
	path := fmt.Sprintf("/process-paths/%s/capacity?location=%s&window_start=%s&window_end=%s&units_per_order=%v&packages_per_order=%v",
		pathID, location, windowStart, windowEnd, unitsPerOrder, packagesPerOrder)
	return w.record(ctx, http.MethodGet, path, nil)
}

// iCreateACapacityPlan handles "I create a capacity plan for warehouse ...".
func (w *world) iCreateACapacityPlan(ctx context.Context, warehouse, location, pathID, windowStart, windowEnd string, demand, unitsPerOrder, packagesPerOrder float64) error {
	if err := w.record(ctx, http.MethodPost, "/capacity-plans", map[string]any{
		"warehouse_id":       warehouse,
		"location":           location,
		"window_start":       windowStart,
		"window_end":         windowEnd,
		"path_id":            pathID,
		"assigned_demand":    demand,
		"units_per_order":    unitsPerOrder,
		"packages_per_order": packagesPerOrder,
	}); err != nil {
		return err
	}
	if w.status == http.StatusCreated {
		var body struct {
			ID string `json:"id"`
		}
		if err := w.decode(&body); err != nil {
			return err
		}
		w.planID = body.ID
	}
	return nil
}

func (w *world) iPublishTheCapacityPlan(ctx context.Context) error {
	return w.record(ctx, http.MethodPost, "/capacity-plans/"+w.planID+"/publish", nil)
}

func (w *world) iPublishTheCapacityPlanWithID(ctx context.Context, id string) error {
	return w.record(ctx, http.MethodPost, "/capacity-plans/"+id+"/publish", nil)
}

// ----------------------------------------------------------------- Then ----

// theCapacityPlanIs asserts the plan in the last response AND the same
// plan as returned by GET /capacity-plans/{id} (so the read side agrees).
func (w *world) theCapacityPlanIs(ctx context.Context, status string, rate float64, unit string, over, shortage float64, bottleneck string) error {
	check := func(body []byte, source string) error {
		var plan struct {
			ID                 string  `json:"id"`
			Status             string  `json:"status"`
			PathCapacity       float64 `json:"path_capacity"`
			BottleneckStep     string  `json:"bottleneck_step"`
			CapacityOverWindow float64 `json:"capacity_over_window"`
			Shortage           float64 `json:"shortage"`
		}
		if err := json.Unmarshal(body, &plan); err != nil {
			return fmt.Errorf("%s: not valid JSON (%w): %s", source, err, string(body))
		}
		if plan.ID != w.planID || plan.Status != status || plan.PathCapacity != rate || plan.BottleneckStep != bottleneck ||
			plan.CapacityOverWindow != over || plan.Shortage != shortage {
			return fmt.Errorf("%s: expected %s plan %s with %v %s/HOUR, over window %v, shortage %v, bottleneck %s; got %s",
				source, status, w.planID, rate, unit, over, shortage, bottleneck, string(body))
		}
		return nil
	}
	if err := check(w.body, "response"); err != nil {
		return err
	}
	status0, body0 := w.status, w.body
	defer func() { w.status, w.body = status0, body0 }()
	if err := w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID, nil); err != nil {
		return err
	}
	if w.status != http.StatusOK {
		return fmt.Errorf("GET /capacity-plans/%s: expected 200, got %d", w.planID, w.status)
	}
	return check(w.body, "GET")
}

func (w *world) theOutboxEventTypesAre(raw string) error {
	var want []string
	for _, name := range strings.Split(raw, ",") {
		want = append(want, "com.warehouse.wes.warehouse-planning.capacityplan."+strings.TrimSpace(name))
	}
	var got []string
	for _, m := range w.outbox.Messages() {
		got = append(got, m.EventType)
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		return fmt.Errorf("expected outbox event types %v, got %v", want, got)
	}
	return nil
}

func (w *world) theResponseStatusIs(expected int) error {
	if w.status != expected {
		return fmt.Errorf("expected status %d, got %d: %s", expected, w.status, string(w.body))
	}
	return nil
}

func (w *world) theEffectiveCapacityResponseReports(rate float64, unit, _ string, binding string) error {
	var body struct {
		EffectiveRate     float64 `json:"effective_rate"`
		EffectiveUnit     string  `json:"effective_unit"`
		BindingConstraint string  `json:"binding_constraint"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.EffectiveRate != rate || body.EffectiveUnit != unit || body.BindingConstraint != binding {
		return fmt.Errorf("expected %v %s/HOUR bound by %s, got %v %s bound by %s", rate, unit, binding, body.EffectiveRate, body.EffectiveUnit, body.BindingConstraint)
	}
	return nil
}

func (w *world) theEffectiveCapacityResponseListsConstraints(expected int) error {
	var body struct {
		Constraints []struct {
			ConstraintType string `json:"constraint_type"`
		} `json:"constraints"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if len(body.Constraints) != expected {
		return fmt.Errorf("expected %d constraints, got %d", expected, len(body.Constraints))
	}
	return nil
}

func (w *world) theProblemDetailTypeIs(slug string) error {
	var problem struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
	}
	if err := w.decode(&problem); err != nil {
		return err
	}
	if got := problem.Type[strings.LastIndex(problem.Type, "/")+1:]; got != slug {
		return fmt.Errorf("expected problem type %q, got %q (from %q)", slug, got, problem.Type)
	}
	if problem.Status != w.status {
		return fmt.Errorf("problem body status %d does not match HTTP status %d", problem.Status, w.status)
	}
	return nil
}

// theProblemDetailMentions asserts the RFC 7807 "detail" of the last response
// contains the fragment (e.g. the step named by a missing-coverage problem).
func (w *world) theProblemDetailMentions(fragment string) error {
	var problem struct {
		Detail string `json:"detail"`
	}
	if err := w.decode(&problem); err != nil {
		return err
	}
	if !strings.Contains(problem.Detail, fragment) {
		return fmt.Errorf("problem detail %q does not mention %q", problem.Detail, fragment)
	}
	return nil
}

// theProcessPathCapacityResponseReports handles "the process path capacity
// response reports <rate> ORDER per HOUR bound by <step>".
func (w *world) theProcessPathCapacityResponseReports(rate float64, unit, _ string, bottleneck string) error {
	var body struct {
		NormalizedRate float64 `json:"normalized_rate"`
		NormalizedUnit string  `json:"normalized_unit"`
		BottleneckStep string  `json:"bottleneck_step"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.NormalizedRate != rate || body.NormalizedUnit != unit || body.BottleneckStep != bottleneck {
		return fmt.Errorf("expected %v %s/HOUR bound by %s, got %v %s bound by %s", rate, unit, bottleneck, body.NormalizedRate, body.NormalizedUnit, body.BottleneckStep)
	}
	return nil
}

// ----------------------------------------------------- station capacity ----

// facilityRegistered feeds n LocationSlotRegistered CloudEvents (one per
// slot, role/payload as facility-layout publishes them) through the real
// consumer's HandleMessage into the tally.
func (w *world) facilityRegistered(payload func(code string) map[string]any, zone string, n int) error {
	for i := 0; i < n; i++ {
		w.slotSeq++
		code := fmt.Sprintf("%s-%03d", zone, w.slotSeq)
		data := payload(code)
		data["locationCode"], data["zoneId"] = code, zone

		e := ce.New(ce.CloudEventsVersionV1)
		e.SetID(fmt.Sprintf("bdd-evt-%d", w.slotSeq))
		e.SetSource("/warehouse/facility-layout")
		e.SetType("com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered")
		e.SetSubject(code)
		e.SetTime(time.Now().UTC())
		e.SetDataSchema("urn:warehouse:facility-layout:events:LocationSlotRegistered:v1")
		if err := e.SetData("application/json", data); err != nil {
			return err
		}
		value, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := w.facility.HandleMessage(context.Background(), value); err != nil {
			return fmt.Errorf("facility consumer rejected slot %s: %w", code, err)
		}
	}
	return nil
}

func (w *world) facilityRegisteredWorkCenterSlots(n int, activity, zone string) error {
	return w.facilityRegistered(func(string) map[string]any {
		return map[string]any{"role": "WorkCenter", "activities": []string{activity}}
	}, zone, n)
}

func (w *world) facilityRegisteredStorageSlots(n int, locationType, zone string) error {
	return w.facilityRegistered(func(string) map[string]any {
		return map[string]any{"locationType": locationType}
	}, zone, n)
}

func (w *world) iDeclareAStationStandard(ctx context.Context, quantity float64, unit, processType, location string) error {
	return w.record(ctx, http.MethodPut, fmt.Sprintf("/station-standards/%s/%s", location, processType), map[string]any{
		"quantity": quantity, "unit": unit, "period_seconds": 3600,
	})
}

func (w *world) iLookUpTheStorageCapacity(ctx context.Context, location string) error {
	return w.record(ctx, http.MethodGet, "/storage-capacity?location="+location, nil)
}

// theStepBreakdownIs handles `the step breakdown is "PICK:3200:LABOR,..."`.
func (w *world) theStepBreakdownIs(raw string) error {
	var body struct {
		Steps []struct {
			Step              string  `json:"step"`
			NormalizedRate    float64 `json:"normalized_rate"`
			BindingConstraint string  `json:"binding_constraint"`
		} `json:"step_breakdown"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	var got []string
	for _, s := range body.Steps {
		got = append(got, fmt.Sprintf("%s:%v:%s", s.Step, s.NormalizedRate, s.BindingConstraint))
	}
	if strings.Join(got, ",") != raw {
		return fmt.Errorf("expected step breakdown %s, got %s (body %s)", raw, strings.Join(got, ","), string(w.body))
	}
	return nil
}

func (w *world) theResponseHasWarnings(expected int) error {
	var body struct {
		Warnings []string `json:"warnings"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.Warnings == nil || len(body.Warnings) != expected {
		return fmt.Errorf("expected %d warnings (as a JSON array), got %v in %s", expected, body.Warnings, string(w.body))
	}
	return nil
}

func (w *world) theWarningMentions(fragment string) error {
	var body struct {
		Warnings []string `json:"warnings"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	for _, warning := range body.Warnings {
		if strings.Contains(warning, fragment) {
			return nil
		}
	}
	return fmt.Errorf("no warning mentions %q: %v", fragment, body.Warnings)
}

// theCapacityPlanIsBoundBy asserts the plan's bottleneck_constraint in the
// last response AND as stored (GET), so the read side agrees.
func (w *world) theCapacityPlanIsBoundBy(ctx context.Context, constraint string) error {
	check := func(body []byte, source string) error {
		var plan struct {
			BottleneckConstraint string `json:"bottleneck_constraint"`
		}
		if err := json.Unmarshal(body, &plan); err != nil {
			return fmt.Errorf("%s: not valid JSON (%w): %s", source, err, string(body))
		}
		if plan.BottleneckConstraint != constraint {
			return fmt.Errorf("%s: expected bottleneck_constraint %s, got %s", source, constraint, string(body))
		}
		return nil
	}
	if err := check(w.body, "response"); err != nil {
		return err
	}
	status0, body0 := w.status, w.body
	defer func() { w.status, w.body = status0, body0 }()
	if err := w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID, nil); err != nil {
		return err
	}
	return check(w.body, "GET")
}

// theStorageCapacityLists handles `the storage capacity lists N <activity>
// stations in zone "Z"` and `... N <locationType> storage positions in zone "Z"`.
func (w *world) theStorageCapacityListsStations(n int, activity, zone string) error {
	var body struct {
		Stations []struct {
			ZoneID   string `json:"zone_id"`
			Activity string `json:"activity"`
			Stations int    `json:"stations"`
		} `json:"stations"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	for _, s := range body.Stations {
		if s.ZoneID == zone && s.Activity == activity && s.Stations == n {
			return nil
		}
	}
	return fmt.Errorf("no entry of %d %s stations in zone %s in %s", n, activity, zone, string(w.body))
}

func (w *world) theStorageCapacityListsPositions(n int, locationType, zone string) error {
	var body struct {
		Positions []struct {
			ZoneID       string `json:"zone_id"`
			LocationType string `json:"location_type"`
			Positions    int    `json:"positions"`
		} `json:"storage_positions"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	for _, p := range body.Positions {
		if p.ZoneID == zone && p.LocationType == locationType && p.Positions == n {
			return nil
		}
	}
	return fmt.Errorf("no entry of %d %s storage positions in zone %s in %s", n, locationType, zone, string(w.body))
}

func (w *world) theStorageCapacityListsNothing() error {
	var body struct {
		Positions []any `json:"storage_positions"`
		Stations  []any `json:"stations"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.Positions == nil || body.Stations == nil || len(body.Positions) != 0 || len(body.Stations) != 0 {
		return fmt.Errorf("expected empty storage_positions and stations arrays, got %s", string(w.body))
	}
	return nil
}

// ------------------------------------------------------------- wiring ------

// InitializeScenario registers the step definitions and gives every
// scenario its own server and its own in-memory state.
func InitializeScenario(sc *godog.ScenarioContext) {
	w := &world{}

	sc.Before(func(ctx context.Context, _ *godog.Scenario) (context.Context, error) {
		w.start()
		return ctx, nil
	})
	sc.After(func(ctx context.Context, _ *godog.Scenario, _ error) (context.Context, error) {
		w.stop()
		return ctx, nil
	})

	sc.Step(`^I register an? (LABOR|LOCATION|EQUIPMENT|STATION|CONVEYOR|BUFFER|REPLENISHMENT) constraint of (\d+(?:\.\d+)?) (UNIT|LINE|ORDER|PACKAGE) per HOUR for ([A-Z-]+) at ([A-Z0-9-]+) for the window "([^"]*)" to "([^"]*)"$`,
		func(ctx context.Context, constraintType string, quantity float64, unit, processType, location, windowStart, windowEnd string) error {
			return w.iRegisterAConstraint(ctx, constraintType, quantity, unit, processType, location, windowStart, windowEnd)
		})
	sc.Step(`^I look up the effective capacity for ([A-Z-]+) at ([A-Z0-9-]+) for the window "([^"]*)" to "([^"]*)"$`, w.iLookUpTheEffectiveCapacity)
	sc.Step(`^I register a process path "([^"]*)" named "([^"]*)" with steps ([A-Z, ]+)$`, w.iRegisterAProcessPath)
	sc.Step(`^I look up the capacity of process path "([^"]*)" at "([^"]*)" for the window "([^"]*)" to "([^"]*)" with units_per_order (\d+(?:\.\d+)?) and packages_per_order (\d+(?:\.\d+)?)$`, w.iLookUpTheProcessPathCapacity)

	sc.Step(`^I create a capacity plan for warehouse "([^"]*)" at "([^"]*)" on path "([^"]*)" for the window "([^"]*)" to "([^"]*)" with assigned demand (-?\d+(?:\.\d+)?), units_per_order (\d+(?:\.\d+)?) and packages_per_order (\d+(?:\.\d+)?)$`, w.iCreateACapacityPlan)
	sc.Step(`^I publish the capacity plan$`, w.iPublishTheCapacityPlan)
	sc.Step(`^I publish the capacity plan "([^"]*)"$`, w.iPublishTheCapacityPlanWithID)

	sc.Step(`^facility-layout registered (\d+) ([A-Za-z]+) work-center slots? in zone "([^"]*)"$`, w.facilityRegisteredWorkCenterSlots)
	sc.Step(`^facility-layout registered (\d+) storage slots? of type "([^"]*)" in zone "([^"]*)"$`, w.facilityRegisteredStorageSlots)
	sc.Step(`^I declare a station standard of (\d+(?:\.\d+)?) (UNIT|LINE|ORDER|PACKAGE) per HOUR for ([A-Z-]+) at ([A-Z0-9-]+)$`, w.iDeclareAStationStandard)
	sc.Step(`^I look up the storage capacity of "([^"]*)"$`, w.iLookUpTheStorageCapacity)
	sc.Step(`^the step breakdown is "([^"]*)"$`, w.theStepBreakdownIs)
	sc.Step(`^the response has (\d+) warnings?$`, w.theResponseHasWarnings)
	sc.Step(`^a warning mentions "([^"]*)"$`, w.theWarningMentions)
	sc.Step(`^the capacity plan is bound by ([A-Z]+)$`, w.theCapacityPlanIsBoundBy)
	sc.Step(`^the storage capacity lists (\d+) ([A-Z]+) stations in zone "([^"]*)"$`, w.theStorageCapacityListsStations)
	sc.Step(`^the storage capacity lists (\d+) ([A-Za-z]+) storage positions in zone "([^"]*)"$`, w.theStorageCapacityListsPositions)
	sc.Step(`^the storage capacity lists nothing$`, w.theStorageCapacityListsNothing)

	sc.Step(`^the response status is (\d+)$`, w.theResponseStatusIs)
	sc.Step(`^the effective capacity response reports (\d+(?:\.\d+)?) (UNIT|LINE|ORDER|PACKAGE) per (HOUR) bound by (LABOR|LOCATION|EQUIPMENT|STATION|CONVEYOR|BUFFER|REPLENISHMENT)$`, w.theEffectiveCapacityResponseReports)
	sc.Step(`^the effective capacity response lists (\d+) constraints?$`, w.theEffectiveCapacityResponseListsConstraints)
	sc.Step(`^the problem detail type is "([^"]*)"$`, w.theProblemDetailTypeIs)
	sc.Step(`^the problem detail mentions "([^"]*)"$`, w.theProblemDetailMentions)
	sc.Step(`^the capacity plan is (DRAFT|PUBLISHED) with path capacity (\d+(?:\.\d+)?) (ORDER) per HOUR, capacity over window (\d+(?:\.\d+)?), shortage (\d+(?:\.\d+)?) and bottleneck ([A-Z-]+)$`, w.theCapacityPlanIs)
	sc.Step(`^the outbox event types are ([A-Za-z, ]+)$`, w.theOutboxEventTypesAre)
	sc.Step(`^the process path capacity response reports (\d+(?:\.\d+)?) (ORDER) per (HOUR) bound by ([A-Z-]+)$`, w.theProcessPathCapacityResponseReports)

	registerDemandSteps(sc, w)
	registerListSteps(sc, w)
}
