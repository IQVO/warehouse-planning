package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"

	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// Wave-2 steps: facts that arrive from Kafka (workforce-management's
// ShiftPlanCommitted, facility-layout's slot lifecycle) are fed through the
// REAL consumers' HandleMessage -- the same bytes the Kafka loop would hand
// them -- and asserted only through REST or the outbox. The generic HTTP
// steps exist for the malformed-input scenarios, which need raw query strings
// and bodies the typed steps deliberately cannot build.

const (
	typeShiftPlanCommitted         = "com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCommitted"
	typeShiftPlanCancelled         = "com.warehouse.wes.workforce-management.shiftplan.ShiftPlanCancelled"
	typeLocationSlotRegistered     = "com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered"
	typeLocationSlotDecommissioned = "com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned"
	outboxEventTypePrefix          = "com.warehouse.wes.warehouse-planning.capacityplan."
)

// wave2State is the per-scenario state of the wave-2 steps.
type wave2State struct {
	// labor is the REAL workforce-management consumer over the same
	// in-memory ProcessCapacity repository the HTTP server reads.
	labor *inboundkafka.LaborCapacityConsumer
	// slotCodes remembers, per zone, the codes of the slots facility-layout
	// registered, so a step can decommission "N slots of the zone".
	slotCodes map[string][]string
}

// newWave2State wires the labor consumer the way cmd/api does: the
// consumer's unit of work wraps (and the use case's Do joins it) the same
// outbox the HTTP surface writes, so a ShiftPlanCommitted that registers
// or changes a LABOR constraint also leaves its ProcessCapacity events
// there.
func newWave2State(repo *memory.ProcessCapacityRepo, outboxRepo *memory.OutboxRepo, encoder ports.EventEncoder, uow ports.UnitOfWork) wave2State {
	processed := memory.NewProcessedEventRepo()
	return wave2State{
		labor: &inboundkafka.LaborCapacityConsumer{
			Register: &usecases.RegisterProcessCapacityConstraint{
				Repo: repo,
				// Same contract as cmd/api: the events raised by the
				// registration join the consumer's open unit of work.
				Outbox: outboxRepo, Encoder: encoder, UnitOfWork: uow,
			},
			ProcessedEvents: processed,
			UoW:             memory.NewUnitOfWork(repo, processed),
			Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		},
		slotCodes: map[string][]string{},
	}
}

func (s *wave2State) noteSlot(zone, code string) {
	s.slotCodes[zone] = append(s.slotCodes[zone], code)
}

// ----------------------------------------------------------- raw HTTP ----

func (w *world) recordRaw(ctx context.Context, method, path, body string) error {
	req, err := http.NewRequestWithContext(ctx, method, w.server.URL+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.server.Client().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	w.status, w.body = resp.StatusCode, raw
	return nil
}

func (w *world) iSendGet(ctx context.Context, path string) error {
	return w.record(ctx, http.MethodGet, path, nil)
}

func (w *world) iSendWithBody(ctx context.Context, method, path string, doc *godog.DocString) error {
	if err := w.recordRaw(ctx, method, path, doc.Content); err != nil {
		return err
	}
	if method == http.MethodPost && path == "/capacity-plans" && w.status == http.StatusCreated {
		var plan struct {
			ID string `json:"id"`
		}
		if err := w.decode(&plan); err != nil {
			return err
		}
		w.planID = plan.ID
	}
	return nil
}

func (w *world) iGetTheCurrentCapacityPlan(ctx context.Context) error {
	return w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID, nil)
}

func (w *world) iGetTheCapacityPlan(ctx context.Context, id string) error {
	return w.record(ctx, http.MethodGet, "/capacity-plans/"+id, nil)
}

// ------------------------------------------------- constraints (REST) ----

// iRegisterAConstraintOverPeriod registers a constraint with an explicit
// period (the typed step always sends 3600 s).
func (w *world) iRegisterAConstraintOverPeriod(ctx context.Context, constraintType string, quantity float64, unit string, periodSeconds float64, processType, location, windowStart, windowEnd string) error {
	return w.record(ctx, http.MethodPost, "/process-capacities", map[string]any{
		"process_type": processType, "location": location,
		"window_start": windowStart, "window_end": windowEnd,
		"constraint_type": constraintType, "quantity": quantity, "unit": unit,
		"period_seconds": periodSeconds,
	})
}

func (w *world) iDeclareAStationStandardOverPeriod(ctx context.Context, quantity float64, unit string, periodSeconds float64, processType, location string) error {
	return w.record(ctx, http.MethodPut, fmt.Sprintf("/station-standards/%s/%s", location, processType), map[string]any{
		"quantity": quantity, "unit": unit, "period_seconds": periodSeconds,
	})
}

func (w *world) iListTheStationStandards(ctx context.Context) error {
	return w.record(ctx, http.MethodGet, "/station-standards", nil)
}

func (w *world) iListTheStationStandardsOf(ctx context.Context, location string) error {
	return w.record(ctx, http.MethodGet, "/station-standards?location="+location, nil)
}

func (w *world) iRegisterAProcessPathWithoutSteps(ctx context.Context, id, name string) error {
	return w.record(ctx, http.MethodPost, "/process-paths", map[string]any{"id": id, "name": name, "steps": []string{}})
}

// iCreateManyPlans creates n plans on the list scenarios' fixed path, each
// created one minute after the previous one (the world's plan clock).
func (w *world) iCreateManyPlans(ctx context.Context, n int, demand float64, location string) error {
	for i := 0; i < n; i++ {
		if err := w.iCreateAPlanWithDemandAt(ctx, demand, location); err != nil {
			return err
		}
		if w.status != http.StatusCreated {
			return fmt.Errorf("plan %d of %d: expected 201, got %d: %s", i+1, n, w.status, string(w.body))
		}
	}
	return nil
}

// --------------------------------------------------- Kafka: workforce ----

func (w *world) deliverLabor(typ, eventID string, data map[string]any, start string) error {
	at, err := time.Parse(time.RFC3339, start)
	if err != nil {
		return err
	}
	value, err := cloudEventBytes(eventID, "/warehouse/workforce-management", typ, eventID, at, data)
	if err != nil {
		return err
	}
	return w.w2.labor.HandleMessage(context.Background(), value)
}

func shiftPlanData(building, pathID string, heads int, rate, hours float64) map[string]any {
	return map[string]any{
		"building_id": building, "shift_id": "shift-A", "path_id": pathID,
		"planned_heads": heads, "planned_rate": rate, "planned_hours": hours,
	}
}

func (w *world) workforceCommittedShiftPlan(eventID, building, pathID string, heads int, rate, hours float64, start string) error {
	return w.deliverLabor(typeShiftPlanCommitted, eventID, shiftPlanData(building, pathID, heads, rate, hours), start)
}

func (w *world) workforceCommittedShiftPlanWithoutBuilding(eventID, pathID, start string) error {
	return w.deliverLabor(typeShiftPlanCommitted, eventID, shiftPlanData("", pathID, 10, 400, 8), start)
}

func (w *world) workforceCancelledShiftPlan(eventID, building, pathID, start string) error {
	return w.deliverLabor(typeShiftPlanCancelled, eventID, shiftPlanData(building, pathID, 10, 400, 8), start)
}

func (w *world) workforceTopicCarriesGarbage() error {
	return w.w2.labor.HandleMessage(context.Background(), []byte(`{"event_id":"legacy","event_type":"ShiftPlanCommitted","payload":{}}`))
}

// ------------------------------------------------- Kafka: facility ----

func (w *world) deliverFacility(typ, eventID, subject string, data map[string]any) error {
	value, err := cloudEventBytes(eventID, "/warehouse/facility-layout", typ, subject, time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC), data)
	if err != nil {
		return err
	}
	return w.facility.HandleMessage(context.Background(), value)
}

func (w *world) facilityAnnouncedStorageSlot(code, locationType, zone, eventID string) error {
	return w.deliverFacility(typeLocationSlotRegistered, eventID, code, map[string]any{
		"locationCode": code, "zoneId": zone, "locationType": locationType,
	})
}

func (w *world) facilityDecommissionedSlot(code string) error {
	return w.deliverFacility(typeLocationSlotDecommissioned, "wave2-decommission-"+code, code, map[string]any{"locationCode": code})
}

func (w *world) facilityDecommissionedSlotsInZone(n int, zone string) error {
	codes := w.w2.slotCodes[zone]
	if n > len(codes) {
		return fmt.Errorf("zone %s has only %d registered slots, cannot decommission %d", zone, len(codes), n)
	}
	for _, code := range codes[:n] {
		if err := w.facilityDecommissionedSlot(code); err != nil {
			return err
		}
	}
	w.w2.slotCodes[zone] = codes[n:]
	return nil
}

func cloudEventBytes(id, source, typ, subject string, at time.Time, data any) ([]byte, error) {
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(id)
	e.SetSource(source)
	e.SetType(typ)
	e.SetSubject(subject)
	e.SetTime(at)
	if err := e.SetData("application/json", data); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// -------------------------------------------------------------- Then ----

func (w *world) theResponseFieldIs(field, raw string) error {
	var body map[string]any
	if err := w.decode(&body); err != nil {
		return err
	}
	var want any
	if err := json.Unmarshal([]byte(raw), &want); err != nil {
		return fmt.Errorf("expected value %s is not JSON: %w", raw, err)
	}
	got, present := body[field]
	if !present || !reflect.DeepEqual(got, want) {
		return fmt.Errorf("response field %q = %v (present: %v), want %v in %s", field, got, present, want, string(w.body))
	}
	return nil
}

func (w *world) theResponseFieldIsPresent(field string, want bool) error {
	var body map[string]any
	if err := w.decode(&body); err != nil {
		return err
	}
	if _, present := body[field]; present != want {
		return fmt.Errorf("response field %q present = %v, want %v in %s", field, present, want, string(w.body))
	}
	return nil
}

func (w *world) theCapacityPlanListHasPlans(n int) error {
	var body struct {
		CapacityPlans []json.RawMessage `json:"capacity_plans"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if len(body.CapacityPlans) != n {
		return fmt.Errorf("expected %d listed plans, got %d", n, len(body.CapacityPlans))
	}
	return nil
}

// theStationStandardsListIs handles `the station standards list is
// "SIM1/PACK=180 PACKAGE, SIM1/PICK=90 UNIT"` (empty string = none).
func (w *world) theStationStandardsListIs(raw string) error {
	var body struct {
		Standards []struct {
			Location    string  `json:"location"`
			ProcessType string  `json:"process_type"`
			Quantity    float64 `json:"quantity"`
			Unit        string  `json:"unit"`
		} `json:"standards"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	if body.Standards == nil {
		return fmt.Errorf("standards must be an array, never null: %s", string(w.body))
	}
	got := make([]string, 0, len(body.Standards))
	for _, s := range body.Standards {
		got = append(got, fmt.Sprintf("%s/%s=%v %s", s.Location, s.ProcessType, s.Quantity, s.Unit))
	}
	want := make([]string, 0)
	for _, item := range strings.Split(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			want = append(want, item)
		}
	}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		return fmt.Errorf("expected station standards %v, got %v", want, got)
	}
	return nil
}

func (w *world) theStorageCapacityListsNoZone(zone string) error {
	var body struct {
		Positions []struct {
			ZoneID string `json:"zone_id"`
		} `json:"storage_positions"`
		Stations []struct {
			ZoneID string `json:"zone_id"`
		} `json:"stations"`
	}
	if err := w.decode(&body); err != nil {
		return err
	}
	for _, p := range body.Positions {
		if p.ZoneID == zone {
			return fmt.Errorf("zone %s must not be listed, got %s", zone, string(w.body))
		}
	}
	for _, s := range body.Stations {
		if s.ZoneID == zone {
			return fmt.Errorf("zone %s must not be listed, got %s", zone, string(w.body))
		}
	}
	return nil
}

// theOutboxEventHas asserts one queued integration event of the current plan:
// its CloudEvents subject and Kafka key are the plan id, and one field of its
// data payload has the stated JSON value.
func (w *world) theOutboxEventHas(name, field, raw string) error {
	var want any
	if err := json.Unmarshal([]byte(raw), &want); err != nil {
		return fmt.Errorf("expected value %s is not JSON: %w", raw, err)
	}
	for _, m := range w.outbox.Messages() {
		if m.EventType != outboxEventTypePrefix+name {
			continue
		}
		var envelope struct {
			Subject string         `json:"subject"`
			Data    map[string]any `json:"data"`
		}
		if err := json.Unmarshal(m.Value, &envelope); err != nil {
			return err
		}
		if envelope.Subject != w.planID || string(m.Key) != w.planID {
			return fmt.Errorf("%s: subject %q / key %q, want the plan id %q", name, envelope.Subject, string(m.Key), w.planID)
		}
		if got := envelope.Data[field]; !reflect.DeepEqual(got, want) {
			return fmt.Errorf("%s: data.%s = %v, want %v (data %v)", name, field, got, want, envelope.Data)
		}
		return nil
	}
	return fmt.Errorf("no %s event in the outbox", name)
}

// ------------------------------------------------------------ wiring ----

func registerWave2Steps(sc *godog.ScenarioContext, w *world) {
	const (
		ts       = `\"([^\"]*)\"`
		num      = `(\d+(?:\.\d+)?)`
		units    = `(UNIT|LINE|ORDER|PACKAGE)`
		ctypes   = `(LABOR|LOCATION|EQUIPMENT|STATION|CONVEYOR|BUFFER|REPLENISHMENT)`
		jsonVal  = `("[^"]*"|-?\d+(?:\.\d+)?|true|false|null)`
		eventTyp = `(CapacityPlanCreated|CapacityPlanPublished|CapacityShortageDetected|BottleneckDetected)`
	)

	sc.Step(`^I send GET `+ts+`$`, w.iSendGet)
	sc.Step(`^I send (POST|PUT) `+ts+` with body:$`, w.iSendWithBody)
	sc.Step(`^I get the current capacity plan$`, w.iGetTheCurrentCapacityPlan)
	sc.Step(`^I get the capacity plan `+ts+`$`, w.iGetTheCapacityPlan)

	sc.Step(`^I register an? `+ctypes+` constraint of (-\d+(?:\.\d+)?) `+units+` per HOUR for ([A-Z-]+) at ([A-Z0-9-]+) for the window `+ts+` to `+ts+`$`,
		func(ctx context.Context, constraintType string, quantity float64, unit, processType, location, start, end string) error {
			return w.iRegisterAConstraint(ctx, constraintType, quantity, unit, processType, location, start, end)
		})
	sc.Step(`^I register an? `+ctypes+` constraint of `+num+` `+units+` per (-?\d+) seconds for ([A-Z-]+) at ([A-Z0-9-]+) for the window `+ts+` to `+ts+`$`, w.iRegisterAConstraintOverPeriod)
	sc.Step(`^I declare a station standard of `+num+` `+units+` per (-?\d+) seconds for ([A-Z-]+) at ([A-Z0-9-]+)$`, w.iDeclareAStationStandardOverPeriod)
	sc.Step(`^I list the station standards$`, w.iListTheStationStandards)
	sc.Step(`^I list the station standards of `+ts+`$`, w.iListTheStationStandardsOf)
	sc.Step(`^I register a process path `+ts+` named `+ts+` with no steps$`, w.iRegisterAProcessPathWithoutSteps)
	sc.Step(`^I create (\d+) capacity plans with assigned demand (\d+) at `+ts+` on path "pick-rebin-pack"$`, w.iCreateManyPlans)

	sc.Step(`^workforce-management committed shift plan `+ts+` for building `+ts+` on path `+ts+` with (\d+) heads at `+num+` per head per hour for `+num+` hours starting `+ts+`$`, w.workforceCommittedShiftPlan)
	sc.Step(`^workforce-management committed shift plan `+ts+` without a building on path `+ts+` starting `+ts+`$`, w.workforceCommittedShiftPlanWithoutBuilding)
	sc.Step(`^workforce-management cancelled shift plan `+ts+` for building `+ts+` on path `+ts+` starting `+ts+`$`, w.workforceCancelledShiftPlan)
	sc.Step(`^the workforce topic carries a message that is not a CloudEvent$`, w.workforceTopicCarriesGarbage)

	sc.Step(`^facility-layout announced storage slot `+ts+` of type `+ts+` in zone `+ts+` as event `+ts+`$`, w.facilityAnnouncedStorageSlot)
	sc.Step(`^facility-layout decommissioned the slot `+ts+`$`, w.facilityDecommissionedSlot)
	sc.Step(`^facility-layout decommissioned (\d+) slots in zone `+ts+`$`, w.facilityDecommissionedSlotsInZone)

	sc.Step(`^the response field `+ts+` is `+jsonVal+`$`, w.theResponseFieldIs)
	sc.Step(`^the response field `+ts+` is present$`, func(field string) error { return w.theResponseFieldIsPresent(field, true) })
	sc.Step(`^the response field `+ts+` is absent$`, func(field string) error { return w.theResponseFieldIsPresent(field, false) })
	sc.Step(`^the capacity plan list has (\d+) plans$`, w.theCapacityPlanListHasPlans)
	sc.Step(`^the station standards list is `+ts+`$`, w.theStationStandardsListIs)
	sc.Step(`^the storage capacity lists no zone `+ts+`$`, w.theStorageCapacityListsNoZone)
	sc.Step(`^the `+eventTyp+` event of the plan has `+`([a-z_]+) `+jsonVal+`$`, w.theOutboxEventHas)
}
