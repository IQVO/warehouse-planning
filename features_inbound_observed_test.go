package main_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"

	inboundkafka "github.com/claudioed/warehouse-planning/internal/adapters/inbound/kafka"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
)

// inboundObservedState is the ADR 0017 slice of the BDD world: the REAL
// dock-appointment and task-completion consumers over the in-memory read
// models the REST surface reads. Events reach the model only through the
// consumers' HandleMessage, exactly the bytes the Kafka loop would hand them.
type inboundObservedState struct {
	standards    ports.InboundStandardRepository
	observedRepo *memory.ObservedCapacityRepo

	declare          *usecases.DeclareInboundLabourStandard
	getInboundDemand *usecases.GetInboundDemand

	inbound   *inboundkafka.InboundDemandConsumer
	completed *inboundkafka.ObservedCapacityConsumer

	seq         int
	lastInbound []byte
	lastTask    []byte

	// rememberedPlan / rememberedOutbox are taken before a variance request
	// so a scenario can prove the advisory read changed nothing.
	rememberedPlan   []byte
	rememberedOutbox int
}

func newInboundObservedState() inboundObservedState {
	appointments, standards := memory.NewInboundAppointmentRepo(), memory.NewInboundStandardRepo()
	observedRepo := memory.NewObservedCapacityRepo()
	inboundProcessed, observedProcessed := memory.NewProcessedEventRepo(), memory.NewProcessedEventRepo()
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	return inboundObservedState{
		standards:        standards,
		observedRepo:     observedRepo,
		declare:          &usecases.DeclareInboundLabourStandard{Repo: standards},
		getInboundDemand: &usecases.GetInboundDemand{Appointments: appointments, Standards: standards},
		inbound: &inboundkafka.InboundDemandConsumer{
			Record: &usecases.RecordInboundAppointment{
				UoW: memory.NewUnitOfWork(appointments, inboundProcessed), ProcessedEvents: inboundProcessed, Appointments: appointments,
			},
			Location: "SIM1", Logger: quiet,
		},
		completed: &inboundkafka.ObservedCapacityConsumer{
			Record: &usecases.RecordObservedCompletion{
				UoW: memory.NewUnitOfWork(observedRepo, observedProcessed), ProcessedEvents: observedProcessed, Observed: observedRepo,
			},
			Location: "SIM1", Logger: quiet,
		},
	}
}

func (w *world) nextIOID(prefix string) string {
	w.io.seq++
	return fmt.Sprintf("bdd-%s-%d", prefix, w.io.seq)
}

// inboundEvent builds a CloudEvents 1.0 inbound-receiving event and returns
// its JSON bytes.
func (w *world) inboundEvent(eventName, appointmentID string, at time.Time, data map[string]any) ([]byte, error) {
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(w.nextIOID("inbound-evt"))
	e.SetSource("/warehouse/inbound-receiving")
	e.SetType("com.warehouse.wms.inbound-receiving.dockappointment." + eventName)
	e.SetSubject(appointmentID)
	e.SetTime(at)
	e.SetDataSchema("urn:warehouse:inbound-receiving:events:" + eventName + ":v1")
	if err := e.SetData("application/json", data); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

func (w *world) handleInbound(value []byte) error {
	w.io.lastInbound = value
	return w.io.inbound.HandleMessage(context.Background(), value)
}

func (w *world) inboundReceivingPublishedBooked(appointmentID, start, end string, asns int, eventTime string) error {
	at, err := time.Parse(time.RFC3339, eventTime)
	if err != nil {
		return err
	}
	numbers := make([]string, 0, asns)
	for i := 1; i <= asns; i++ {
		numbers = append(numbers, fmt.Sprintf("ASN-%s-%d", appointmentID, i))
	}
	value, err := w.inboundEvent("DockAppointmentBooked", appointmentID, at, map[string]any{
		"appointment_id": appointmentID, "door_code": "SIM1-DOCK-IN-01", "carrier": "ACME Freight",
		"window_start": start, "window_end": end, "asn_numbers": numbers,
	})
	if err != nil {
		return err
	}
	return w.handleInbound(value)
}

func (w *world) inboundReceivingPublishedCancelled(appointmentID, eventTime string) error {
	at, err := time.Parse(time.RFC3339, eventTime)
	if err != nil {
		return err
	}
	value, err := w.inboundEvent("DockAppointmentCancelled", appointmentID, at, map[string]any{
		"appointment_id": appointmentID, "door_code": "SIM1-DOCK-IN-01", "reason": "Carrier delayed",
	})
	if err != nil {
		return err
	}
	return w.handleInbound(value)
}

func (w *world) inboundReceivingPublishedCheckedIn(appointmentID string) error {
	value, err := w.inboundEvent("DockAppointmentCheckedIn", appointmentID, orderBaseTime, map[string]any{
		"appointment_id": appointmentID, "door_code": "SIM1-DOCK-IN-01", "checked_in_at": orderBaseTime.Format(time.RFC3339),
	})
	if err != nil {
		return err
	}
	return w.handleInbound(value)
}

func (w *world) inboundReceivingRedelivers() error {
	if w.io.lastInbound == nil {
		return fmt.Errorf("no inbound-receiving event was published in this scenario")
	}
	return w.io.inbound.HandleMessage(context.Background(), w.io.lastInbound)
}

func (w *world) iDeclareAnInboundStandard(ctx context.Context, appointmentMinutes, asnMinutes float64, pathID, location string) error {
	return w.record(ctx, http.MethodPut, fmt.Sprintf("/inbound-labour-standards/%s/%s", location, pathID), map[string]any{
		"minutes_per_appointment": appointmentMinutes, "minutes_per_asn": asnMinutes,
	})
}

func (w *world) iDeclareAnInboundStandardWithoutAppointmentMinutes(ctx context.Context, pathID, location string) error {
	return w.record(ctx, http.MethodPut, fmt.Sprintf("/inbound-labour-standards/%s/%s", location, pathID), map[string]any{"minutes_per_asn": 5})
}

func (w *world) iRequestTheInboundDemand(ctx context.Context, location, pathID, start, end string) error {
	return w.record(ctx, http.MethodGet, fmt.Sprintf("/inbound-demand?location=%s&path_id=%s&window_start=%s&window_end=%s", location, pathID, start, end), nil)
}

func (w *world) iListTheInboundStandards(ctx context.Context, location string) error {
	return w.record(ctx, http.MethodGet, "/inbound-labour-standards?location="+location, nil)
}

func (w *world) decodeMap() (map[string]any, error) {
	var got map[string]any
	if err := w.decode(&got); err != nil {
		return nil, err
	}
	return got, nil
}

func (w *world) theInboundDemandIs(appointments, asns int, minutes float64) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	if got["appointments"] != float64(appointments) || got["asns"] != float64(asns) || got["labour_minutes"] != minutes || got["source"] != "inbound-receiving" {
		return fmt.Errorf("inbound demand = %v, want %d appointments, %d ASNs and %v labour minutes from inbound-receiving", got, appointments, asns, minutes)
	}
	return nil
}

func (w *world) theInboundAverageHeadsAre(heads float64) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	if got["average_heads"] != heads {
		return fmt.Errorf("average_heads = %v, want %v", got["average_heads"], heads)
	}
	return nil
}

func (w *world) theInboundStandardIs(state string) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	declared, missing := got["standard_declared"], got["standard_missing"]
	wantDeclared := state == "declared"
	wantMissing := state == "missing"
	if declared != wantDeclared || missing != wantMissing {
		return fmt.Errorf("standard_declared=%v standard_missing=%v, want the standard %s", declared, missing, state)
	}
	return nil
}

func (w *world) theInboundDemandAsOfIs(asOf string) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	if got["as_of"] != asOf {
		return fmt.Errorf("as_of = %v, want %s", got["as_of"], asOf)
	}
	return nil
}

func (w *world) theInboundStandardsListed(n int) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	list, _ := got["standards"].([]any)
	if len(list) != n {
		return fmt.Errorf("standards = %v, want %d", got["standards"], n)
	}
	return nil
}

func (w *world) theCapacityPlanShowsInbound(appointments int, minutes float64) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	if got["inbound_appointments"] != float64(appointments) || got["inbound_labour_minutes"] != minutes {
		return fmt.Errorf("plan inbound = %v appointments and %v minutes, want %d and %v (body %v)",
			got["inbound_appointments"], got["inbound_labour_minutes"], appointments, minutes, got)
	}
	return nil
}

func (w *world) theCapacityPlanReportsTheInboundStandard(state string) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	if want := state == "missing"; got["inbound_standard_missing"] != want {
		return fmt.Errorf("inbound_standard_missing = %v, want %v", got["inbound_standard_missing"], want)
	}
	return nil
}

func (w *world) theOrderShortageIsUnchangedByInbound(shortage float64) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	if got["shortage"] != shortage || got["demand_source"] != "request" {
		return fmt.Errorf("shortage = %v demand_source = %v, want %v from request: inbound labour is never summed into the order demand", got["shortage"], got["demand_source"], shortage)
	}
	return nil
}

// ---- observed capacity ------------------------------------------------------

func (w *world) taskCompletedEvent(taskType string, at time.Time) ([]byte, error) {
	taskID := w.nextIOID("task")
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(w.nextIOID("task-evt"))
	e.SetSource("/warehouse/fulfillment-execution")
	e.SetType("com.warehouse.wes.fulfillment-execution.task.TaskCompleted")
	e.SetSubject(taskID)
	e.SetTime(at)
	e.SetDataSchema("urn:warehouse:fulfillment-execution:events:TaskCompleted:v1")
	data := map[string]any{"task_id": taskID, "duration_seconds": 60}
	if taskType != "" {
		data["task_type"] = taskType
	}
	if err := e.SetData("application/json", data); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

func (w *world) fulfillmentPublishedCompletions(n int, taskType, eventTime string) error {
	at, err := time.Parse(time.RFC3339, eventTime)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		value, err := w.taskCompletedEvent(taskType, at)
		if err != nil {
			return err
		}
		w.io.lastTask = value
		if err := w.io.completed.HandleMessage(context.Background(), value); err != nil {
			return err
		}
	}
	return nil
}

func (w *world) fulfillmentRedelivers() error {
	if w.io.lastTask == nil {
		return fmt.Errorf("no fulfillment-execution event was published in this scenario")
	}
	return w.io.completed.HandleMessage(context.Background(), w.io.lastTask)
}

// fulfillmentPublishedUnattributable publishes a TaskCompleted without a
// task_type and asserts the consumer rejects it deterministically (the run
// loop dead-letters that, it never retries it nor counts it).
func (w *world) fulfillmentPublishedUnattributable() error {
	value, err := w.taskCompletedEvent("", orderBaseTime)
	if err != nil {
		return err
	}
	if err := w.io.completed.HandleMessage(context.Background(), value); err == nil {
		return fmt.Errorf("a TaskCompleted without task_type was accepted, want a deterministic (dead-letter) failure")
	}
	return nil
}

func (w *world) iRememberThePlan(ctx context.Context) error {
	if err := w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID, nil); err != nil {
		return err
	}
	w.io.rememberedPlan = bytes.Clone(w.body)
	w.io.rememberedOutbox = len(w.outbox.Messages())
	return nil
}

func (w *world) iRequestTheVariance(ctx context.Context) error {
	return w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID+"/variance", nil)
}

func (w *world) iRequestTheVarianceWithFactor(ctx context.Context, factor string) error {
	return w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID+"/variance?tasks_per_order="+factor, nil)
}

func (w *world) section(name string) (map[string]any, error) {
	got, err := w.decodeMap()
	if err != nil {
		return nil, err
	}
	section, _ := got[name].(map[string]any)
	if section == nil {
		return nil, fmt.Errorf("response has no %q object: %v", name, got)
	}
	return section, nil
}

func (w *world) theObservedThroughputIs(tasks int) error {
	observed, err := w.section("observed")
	if err != nil {
		return err
	}
	if observed["tasks"] != float64(tasks) || observed["has_data"] != true || observed["source"] != "fulfillment-execution" {
		return fmt.Errorf("observed = %v, want %d tasks with data from fulfillment-execution", observed, tasks)
	}
	return nil
}

func (w *world) theObservedSideHasNoData() error {
	observed, err := w.section("observed")
	if err != nil {
		return err
	}
	if observed["has_data"] != false || observed["tasks"] != float64(0) || observed["as_of"] != nil {
		return fmt.Errorf("observed = %v, want has_data false, 0 tasks and as_of null (never zero throughput)", observed)
	}
	return nil
}

func (w *world) thePlannedCapacityIs(perHour, overWindow float64) error {
	got, err := w.decodeMap()
	if err != nil {
		return err
	}
	if got["planned_orders_per_hour"] != perHour || got["planned_orders_over_window"] != overWindow || got["advisory"] != true {
		return fmt.Errorf("variance = %v, want planned %v/h and %v over the window, advisory true", got, perHour, overWindow)
	}
	return nil
}

func (w *world) theVarianceIs(observedOrders, orders, percent float64) error {
	comparison, err := w.section("comparison")
	if err != nil {
		return err
	}
	if comparison["comparable"] != true || comparison["observed_orders"] != observedOrders ||
		comparison["variance_orders"] != orders || comparison["variance_percent"] != percent {
		return fmt.Errorf("comparison = %v, want observed %v orders, variance %v orders, %v percent", comparison, observedOrders, orders, percent)
	}
	return nil
}

func (w *world) theVarianceIsNotComparable(reason string) error {
	comparison, err := w.section("comparison")
	if err != nil {
		return err
	}
	text, _ := comparison["reason"].(string)
	if comparison["comparable"] != false || !strings.Contains(text, reason) || comparison["variance_orders"] != nil {
		return fmt.Errorf("comparison = %v, want not comparable with a reason containing %q and null figures", comparison, reason)
	}
	return nil
}

func (w *world) thePlanAndOutboxAreUnchanged(ctx context.Context) error {
	if err := w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID, nil); err != nil {
		return err
	}
	if !bytes.Equal(w.io.rememberedPlan, w.body) {
		return fmt.Errorf("the capacity plan changed after the variance read:\nbefore %s\nafter  %s", w.io.rememberedPlan, w.body)
	}
	if n := len(w.outbox.Messages()); n != w.io.rememberedOutbox {
		return fmt.Errorf("the outbox holds %d messages after the variance read, want %d: it never republishes", n, w.io.rememberedOutbox)
	}
	return nil
}

func registerInboundObservedSteps(sc *godog.ScenarioContext, w *world) {
	const ts = `\"([^\"]*)\"`
	num := `(\d+(?:\.\d+)?)`
	sc.Step(`^inbound-receiving published DockAppointmentBooked for appointment `+ts+` from `+ts+` to `+ts+` covering (\d+) ASNs? at event time `+ts+`$`, w.inboundReceivingPublishedBooked)
	sc.Step(`^inbound-receiving published DockAppointmentCancelled for appointment `+ts+` at event time `+ts+`$`, w.inboundReceivingPublishedCancelled)
	sc.Step(`^inbound-receiving published DockAppointmentCheckedIn for appointment `+ts+`$`, w.inboundReceivingPublishedCheckedIn)
	sc.Step(`^inbound-receiving redelivers its last event$`, w.inboundReceivingRedelivers)
	sc.Step(`^I declare an inbound labour standard of `+num+` minutes per appointment and `+num+` minutes per ASN for path `+ts+` at `+ts+`$`, w.iDeclareAnInboundStandard)
	sc.Step(`^I declare an inbound labour standard without minutes per appointment for path `+ts+` at `+ts+`$`, w.iDeclareAnInboundStandardWithoutAppointmentMinutes)
	sc.Step(`^I list the inbound labour standards of `+ts+`$`, w.iListTheInboundStandards)
	sc.Step(`^I request the inbound demand for `+ts+` and path `+ts+` for the window `+ts+` to `+ts+`$`, w.iRequestTheInboundDemand)
	sc.Step(`^the inbound demand is (\d+) appointments? and (\d+) ASNs? with `+num+` labour minutes$`, w.theInboundDemandIs)
	sc.Step(`^the inbound average heads are `+num+`$`, w.theInboundAverageHeadsAre)
	sc.Step(`^the inbound standard is (declared|missing)$`, w.theInboundStandardIs)
	sc.Step(`^the inbound demand as of is `+ts+`$`, w.theInboundDemandAsOfIs)
	sc.Step(`^(\d+) inbound labour standards? (?:is|are) listed$`, w.theInboundStandardsListed)
	sc.Step(`^the capacity plan shows (\d+) inbound appointments? and `+num+` inbound labour minutes$`, w.theCapacityPlanShowsInbound)
	sc.Step(`^the capacity plan reports the inbound standard as (missing|not missing)$`, w.theCapacityPlanReportsTheInboundStandard)
	sc.Step(`^the order shortage is `+num+` and the inbound labour is not part of it$`, w.theOrderShortageIsUnchangedByInbound)

	sc.Step(`^fulfillment-execution published (\d+) TaskCompleted events? for task type `+ts+` at `+ts+`$`, w.fulfillmentPublishedCompletions)
	sc.Step(`^fulfillment-execution redelivers its last task event$`, w.fulfillmentRedelivers)
	sc.Step(`^fulfillment-execution published a TaskCompleted without a task type that is rejected as a poison message$`, w.fulfillmentPublishedUnattributable)
	sc.Step(`^I remember the capacity plan$`, w.iRememberThePlan)
	sc.Step(`^I request the variance of the capacity plan$`, w.iRequestTheVariance)
	sc.Step(`^I request the variance of the capacity plan with tasks_per_order `+num+`$`, w.iRequestTheVarianceWithFactor)
	sc.Step(`^the observed throughput is (\d+) tasks$`, w.theObservedThroughputIs)
	sc.Step(`^the observed side has no data$`, w.theObservedSideHasNoData)
	sc.Step(`^the planned capacity is `+num+` orders per hour and `+num+` over the window$`, w.thePlannedCapacityIs)
	sc.Step(`^the variance is `+num+` observed orders, (-?\d+(?:\.\d+)?) orders and (-?\d+(?:\.\d+)?) percent$`, w.theVarianceIs)
	sc.Step(`^the variance is not comparable because `+ts+`$`, w.theVarianceIsNotComparable)
	sc.Step(`^the capacity plan and the outbox are unchanged$`, w.thePlanAndOutboxAreUnchanged)
}
