package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"

	"github.com/cucumber/godog"
)

// Documentation-derived steps (warehouse-docs: aggregate-design-canvas,
// domain-events, troubleshooting, ubiquitous-language). Facts that arrive from
// Kafka are fed through the REAL consumers' HandleMessage with the raw payload
// the scenario names, so a payload the consumer must refuse is a payload the
// scenario actually sends; everything is asserted through REST or the outbox.

// docRulesState is the per-scenario state of these steps.
type docRulesState struct {
	// replayed is the Idempotency-Replayed header of the last keyed request.
	replayed string
	// firstAnswer remembers the first response body per Idempotency-Key.
	firstAnswer map[string][]byte
	// noted maps a scenario nickname to a capacity plan id.
	noted map[string]string
	// listBase is the last paged list request without its cursor.
	listBase string
}

func newDocRulesState() docRulesState {
	return docRulesState{firstAnswer: map[string][]byte{}, noted: map[string]string{}}
}

// ------------------------------------------------------- keyed requests ----

// recordKeyed sends a request with an explicit Idempotency-Key (none when key
// is empty) and keeps the replay header, which the typed steps discard.
func (w *world) recordKeyed(ctx context.Context, method, path, key, body string) error {
	req, err := http.NewRequestWithContext(ctx, method, w.server.URL+path, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
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
	w.doc.replayed = resp.Header.Get("Idempotency-Replayed")
	if key != "" {
		if _, seen := w.doc.firstAnswer[key]; !seen {
			w.doc.firstAnswer[key] = raw
		}
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

func (w *world) iSendWithKey(ctx context.Context, method, path, key string, doc *godog.DocString) error {
	return w.recordKeyed(ctx, method, path, key, doc.Content)
}

func (w *world) iSendWithoutKey(ctx context.Context, path string, doc *godog.DocString) error {
	return w.recordKeyed(ctx, http.MethodPost, path, "", doc.Content)
}

func (w *world) theResponseIsAReplay(kind string) error {
	want := ""
	if kind == "is" {
		want = "true"
	}
	if w.doc.replayed != want {
		return fmt.Errorf("Idempotency-Replayed = %q, want %q (response %d: %s)", w.doc.replayed, want, w.status, string(w.body))
	}
	return nil
}

func (w *world) theResponseIsTheFirstAnswerFor(key string) error {
	first, ok := w.doc.firstAnswer[key]
	if !ok {
		return fmt.Errorf("no answer was recorded for the Idempotency-Key %q", key)
	}
	if string(first) != string(w.body) {
		return fmt.Errorf("replay answered %s, want the first answer %s", string(w.body), string(first))
	}
	return nil
}

// ----------------------------------------------------------- noted plans ----

func (w *world) iNoteThePlan(name string) error {
	if w.planID == "" {
		return fmt.Errorf("no capacity plan was created yet")
	}
	w.doc.noted[name] = w.planID
	return nil
}

func (w *world) notedID(name string) (string, error) {
	id, ok := w.doc.noted[name]
	if !ok {
		return "", fmt.Errorf("no capacity plan was noted as %q", name)
	}
	return id, nil
}

func (w *world) iPublishTheNotedPlan(ctx context.Context, name string) error {
	id, err := w.notedID(name)
	if err != nil {
		return err
	}
	return w.record(ctx, http.MethodPost, "/capacity-plans/"+id+"/publications", nil)
}

func (w *world) theNotedPlanIs(ctx context.Context, name, status string) error {
	id, err := w.notedID(name)
	if err != nil {
		return err
	}
	if err := w.record(ctx, http.MethodGet, "/capacity-plans/"+id, nil); err != nil {
		return err
	}
	var plan struct {
		Status string `json:"status"`
	}
	if err := w.decode(&plan); err != nil {
		return err
	}
	if plan.Status != status {
		return fmt.Errorf("plan %q (%s) is %s, want %s", name, id, plan.Status, status)
	}
	return nil
}

func (w *world) theNotedPlansDiffer(a, b string) error {
	ida, err := w.notedID(a)
	if err != nil {
		return err
	}
	idb, err := w.notedID(b)
	if err != nil {
		return err
	}
	if ida == idb {
		return fmt.Errorf("plans %q and %q are the same plan %s, want two plans", a, b, ida)
	}
	return nil
}

// -------------------------------------------------------------- outbox ----

// queuedEnvelope is the structured-mode CloudEvent of one outbox row.
type queuedEnvelope map[string]any

func (w *world) lastQueued(name string) (queuedEnvelope, error) {
	var found queuedEnvelope
	for _, m := range w.outbox.Messages() {
		if !strings.HasSuffix(m.EventType, "."+name) {
			continue
		}
		var env queuedEnvelope
		if err := json.Unmarshal(m.Value, &env); err != nil {
			return nil, err
		}
		found = env
	}
	if found == nil {
		return nil, fmt.Errorf("no %s event in the outbox", name)
	}
	return found, nil
}

func (w *world) theLastProcessCapacityEventHas(name, field, raw string) error {
	env, err := w.lastQueued(name)
	if err != nil {
		return err
	}
	var want any
	if err := json.Unmarshal([]byte(raw), &want); err != nil {
		return fmt.Errorf("expected value %s is not JSON: %w", raw, err)
	}
	data, _ := env["data"].(map[string]any)
	if got := data[field]; !reflect.DeepEqual(got, want) {
		return fmt.Errorf("%s: data.%s = %v, want %v (data %v)", name, field, got, want, data)
	}
	return nil
}

func (w *world) theProcessCapacityEventsAreKeyed(id string) error {
	n := 0
	for _, m := range w.outbox.Messages() {
		if !strings.Contains(m.EventType, ".processcapacity.") {
			continue
		}
		n++
		if m.Subject != id || string(m.Key) != id {
			return fmt.Errorf("%s: subject %q / key %q, want %q", m.EventType, m.Subject, string(m.Key), id)
		}
	}
	if n == 0 {
		return fmt.Errorf("no ProcessCapacity event in the outbox")
	}
	return nil
}

func (w *world) theEventHasAttribute(name, attr, want string) error {
	env, err := w.lastQueued(name)
	if err != nil {
		return err
	}
	if got := fmt.Sprint(env[attr]); got != want {
		return fmt.Errorf("%s: CloudEvents %s = %q, want %q", name, attr, got, want)
	}
	return nil
}

func (w *world) theEventOfThePlanHasNoField(name, field string) error {
	env, err := w.lastQueued(name)
	if err != nil {
		return err
	}
	data, _ := env["data"].(map[string]any)
	if _, present := data[field]; present {
		return fmt.Errorf("%s carries data.%s = %v, want the field absent", name, field, data[field])
	}
	return nil
}

func (w *world) theOutboxIsEmpty() error {
	if got := w.outbox.Messages(); len(got) != 0 {
		return fmt.Errorf("the outbox holds %d messages (first: %s), want none", len(got), got[0].EventType)
	}
	return nil
}

func (w *world) everyQueuedEventHasItsOwnID() error {
	seen := map[string]bool{}
	for _, m := range w.outbox.Messages() {
		var env queuedEnvelope
		if err := json.Unmarshal(m.Value, &env); err != nil {
			return err
		}
		id, _ := env["id"].(string)
		if id == "" || id != m.EventID {
			return fmt.Errorf("%s: CloudEvents id %q, row id %q: want them equal and non-empty", m.EventType, id, m.EventID)
		}
		if seen[id] {
			return fmt.Errorf("CloudEvents id %s is used by two queued events", id)
		}
		seen[id] = true
	}
	if len(seen) == 0 {
		return fmt.Errorf("the outbox is empty")
	}
	return nil
}

// ---------------------------------------------------- Kafka: raw events ----

func (w *world) facilityRaw(typ, eventID, subject string, data string) error {
	var payload any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return fmt.Errorf("scenario data %s is not JSON: %w", data, err)
	}
	value, err := cloudEventBytes(eventID, "/warehouse/facility-layout", typ, subject, time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC), payload)
	if err != nil {
		return err
	}
	return w.facility.HandleMessage(context.Background(), value)
}

func (w *world) facilityAnnouncedRaw(eventID, data string) error {
	return w.facilityRaw(typeLocationSlotRegistered, eventID, eventID, data)
}

func (w *world) facilityDecommissionedAs(code, eventID string) error {
	return w.facilityRaw(typeLocationSlotDecommissioned, eventID, code, fmt.Sprintf(`{"locationCode":%q}`, code))
}

func (w *world) facilityTopicGarbage() error {
	return w.facility.HandleMessage(context.Background(), []byte(`{"event_id":"legacy","event_type":"LocationSlotRegistered","payload":{}}`))
}

func (w *world) facilityUnrelated(typ string) error {
	return w.facilityRaw(typ, "unrelated-1", "x", `{"locationCode":"SIM1-OPS-WC-999","zoneId":"SIM1-OPS-WC","role":"WorkCenter","activities":["PACK"]}`)
}

func (w *world) workforceRaw(eventID, start, data string) error {
	var payload any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return fmt.Errorf("scenario data %s is not JSON: %w", data, err)
	}
	at, err := time.Parse(time.RFC3339, start)
	if err != nil {
		return err
	}
	value, err := cloudEventBytes(eventID, "/warehouse/workforce-management", typeShiftPlanCommitted, eventID, at, payload)
	if err != nil {
		return err
	}
	// A deterministic rejection is acknowledged (nil), never retried.
	return w.w2.labor.HandleMessage(context.Background(), value)
}

func (w *world) orderRaw(eventName, eventID, subject, at, data string) error {
	var payload any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		return fmt.Errorf("scenario data %s is not JSON: %w", data, err)
	}
	when, err := time.Parse(time.RFC3339, at)
	if err != nil {
		return err
	}
	value, err := cloudEventBytes(eventID, "/warehouse/order-management", "com.warehouse.wes.order-management.order."+eventName, subject, when, payload)
	if err != nil {
		return err
	}
	return w.orders.HandleMessage(context.Background(), value)
}

// -------------------------------------------------------- inbound / lists ----

func (w *world) iDeclareAnInboundStandardWithNegativeASN(ctx context.Context, appointmentMinutes, asnMinutes float64, pathID, location string) error {
	return w.record(ctx, http.MethodPut, fmt.Sprintf("/inbound-labour-standards/%s/%s", location, pathID), map[string]any{
		"minutes_per_appointment": appointmentMinutes, "minutes_per_asn": asnMinutes,
	})
}

func (w *world) iListThePlansWithLimit(ctx context.Context, limit string) error {
	w.doc.listBase = "/capacity-plans?limit=" + limit
	return w.record(ctx, http.MethodGet, w.doc.listBase, nil)
}

func (w *world) iFollowTheNextCursor(ctx context.Context) error {
	var page struct {
		NextCursor string `json:"nextCursor"`
	}
	if err := w.decode(&page); err != nil {
		return err
	}
	if page.NextCursor == "" || w.doc.listBase == "" {
		return fmt.Errorf("the last list has no nextCursor to follow: %s", string(w.body))
	}
	return w.record(ctx, http.MethodGet, w.doc.listBase+"&cursor="+url.QueryEscape(page.NextCursor), nil)
}

// dockBookingWithoutASNIsPoison publishes an INBOUND booking that covers no ASN
// and asserts the consumer rejects it deterministically (the run loop
// dead-letters that: it never retries it nor counts it).
func (w *world) dockBookingWithoutASNIsPoison(appointmentID, site, start, end string) error {
	err := w.dockSchedulingPublishedBooked(1, "INBOUND", appointmentID, site, start, end, 0, "2026-10-04T09:00:00Z")
	if err == nil {
		return fmt.Errorf("an INBOUND booking without any ASN was accepted, want a deterministic (dead-letter) failure")
	}
	if !strings.Contains(err.Error(), "at least one ASN") {
		return fmt.Errorf("the booking was rejected for another reason: %w", err)
	}
	return nil
}

// theObservedWindowIsEmpty asserts no completion fell inside the plan window.
// The newest completion the model knows (as_of) may be outside it, so unlike
// "the observed side has no data" this does not demand a null as_of.
func (w *world) theObservedWindowIsEmpty() error {
	section, err := w.section("observed")
	if err != nil {
		return err
	}
	if section["has_data"] != false || section["tasks"] != float64(0) {
		return fmt.Errorf("observed = %v, want has_data false and 0 tasks inside the plan window", section)
	}
	return nil
}

func (w *world) iRequestTheVarianceWithRawFactor(ctx context.Context, raw string) error {
	return w.record(ctx, http.MethodGet, "/capacity-plans/"+w.planID+"/variance?tasks_per_order="+url.QueryEscape(raw), nil)
}

func (w *world) theResponseHasANextCursor(kind string) error {
	var page struct {
		NextCursor string `json:"nextCursor"`
	}
	if err := w.decode(&page); err != nil {
		return err
	}
	if has := page.NextCursor != ""; has != (kind == "a") {
		return fmt.Errorf("nextCursor = %q, want %s next cursor: %s", page.NextCursor, kind, string(w.body))
	}
	return nil
}

// ------------------------------------------------------------- wiring ----

func registerDocRulesSteps(sc *godog.ScenarioContext, w *world) {
	const (
		ts      = `\"([^\"]*)\"`
		num     = `(\d+(?:\.\d+)?)`
		jsonVal = `(\"[^\"]*\"|-?\d+(?:\.\d+)?|true|false|null)`
		anyEv   = `(CapacityPlanCreated|CapacityPlanPublished|CapacityShortageDetected|BottleneckDetected|ProcessCapacityRegistered|ProcessCapacityChanged)`
		planEv  = `(CapacityPlanCreated|CapacityPlanPublished|CapacityShortageDetected|BottleneckDetected)`
		pcEv    = `(ProcessCapacityRegistered|ProcessCapacityChanged)`
	)

	sc.Step(`^I send (POST|PUT) `+ts+` with the Idempotency-Key `+ts+` and body:$`, w.iSendWithKey)
	sc.Step(`^I send POST `+ts+` without an Idempotency-Key and body:$`, w.iSendWithoutKey)
	sc.Step(`^the response (is|is not) an idempotent replay$`, w.theResponseIsAReplay)
	sc.Step(`^the response is the first answer given for the Idempotency-Key `+ts+`$`, w.theResponseIsTheFirstAnswerFor)

	sc.Step(`^I note the capacity plan as `+ts+`$`, w.iNoteThePlan)
	sc.Step(`^I publish the noted capacity plan `+ts+`$`, w.iPublishTheNotedPlan)
	sc.Step(`^the noted capacity plan `+ts+` is (DRAFT|PUBLISHED|SUPERSEDED)$`, w.theNotedPlanIs)
	sc.Step(`^the noted capacity plans `+ts+` and `+ts+` are different plans$`, w.theNotedPlansDiffer)

	sc.Step(`^the last `+pcEv+` event has `+`([a-z_]+) `+jsonVal+`$`, w.theLastProcessCapacityEventHas)
	sc.Step(`^every ProcessCapacity event is keyed by `+ts+`$`, w.theProcessCapacityEventsAreKeyed)
	sc.Step(`^the `+anyEv+` event has the CloudEvents attribute `+ts+` `+ts+`$`, w.theEventHasAttribute)
	sc.Step(`^the `+planEv+` event of the plan has no field `+ts+`$`, w.theEventOfThePlanHasNoField)
	sc.Step(`^every queued event has its own CloudEvents id$`, w.everyQueuedEventHasItsOwnID)
	sc.Step(`^the outbox is empty$`, w.theOutboxIsEmpty)

	sc.Step(`^facility-layout announced a LocationSlotRegistered as event `+ts+` with data (.+)$`, w.facilityAnnouncedRaw)
	sc.Step(`^facility-layout decommissioned the slot `+ts+` as event `+ts+`$`, w.facilityDecommissionedAs)
	sc.Step(`^the facility topic carries a message that is not a CloudEvent$`, w.facilityTopicGarbage)
	sc.Step(`^facility-layout published an unrelated event of type `+ts+`$`, w.facilityUnrelated)
	sc.Step(`^workforce-management delivered a ShiftPlanCommitted as event `+ts+` starting `+ts+` with data (.+)$`, w.workforceRaw)
	sc.Step(`^order-management delivered an? (OrderAllocated|OrderPartiallyAllocated) event `+ts+` about subject `+ts+` at `+ts+` with data (.+)$`, w.orderRaw)

	sc.Step(`^I declare an inbound labour standard of `+num+` minutes per appointment and (-\d+(?:\.\d+)?) minutes per ASN for path `+ts+` at `+ts+`$`, w.iDeclareAnInboundStandardWithNegativeASN)
	sc.Step(`^I list the capacity plans of every location with limit `+ts+`$`, w.iListThePlansWithLimit)
	sc.Step(`^dock-scheduling published an INBOUND booking `+ts+` at site `+ts+` from `+ts+` to `+ts+` covering no ASN that is rejected as a poison message$`, w.dockBookingWithoutASNIsPoison)
	sc.Step(`^no completion fell inside the plan window$`, w.theObservedWindowIsEmpty)
	sc.Step(`^I follow the next cursor$`, w.iFollowTheNextCursor)
	sc.Step(`^I request the variance of the capacity plan with the tasks_per_order `+ts+`$`, w.iRequestTheVarianceWithRawFactor)
	sc.Step(`^the response has (a|no) next cursor$`, w.theResponseHasANextCursor)
}
