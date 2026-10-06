package main_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	ce "github.com/cloudevents/sdk-go/v2/event"
	"github.com/cucumber/godog"
)

// orderBaseTime is the CloudEvents time of the first order event of a
// scenario; every further event is one minute later unless a step says
// otherwise, so "later" events really are later.
var orderBaseTime = time.Date(2026, 10, 4, 9, 0, 0, 0, time.UTC)

// publishOrderEvent hands one order-management CloudEvent to the REAL
// consumer's HandleMessage, exactly the bytes the Kafka loop would.
func (w *world) publishOrderEvent(eventName, orderID, promise string, lines int, at time.Time) error {
	w.orderSeq++
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(fmt.Sprintf("bdd-order-evt-%d", w.orderSeq))
	e.SetSource("/warehouse/order-management")
	e.SetType("com.warehouse.wes.order-management.order." + eventName)
	e.SetSubject(orderID)
	e.SetTime(at)
	e.SetDataSchema("urn:warehouse:order-management:events:" + eventName + ":v1")
	released := make([]map[string]any, 0, lines)
	for i := 1; i <= lines; i++ {
		released = append(released, map[string]any{"line_no": i, "sku": "SKU-1", "path_id": "pick", "gift_wrap": false, "fulfillment_class": "SINGLE"})
	}
	if err := e.SetData("application/json", map[string]any{"order_id": orderID, "promise_date": promise, "lines": released}); err != nil {
		return err
	}
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return w.orders.HandleMessage(context.Background(), value)
}

func (w *world) nextOrderTime() time.Time {
	return orderBaseTime.Add(time.Duration(w.orderSeq) * time.Minute)
}

func (w *world) orderManagementPublished(eventName, orderID, promise string, lines int) error {
	return w.publishOrderEvent(eventName, orderID, promise, lines, w.nextOrderTime())
}

func (w *world) orderManagementPublishedAt(eventName, orderID, promise string, lines int, eventTime string) error {
	at, err := time.Parse(time.RFC3339, eventTime)
	if err != nil {
		return err
	}
	return w.publishOrderEvent(eventName, orderID, promise, lines, at)
}

func (w *world) orderManagementPublishedMany(n int, promise string) error {
	for i := 1; i <= n; i++ {
		if err := w.orderManagementPublished("OrderAllocated", fmt.Sprintf("ord-bulk-%03d", i), promise, 1); err != nil {
			return err
		}
	}
	return nil
}

func (w *world) orderManagementPublishedRepromised(orderID string) error {
	w.orderSeq++
	e := ce.New(ce.CloudEventsVersionV1)
	e.SetID(fmt.Sprintf("bdd-order-evt-%d", w.orderSeq))
	e.SetSource("/warehouse/order-management")
	e.SetType("com.warehouse.wes.order-management.order.OrderRepromised")
	e.SetSubject(orderID)
	e.SetTime(w.nextOrderTime())
	if err := e.SetData("application/json", map[string]any{"order_id": orderID, "cpt_id_old": "sp1-1200", "cpt_id_new": "sp1-1800", "reason": "TaskCPTMissed"}); err != nil {
		return err
	}
	value, err := json.Marshal(e)
	if err != nil {
		return err
	}
	return w.orders.HandleMessage(context.Background(), value)
}

func (w *world) orderManagementPublishedGarbage() error {
	return w.orders.HandleMessage(context.Background(), []byte(`{"event_id":"legacy","event_type":"OrderAllocated","payload":{}}`))
}

func (w *world) iRequestTheExpectedDemand(ctx context.Context, location, start, end string) error {
	return w.record(ctx, http.MethodGet, fmt.Sprintf("/demand?location=%s&window_start=%s&window_end=%s", location, start, end), nil)
}

func (w *world) iCreateAPlanWithoutDemand(ctx context.Context, warehouse, site, location, pathID, start, end string, units, packages float64) error {
	if err := w.record(ctx, http.MethodPost, "/capacity-plans", map[string]any{
		"warehouse_id": warehouse, "site_id": site, "location": location, "path_id": pathID,
		"window_start": start, "window_end": end,
		"units_per_order": units, "packages_per_order": packages,
	}); err != nil {
		return err
	}
	if w.status == http.StatusCreated {
		var plan struct {
			ID string `json:"id"`
		}
		if err := w.decode(&plan); err != nil {
			return err
		}
		w.planID = plan.ID // the publish/get steps act on the plan just created
	}
	return nil
}

func (w *world) theOutboxIsEmpty() error {
	if n := len(w.outbox.Messages()); n != 0 {
		return fmt.Errorf("the outbox holds %d messages, want none: a rejected request queues nothing", n)
	}
	return nil
}

func (w *world) theExpectedDemandIs(orders, lines int) error {
	var got map[string]any
	if err := w.decode(&got); err != nil {
		return err
	}
	if got["orders"] != float64(orders) || got["released_lines"] != float64(lines) || got["source"] != "order-management" {
		return fmt.Errorf("expected demand = %v, want %d orders and %d released lines from order-management", got, orders, lines)
	}
	return nil
}

func (w *world) theExpectedDemandAsOfIs(asOf string) error {
	var got map[string]any
	if err := w.decode(&got); err != nil {
		return err
	}
	value, present := got["as_of"]
	if !present {
		return fmt.Errorf("as_of is missing from %v", got)
	}
	if asOf == "null" {
		if value != nil {
			return fmt.Errorf("as_of = %v, want null", value)
		}
		return nil
	}
	if value != asOf {
		return fmt.Errorf("as_of = %v, want %s", value, asOf)
	}
	return nil
}

func (w *world) theCapacityPlanUsesDemand(demand float64, source string, shortage float64) error {
	var plan map[string]any
	if err := w.decode(&plan); err != nil {
		return err
	}
	if plan["assigned_demand"] != demand || plan["demand_source"] != source || plan["shortage"] != shortage {
		return fmt.Errorf("plan = demand %v source %v shortage %v, want demand %v source %s shortage %v",
			plan["assigned_demand"], plan["demand_source"], plan["shortage"], demand, source, shortage)
	}
	return nil
}

func (w *world) theStoredPlanRemembersItsDemandSource(source string) error {
	var plan map[string]any
	if err := w.decode(&plan); err != nil {
		return err
	}
	id, _ := plan["id"].(string)
	if err := w.record(context.Background(), http.MethodGet, "/capacity-plans/"+id, nil); err != nil {
		return err
	}
	var stored map[string]any
	if err := w.decode(&stored); err != nil {
		return err
	}
	if stored["demand_source"] != source {
		return fmt.Errorf("GET /capacity-plans/%s demand_source = %v, want %s", id, stored["demand_source"], source)
	}
	return nil
}

func registerDemandSteps(sc *godog.ScenarioContext, w *world) {
	const ts = `\"([^\"]*)\"`
	sc.Step(`^order-management published (OrderAllocated|OrderPartiallyAllocated) for order `+ts+` promised at `+ts+` with (\d+) released lines?$`, w.orderManagementPublished)
	sc.Step(`^order-management published (OrderAllocated|OrderPartiallyAllocated) for order `+ts+` promised at `+ts+` with (\d+) released lines? at event time `+ts+`$`, w.orderManagementPublishedAt)
	sc.Step(`^order-management published (\d+) orders promised at `+ts+`$`, w.orderManagementPublishedMany)
	sc.Step(`^order-management published an OrderRepromised for order `+ts+`$`, w.orderManagementPublishedRepromised)
	sc.Step(`^order-management topic carries a message that is not a CloudEvent$`, w.orderManagementPublishedGarbage)

	sc.Step(`^I request the expected demand for `+ts+` for the window `+ts+` to `+ts+`$`, w.iRequestTheExpectedDemand)
	sc.Step(`^I create a capacity plan for warehouse `+ts+` at site `+ts+` in location `+ts+` on path `+ts+` for the window `+ts+` to `+ts+` without assigned demand, units_per_order (\d+(?:\.\d+)?) and packages_per_order (\d+(?:\.\d+)?)$`, w.iCreateAPlanWithoutDemand)
	sc.Step(`^the expected demand is (\d+) orders? and (\d+) released lines?$`, w.theExpectedDemandIs)
	sc.Step(`^the expected demand as of is (null|`+ts+`)$`, func(raw string) error { return w.theExpectedDemandAsOfIs(strings.Trim(raw, `"`)) })
	sc.Step(`^the capacity plan uses demand (\d+(?:\.\d+)?) from (orders|request) with shortage (\d+(?:\.\d+)?)$`, w.theCapacityPlanUsesDemand)
	sc.Step(`^the stored capacity plan remembers its demand source is (orders|request)$`, w.theStoredPlanRemembersItsDemandSource)
	sc.Step(`^the outbox is empty$`, w.theOutboxIsEmpty)
}
