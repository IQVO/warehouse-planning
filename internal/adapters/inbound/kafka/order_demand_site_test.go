package kafka_test

import (
	"context"
	"testing"
	"time"
)

// ADR 0021: order-management's OrderAllocated / OrderPartiallyAllocated carry
// an optional top-level site_id (its ADR 0050). The consumer attributes the
// order to that site and falls back to the configured DEMAND_SITE_ID only when
// the event carries none.

func allocationDataAtSite(orderID, promise string, lines int, site string) map[string]any {
	d := allocationData(orderID, promise, lines)
	d["site_id"] = site
	return d
}

func TestOrderDemandConsumer_AnEventSiteWinsOverTheConfiguredSite(t *testing.T) {
	h := newOrderHarness()
	msg := orderWireEvent(t, "evt-s", typeAllocated, orderIDOne, eventTime, allocationDataAtSite(orderIDOne, promiseRFC3339, 2, "WH2"))

	if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}

	o, ok := h.repo.Get(orderIDOne)
	if !ok {
		t.Fatal("order not in the model")
	}
	if o.Location() != "WH2" {
		t.Fatalf("location = %q, want the event's site WH2", o.Location())
	}
	if s := h.expected(t); s.Orders != 0 {
		t.Fatalf("configured site %s summary = %+v, want no order (it belongs to WH2)", demandSiteID, s)
	}
	s, err := h.repo.Expected(context.Background(), "WH2", windowFrom, windowTo)
	if err != nil {
		t.Fatal(err)
	}
	if s.Orders != 1 || s.ReleasedLines != 2 {
		t.Fatalf("WH2 summary = %+v, want 1 order with 2 lines", s)
	}
}

func TestOrderDemandConsumer_ABlankEventSiteFallsBackToTheConfiguredSite(t *testing.T) {
	for name, site := range map[string]string{"empty": "", "whitespace": " \t\n"} {
		t.Run(name, func(t *testing.T) {
			h := newOrderHarness()
			msg := orderWireEvent(t, "evt-b", typePartially, orderIDOne, eventTime, allocationDataAtSite(orderIDOne, promiseRFC3339, 1, site))
			if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
				t.Fatalf("HandleMessage: %v", err)
			}
			o, ok := h.repo.Get(orderIDOne)
			if !ok || o.Location() != demandSiteID {
				t.Fatalf("order = %+v (found %v), want the configured site %s", o, ok, demandSiteID)
			}
		})
	}
}

func TestOrderDemandConsumer_AnEventSiteIsUsedExactlyAsGivenApartFromSurroundingSpace(t *testing.T) {
	h := newOrderHarness()
	msg := orderWireEvent(t, "evt-t", typeAllocated, orderIDOne, eventTime, allocationDataAtSite(orderIDOne, promiseRFC3339, 1, " DC7 "))
	if err := h.consumer.HandleMessage(context.Background(), msg); err != nil {
		t.Fatalf("HandleMessage: %v", err)
	}
	if o, _ := h.repo.Get(orderIDOne); o.Location() != "DC7" {
		t.Fatalf("location = %q, want DC7", o.Location())
	}
}

func TestOrderDemandConsumer_ANewerAllocationMovesTheOrderToItsNewSite(t *testing.T) {
	h := newOrderHarness()
	ctx := context.Background()
	first := orderWireEvent(t, "evt-1", typePartially, orderIDOne, eventTime, allocationData(orderIDOne, promiseRFC3339, 1))
	later := orderWireEvent(t, "evt-2", typeAllocated, orderIDOne, eventTime.Add(time.Hour), allocationDataAtSite(orderIDOne, promiseRFC3339, 2, "WH2"))
	for _, msg := range [][]byte{first, later} {
		if err := h.consumer.HandleMessage(ctx, msg); err != nil {
			t.Fatalf("HandleMessage: %v", err)
		}
	}
	if s := h.expected(t); s.Orders != 0 {
		t.Fatalf("configured site summary = %+v, want the order moved away", s)
	}
	if o, _ := h.repo.Get(orderIDOne); o.Location() != "WH2" {
		t.Fatalf("location = %q, want WH2", o.Location())
	}
}
