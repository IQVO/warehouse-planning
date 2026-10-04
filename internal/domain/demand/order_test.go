package demand

import (
	"errors"
	"testing"
	"time"
)

var (
	winStart = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)
	winEnd   = time.Date(2026, 10, 5, 16, 0, 0, 0, time.UTC)
	t0       = time.Date(2026, 10, 4, 9, 15, 30, 0, time.UTC)
)

func mustOrder(t *testing.T, id string, promise time.Time, lines int, asOf time.Time) Order {
	t.Helper()
	o, err := NewOrder(OrderParams{OrderID: id, Location: "SIM1", PromiseAt: promise, ReleasedLines: lines, AsOf: asOf})
	if err != nil {
		t.Fatalf("NewOrder(%s): %v", id, err)
	}
	return o
}

func TestNewOrder_Validation(t *testing.T) {
	ok := OrderParams{OrderID: "ord-1", Location: "SIM1", PromiseAt: winStart, ReleasedLines: 0, AsOf: t0}
	cases := []struct {
		name   string
		mutate func(*OrderParams)
		want   error
	}{
		{"valid with zero released lines (the boundary)", func(*OrderParams) {}, nil},
		{"blank order id", func(p *OrderParams) { p.OrderID = "" }, ErrRequiredField},
		{"blank location", func(p *OrderParams) { p.Location = "" }, ErrRequiredField},
		{"missing promise", func(p *OrderParams) { p.PromiseAt = time.Time{} }, ErrPromiseRequired},
		{"missing as-of", func(p *OrderParams) { p.AsOf = time.Time{} }, ErrAsOfRequired},
		{"negative lines", func(p *OrderParams) { p.ReleasedLines = -1 }, ErrNegativeLines},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := ok
			tc.mutate(&p)
			o, err := NewOrder(p)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.want == nil && (o.ID() != "ord-1" || o.Location() != "SIM1") {
				t.Fatalf("order = %+v", o)
			}
		})
	}
}

func TestNewOrder_NormalizesToUTCAndExposesFields(t *testing.T) {
	zone := time.FixedZone("BRT", -3*3600)
	promise := time.Date(2026, 10, 5, 10, 0, 0, 0, zone) // 13:00Z
	asOf := time.Date(2026, 10, 4, 6, 15, 30, 0, zone)   // 09:15:30Z
	o, err := NewOrder(OrderParams{OrderID: "ord-7", Location: "SIM1", PromiseAt: promise, ReleasedLines: 3, AsOf: asOf})
	if err != nil {
		t.Fatal(err)
	}
	if o.ID() != "ord-7" || o.Location() != "SIM1" || o.ReleasedLines() != 3 {
		t.Fatalf("fields = %+v", o)
	}
	if !o.PromiseAt().Equal(promise) || o.PromiseAt().Location() != time.UTC {
		t.Fatalf("promise = %v (%v)", o.PromiseAt(), o.PromiseAt().Location())
	}
	if !o.AsOf().Equal(asOf) || o.AsOf().Location() != time.UTC {
		t.Fatalf("asOf = %v (%v)", o.AsOf(), o.AsOf().Location())
	}
}

// The window is half-open: a cutoff exactly at start counts, a cutoff
// exactly at end does not. Every boundary is tested at the value itself and
// one second either side.
func TestOrder_CountsIn_HalfOpenWindow(t *testing.T) {
	cases := []struct {
		name    string
		promise time.Time
		want    bool
	}{
		{"one second before start", winStart.Add(-time.Second), false},
		{"exactly at start counts", winStart, true},
		{"one second after start", winStart.Add(time.Second), true},
		{"middle", winStart.Add(4 * time.Hour), true},
		{"one second before end", winEnd.Add(-time.Second), true},
		{"exactly at end does not count", winEnd, false},
		{"one second after end", winEnd.Add(time.Second), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := mustOrder(t, "ord-b", tc.promise, 1, t0)
			if got := o.CountsIn(winStart, winEnd); got != tc.want {
				t.Fatalf("CountsIn(%v) = %v, want %v", tc.promise, got, tc.want)
			}
		})
	}
}

// Adjacent windows partition time: an order on the shared boundary is in
// exactly one of them.
func TestOrder_CountsIn_AdjacentWindowsNeverDoubleCount(t *testing.T) {
	boundary := winEnd
	o := mustOrder(t, "ord-adj", boundary, 1, t0)
	nextEnd := boundary.Add(8 * time.Hour)
	if o.CountsIn(winStart, boundary) {
		t.Fatal("counted in the window that ENDS at its cutoff")
	}
	if !o.CountsIn(boundary, nextEnd) {
		t.Fatal("not counted in the window that STARTS at its cutoff")
	}
}

func TestOrder_Supersedes_LastWriterWinsOnEventTime(t *testing.T) {
	prev := mustOrder(t, "ord-1", winStart, 1, t0)
	cases := []struct {
		name string
		asOf time.Time
		want bool
	}{
		{"older does not replace", t0.Add(-time.Nanosecond), false},
		{"equal replaces", t0, true},
		{"newer replaces", t0.Add(time.Nanosecond), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next := mustOrder(t, "ord-1", winEnd, 2, tc.asOf)
			if got := next.Supersedes(prev); got != tc.want {
				t.Fatalf("Supersedes = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSummarize_CountsWindowAndSumsLines(t *testing.T) {
	newest := t0.Add(90 * time.Minute)
	orders := []Order{
		mustOrder(t, "in-1", winStart, 2, t0),                                 // boundary: counts
		mustOrder(t, "in-2", winStart.Add(3*time.Hour), 3, t0.Add(time.Hour)), // counts
		mustOrder(t, "in-3", winEnd.Add(-time.Second), 5, t0.Add(time.Minute)),
		mustOrder(t, "out-end", winEnd, 11, t0.Add(2*time.Minute)),                 // boundary: not counted
		mustOrder(t, "out-before", winStart.Add(-time.Second), 13, newest),         // newest event, outside the window
		mustOrder(t, "out-far", winEnd.Add(24*time.Hour), 17, t0.Add(time.Hour+1)), // not counted
	}
	got := Summarize(orders, winStart, winEnd)
	if got.Orders != 3 {
		t.Fatalf("Orders = %d, want 3", got.Orders)
	}
	if got.ReleasedLines != 2+3+5 {
		t.Fatalf("ReleasedLines = %d, want 10", got.ReleasedLines)
	}
	if !got.AsOf.Equal(newest) {
		t.Fatalf("AsOf = %v, want the newest event of the site %v (even outside the window)", got.AsOf, newest)
	}
	if !got.HasData() {
		t.Fatal("HasData = false with 3 orders")
	}
}

func TestSummarize_EmptyAndOutOfWindowIsNoData(t *testing.T) {
	empty := Summarize(nil, winStart, winEnd)
	if empty.Orders != 0 || empty.ReleasedLines != 0 || !empty.AsOf.IsZero() || empty.HasData() {
		t.Fatalf("empty summary = %+v", empty)
	}

	outside := Summarize([]Order{mustOrder(t, "only", winEnd, 4, t0)}, winStart, winEnd)
	if outside.HasData() || outside.Orders != 0 {
		t.Fatalf("an order exactly at the window end must not make data: %+v", outside)
	}
	if !outside.AsOf.Equal(t0) {
		t.Fatalf("AsOf = %v, want %v: freshness covers the whole site", outside.AsOf, t0)
	}
}
