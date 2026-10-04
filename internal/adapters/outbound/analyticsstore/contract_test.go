package analyticsstore_test

import (
	"context"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

// This file is the CONTRACT every analytical store must satisfy; the same
// function runs against the in-memory twin (always) and the real Postgres
// store (postgres_integration_test.go, -tags=integration). The fixtures use
// distinct non-zero numbers, plans that sit exactly ON the range bounds, a
// plan seen only through CapacityPlanPublished and events applied in
// "wrong" orders, so a naive implementation fails somewhere.

func at(day, hour, min, sec int) time.Time {
	return time.Date(2026, 10, day, hour, min, sec, 0, time.UTC)
}

func ptr[T any](v T) *T { return &v }

var (
	siteA = report.Site{WarehouseID: "WH-1", Location: "SIM1"}
	siteB = report.Site{WarehouseID: "WH-2", Location: "SIM2"}
	siteC = report.Site{WarehouseID: "WH-3", Location: "SIM3"}
)

// contractRange is [Oct 5 00:00:00, Oct 7 00:00:00) UTC.
var contractRange = report.Range{From: at(5, 0, 0, 0), To: at(7, 0, 0, 0)}

type planFixture struct {
	id                string
	site              report.Site
	step, constraint  string
	shortage          float64
	created, publishd *time.Time
}

func contractPlans() []planFixture {
	return []planFixture{
		// Site A, Oct 5: latencies 600, 180, 1800, 20 s; shortage on two of four.
		{"p1", siteA, "PACK", "STATION", 5600, ptr(at(5, 8, 0, 0)), ptr(at(5, 8, 10, 0))},
		{"p2", siteA, "PACK", "STATION", 0, ptr(at(5, 9, 0, 0)), ptr(at(5, 9, 3, 0))},
		{"p3", siteA, "REBIN", "LABOR", 40, ptr(at(5, 10, 0, 0)), ptr(at(5, 10, 30, 0))},
		{"p4", siteA, "PACK", "LABOR", 0, ptr(at(5, 11, 0, 0)), ptr(at(5, 11, 0, 20))},
		{"p6", siteA, "PICK", "LABOR", 9, ptr(at(5, 13, 0, 0)), nil}, // created, never published
		// Site B: p5 on Oct 5 (420 s), p7 seen ONLY through its Published event,
		// p9 created at the very start of Oct 6 and published at its last second.
		{"p5", siteB, "PICK", "LABOR", 7, ptr(at(5, 12, 0, 0)), ptr(at(5, 12, 7, 0))},
		{"p7", siteB, "PICK", "LABOR", 0, nil, ptr(at(5, 14, 0, 0))},
		{"p9", siteB, "PACK", "STATION", 12, ptr(at(6, 0, 0, 0)), ptr(at(6, 23, 59, 59))},
		// Site C sits on the bounds. pFrom is published exactly AT from (in),
		// pTo exactly AT to (out), pAtTo is created exactly at to (out).
		{"pFrom", siteC, "REBIN", "STATION", 3, ptr(at(4, 23, 50, 0)), ptr(at(5, 0, 0, 0))},
		{"pTo", siteC, "PICK", "LABOR", 1, ptr(at(6, 23, 59, 30)), ptr(at(7, 0, 0, 0))},
		{"pAtTo", siteC, "PICK", "LABOR", 2, ptr(at(7, 0, 0, 0)), nil},
	}
}

// contractEvents expands a plan into the events the OLTP side would emit,
// each with its own CloudEvents id.
func contractEvents(p planFixture) []report.PlanEvent {
	base := func(kind report.Kind, suffix string, when time.Time) report.PlanEvent {
		return report.PlanEvent{Kind: kind, EventID: p.id + "-" + suffix, At: when, PlanID: p.id, WarehouseID: p.site.WarehouseID, Location: p.site.Location}
	}
	var out []report.PlanEvent
	if p.created != nil {
		e := base(report.KindCreated, "created", *p.created)
		e.BottleneckStep, e.Shortage, e.CreatedAt = ptr(p.step), ptr(p.shortage), p.created
		out = append(out, e)
	}
	if p.publishd != nil {
		e := base(report.KindPublished, "published", *p.publishd)
		e.BottleneckStep, e.BindingConstraint, e.Shortage, e.PublishedAt = ptr(p.step), ptr(p.constraint), ptr(p.shortage), p.publishd
		out = append(out, e)
		if p.shortage > 0 {
			s := base(report.KindShortageDetected, "shortage", *p.publishd)
			s.BottleneckStep, s.Shortage = ptr(p.step), ptr(p.shortage)
			out = append(out, s)
		}
		b := base(report.KindBottleneckDetected, "bottleneck", *p.publishd)
		b.BottleneckStep = ptr(p.step)
		out = append(out, b)
	}
	return out
}

func loadContract(t *testing.T, p report.Projection) []report.PlanEvent {
	t.Helper()
	var all []report.PlanEvent
	for i, plan := range contractPlans() {
		evs := contractEvents(plan)
		if i%2 == 1 { // deliberately arrive out of order: Published/Bottleneck before Created
			for l, r := 0, len(evs)-1; l < r; l, r = l+1, r-1 {
				evs[l], evs[r] = evs[r], evs[l]
			}
		}
		all = append(all, evs...)
	}
	for _, e := range all {
		applied, err := p.Apply(context.Background(), e)
		if err != nil || !applied {
			t.Fatalf("Apply(%s) = %v, %v; want applied", e.EventID, applied, err)
		}
	}
	return all
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

func runStoreContract(t *testing.T, newStore func(t *testing.T) (report.Projection, report.Reader)) {
	t.Run("bottleneck counts: published plans only, grouped by site step constraint", func(t *testing.T) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.BottleneckCounts(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.BottleneckCount{
			{Site: siteA, BottleneckStep: "PACK", BindingConstraint: "STATION", Plans: 2},
			{Site: siteA, BottleneckStep: "PACK", BindingConstraint: "LABOR", Plans: 1},
			{Site: siteA, BottleneckStep: "REBIN", BindingConstraint: "LABOR", Plans: 1},
			{Site: siteB, BottleneckStep: "PICK", BindingConstraint: "LABOR", Plans: 2},
			{Site: siteB, BottleneckStep: "PACK", BindingConstraint: "STATION", Plans: 1},
			{Site: siteC, BottleneckStep: "REBIN", BindingConstraint: "STATION", Plans: 1},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got  %+v\nwant %+v", got, want)
		}
	})

	t.Run("shortage days: every published plan counted, UTC days", func(t *testing.T) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.ShortageDays(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.ShortageDay{
			{Site: siteA, Day: at(5, 0, 0, 0), PlansPublished: 4, PlansWithShortage: 2, TotalShortage: 5640},
			{Site: siteB, Day: at(5, 0, 0, 0), PlansPublished: 2, PlansWithShortage: 1, TotalShortage: 7},
			{Site: siteC, Day: at(5, 0, 0, 0), PlansPublished: 1, PlansWithShortage: 1, TotalShortage: 3},
			{Site: siteB, Day: at(6, 0, 0, 0), PlansPublished: 1, PlansWithShortage: 1, TotalShortage: 12},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got  %+v\nwant %+v", got, want)
		}
	})

	t.Run("throughput days: created by created_at, published by published_at", func(t *testing.T) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.ThroughputDays(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.ThroughputDay{
			{Site: siteA, Day: at(5, 0, 0, 0), PlansCreated: 5, PlansPublished: 4},
			{Site: siteB, Day: at(5, 0, 0, 0), PlansCreated: 1, PlansPublished: 2},
			{Site: siteC, Day: at(5, 0, 0, 0), PlansCreated: 0, PlansPublished: 1},
			{Site: siteB, Day: at(6, 0, 0, 0), PlansCreated: 1, PlansPublished: 1},
			{Site: siteC, Day: at(6, 0, 0, 0), PlansCreated: 1, PlansPublished: 0},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("got  %+v\nwant %+v", got, want)
		}
	})

	t.Run("latency: median and p95 by site over plans with both instants", func(t *testing.T) {
		p, r := newStore(t)
		loadContract(t, p)
		got, err := r.Latencies(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		want := []report.Latency{
			{Site: siteA, Plans: 4, MedianSeconds: 390, P95Seconds: 1620},
			{Site: siteB, Plans: 2, MedianSeconds: 43409.5, P95Seconds: 82100.05},
			{Site: siteC, Plans: 1, MedianSeconds: 600, P95Seconds: 600},
		}
		if len(got) != len(want) {
			t.Fatalf("got %+v, want %+v", got, want)
		}
		for i := range want {
			if got[i].Site != want[i].Site || got[i].Plans != want[i].Plans ||
				!near(got[i].MedianSeconds, want[i].MedianSeconds) || !near(got[i].P95Seconds, want[i].P95Seconds) {
				t.Errorf("row %d = %+v, want %+v", i, got[i], want[i])
			}
		}
	})

	t.Run("range bounds: from inclusive, to exclusive, to the instant", func(t *testing.T) {
		p, r := newStore(t)
		loadContract(t, p)
		ctx := context.Background()
		published := func(rg report.Range) int {
			days, err := r.ShortageDays(ctx, rg)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, d := range days {
				if d.Site == siteC {
					n += d.PlansPublished
				}
			}
			return n
		}
		created := func(rg report.Range) int {
			days, err := r.ThroughputDays(ctx, rg)
			if err != nil {
				t.Fatal(err)
			}
			n := 0
			for _, d := range days {
				if d.Site == siteC {
					n += d.PlansCreated
				}
			}
			return n
		}
		// Site C: pFrom published AT Oct 5 00:00:00; pTo published AT Oct 7 00:00:00
		// and created Oct 6 23:59:30; pAtTo created AT Oct 7 00:00:00.
		if got := published(report.Range{From: at(5, 0, 0, 0), To: at(7, 0, 0, 0)}); got != 1 {
			t.Errorf("[from=pFrom, to=pTo) published = %d, want 1 (pFrom in, pTo out)", got)
		}
		if got := published(report.Range{From: at(5, 0, 0, 1), To: at(7, 0, 0, 0)}); got != 0 {
			t.Errorf("from one second after pFrom: published = %d, want 0", got)
		}
		if got := published(report.Range{From: at(4, 23, 59, 59), To: at(7, 0, 0, 0)}); got != 1 {
			t.Errorf("from one second before pFrom: published = %d, want 1", got)
		}
		if got := published(report.Range{From: at(5, 0, 0, 0), To: at(7, 0, 0, 1)}); got != 2 {
			t.Errorf("to one second after pTo: published = %d, want 2", got)
		}
		if got := published(report.Range{From: at(5, 0, 0, 0), To: at(6, 23, 59, 59)}); got != 1 {
			t.Errorf("to before pTo: published = %d, want 1", got)
		}
		if got := created(report.Range{From: at(5, 0, 0, 0), To: at(7, 0, 0, 0)}); got != 1 {
			t.Errorf("created in [Oct 5, Oct 7) = %d, want 1 (pTo in, pAtTo out, pFrom before)", got)
		}
		if got := created(report.Range{From: at(5, 0, 0, 0), To: at(7, 0, 0, 1)}); got != 2 {
			t.Errorf("created with to one second after pAtTo = %d, want 2", got)
		}
		if got := created(report.Range{From: at(4, 23, 50, 0), To: at(5, 0, 0, 0)}); got != 1 {
			t.Errorf("created with from == pFrom.created = %d, want 1 (from inclusive)", got)
		}
	})

	t.Run("replaying an applied id is a no-op", func(t *testing.T) {
		p, r := newStore(t)
		events := loadContract(t, p)
		before, err := r.ShortageDays(context.Background(), contractRange)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range events {
			applied, err := p.Apply(context.Background(), e)
			if err != nil || applied {
				t.Fatalf("replay %s = %v, %v; want not applied", e.EventID, applied, err)
			}
		}
		// A replay carrying different numbers under the same id must not change anything.
		tampered := events[0]
		tampered.Shortage = ptr(99999.0)
		if applied, err := p.Apply(context.Background(), tampered); err != nil || applied {
			t.Fatalf("tampered replay = %v, %v", applied, err)
		}
		after, _ := r.ShortageDays(context.Background(), contractRange)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("replay changed the model:\nbefore %+v\nafter  %+v", before, after)
		}
	})

	t.Run("an empty store answers with empty non-nil slices", func(t *testing.T) {
		_, r := newStore(t)
		ctx := context.Background()
		b, e1 := r.BottleneckCounts(ctx, contractRange)
		s, e2 := r.ShortageDays(ctx, contractRange)
		th, e3 := r.ThroughputDays(ctx, contractRange)
		l, e4 := r.Latencies(ctx, contractRange)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil {
			t.Fatalf("errors: %v %v %v %v", e1, e2, e3, e4)
		}
		if b == nil || s == nil || th == nil || l == nil || len(b)+len(s)+len(th)+len(l) != 0 {
			t.Fatalf("want empty non-nil slices, got %#v %#v %#v %#v", b, s, th, l)
		}
	})
}
