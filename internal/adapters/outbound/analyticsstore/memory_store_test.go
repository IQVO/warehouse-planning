package analyticsstore_test

import (
	"context"
	"sync"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

func TestMemoryStore_SatisfiesTheStoreContract(t *testing.T) {
	runStoreContract(t, func(*testing.T) (report.Projection, report.Reader) {
		m := analyticsstore.NewMemory()
		return m, m
	})
}

// A convergence check on the in-memory twin: whatever order the four events
// of one plan arrive in, the row ends up the same.
func TestMemoryStore_ConvergesInAnyEventOrder(t *testing.T) {
	plan := contractPlans()[0] // p1
	events := contractEvents(plan)
	orders := [][]int{{0, 1, 2, 3}, {3, 2, 1, 0}, {1, 3, 0, 2}, {2, 0, 3, 1}}
	var first []report.BottleneckCount
	for _, order := range orders {
		m := analyticsstore.NewMemory()
		for _, i := range order {
			if _, err := m.Apply(context.Background(), events[i]); err != nil {
				t.Fatal(err)
			}
		}
		got, _ := m.BottleneckCounts(context.Background(), contractRange)
		if len(got) != 1 || got[0].BindingConstraint != "STATION" || got[0].BottleneckStep != "PACK" {
			t.Fatalf("order %v: %+v", order, got)
		}
		lat, _ := m.Latencies(context.Background(), contractRange)
		if len(lat) != 1 || lat[0].MedianSeconds != 600 {
			t.Fatalf("order %v: latency %+v", order, lat)
		}
		if first == nil {
			first = got
		}
	}
}

func TestMemoryStore_IsSafeForConcurrentApply(t *testing.T) {
	m := analyticsstore.NewMemory()
	var wg sync.WaitGroup
	events := []report.PlanEvent{}
	for _, p := range contractPlans() {
		events = append(events, contractEvents(p)...)
	}
	applied := make(chan bool, len(events)*2)
	for i := 0; i < 2; i++ { // every event applied twice, concurrently
		for _, e := range events {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ok, _ := m.Apply(context.Background(), e)
				applied <- ok
			}()
		}
	}
	wg.Wait()
	close(applied)
	n := 0
	for ok := range applied {
		if ok {
			n++
		}
	}
	if n != len(events) {
		t.Fatalf("applied %d times, want exactly once per event id (%d)", n, len(events))
	}
}
