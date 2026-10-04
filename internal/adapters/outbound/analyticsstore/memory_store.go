package analyticsstore

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

// Memory is the in-memory twin of Projection and Reader, for unit tests,
// the HTTP handler tests and the BDD features. It implements the same
// semantics as the SQL (the shared contract test runs against both):
// idempotent on the event id, only-what-the-event-carries upserts, range
// [from, to), UTC days, percentile_cont latencies.
type Memory struct {
	mu        sync.Mutex
	processed map[string]struct{}
	plans     map[string]*memPlan
}

type memPlan struct {
	warehouseID, location, step, constraint string
	shortage                                float64
	createdAt, publishedAt                  *time.Time
}

// NewMemory returns an empty Memory store.
func NewMemory() *Memory {
	return &Memory{processed: map[string]struct{}{}, plans: map[string]*memPlan{}}
}

var (
	_ report.Projection = (*Memory)(nil)
	_ report.Reader     = (*Memory)(nil)
)

// Apply implements report.Projection.
func (m *Memory) Apply(_ context.Context, e report.PlanEvent) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, dup := m.processed[e.EventID]; dup {
		return false, nil
	}
	m.processed[e.EventID] = struct{}{}
	p, ok := m.plans[e.PlanID]
	if !ok {
		p = &memPlan{}
		m.plans[e.PlanID] = p
	}
	p.warehouseID, p.location = e.WarehouseID, e.Location
	if e.BottleneckStep != nil {
		p.step = *e.BottleneckStep
	}
	if e.BindingConstraint != nil {
		p.constraint = *e.BindingConstraint
	}
	if e.Shortage != nil {
		p.shortage = *e.Shortage
	}
	if e.CreatedAt != nil {
		t := *e.CreatedAt
		p.createdAt = &t
	}
	if e.PublishedAt != nil {
		t := *e.PublishedAt
		p.publishedAt = &t
	}
	return true, nil
}

// inRange is the half-open [From, To) test.
func inRange(t *time.Time, r report.Range) bool {
	return t != nil && t.Compare(r.From) >= 0 && t.Compare(r.To) < 0
}

func siteOf(p *memPlan) report.Site {
	return report.Site{WarehouseID: p.warehouseID, Location: p.location}
}

// published returns the plans published in the range.
func (m *Memory) published(r report.Range) []*memPlan {
	var out []*memPlan
	for _, p := range m.plans {
		if inRange(p.publishedAt, r) {
			out = append(out, p)
		}
	}
	return out
}

// BottleneckCounts implements report.Reader.
func (m *Memory) BottleneckCounts(_ context.Context, r report.Range) ([]report.BottleneckCount, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type key struct {
		site             report.Site
		step, constraint string
	}
	counts := map[key]int{}
	for _, p := range m.published(r) {
		counts[key{siteOf(p), p.step, p.constraint}]++
	}
	out := make([]report.BottleneckCount, 0, len(counts))
	for k, n := range counts {
		out = append(out, report.BottleneckCount{Site: k.site, BottleneckStep: k.step, BindingConstraint: k.constraint, Plans: n})
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		switch {
		case a.WarehouseID != b.WarehouseID:
			return a.WarehouseID < b.WarehouseID
		case a.Location != b.Location:
			return a.Location < b.Location
		case a.Plans != b.Plans:
			return a.Plans > b.Plans
		case a.BottleneckStep != b.BottleneckStep:
			return a.BottleneckStep < b.BottleneckStep
		}
		return a.BindingConstraint < b.BindingConstraint
	})
	return out, nil
}

type dayKey struct {
	day  time.Time
	site report.Site
}

func lessDayKey(a, b dayKey) bool {
	switch {
	case !a.day.Equal(b.day):
		return a.day.Before(b.day)
	case a.site.WarehouseID != b.site.WarehouseID:
		return a.site.WarehouseID < b.site.WarehouseID
	}
	return a.site.Location < b.site.Location
}

// ShortageDays implements report.Reader.
func (m *Memory) ShortageDays(_ context.Context, r report.Range) ([]report.ShortageDay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	agg := map[dayKey]*report.ShortageDay{}
	for _, p := range m.published(r) {
		k := dayKey{day(*p.publishedAt), siteOf(p)}
		d, ok := agg[k]
		if !ok {
			d = &report.ShortageDay{Site: k.site, Day: k.day}
			agg[k] = d
		}
		d.PlansPublished++
		if p.shortage > 0 {
			d.PlansWithShortage++
		}
		d.TotalShortage += p.shortage
	}
	keys := make([]dayKey, 0, len(agg))
	for k := range agg {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return lessDayKey(keys[i], keys[j]) })
	out := make([]report.ShortageDay, 0, len(keys))
	for _, k := range keys {
		out = append(out, *agg[k])
	}
	return out, nil
}

// ThroughputDays implements report.Reader.
func (m *Memory) ThroughputDays(_ context.Context, r report.Range) ([]report.ThroughputDay, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	agg := map[dayKey]*report.ThroughputDay{}
	row := func(k dayKey) *report.ThroughputDay {
		d, ok := agg[k]
		if !ok {
			d = &report.ThroughputDay{Site: k.site, Day: k.day}
			agg[k] = d
		}
		return d
	}
	for _, p := range m.plans {
		if inRange(p.createdAt, r) {
			row(dayKey{day(*p.createdAt), siteOf(p)}).PlansCreated++
		}
		if inRange(p.publishedAt, r) {
			row(dayKey{day(*p.publishedAt), siteOf(p)}).PlansPublished++
		}
	}
	keys := make([]dayKey, 0, len(agg))
	for k := range agg {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return lessDayKey(keys[i], keys[j]) })
	out := make([]report.ThroughputDay, 0, len(keys))
	for _, k := range keys {
		out = append(out, *agg[k])
	}
	return out, nil
}

// Latencies implements report.Reader.
func (m *Memory) Latencies(_ context.Context, r report.Range) ([]report.Latency, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	secs := map[report.Site][]float64{}
	for _, p := range m.published(r) {
		if p.createdAt != nil {
			secs[siteOf(p)] = append(secs[siteOf(p)], p.publishedAt.Sub(*p.createdAt).Seconds())
		}
	}
	sites := make([]report.Site, 0, len(secs))
	for s := range secs {
		sites = append(sites, s)
	}
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].WarehouseID != sites[j].WarehouseID {
			return sites[i].WarehouseID < sites[j].WarehouseID
		}
		return sites[i].Location < sites[j].Location
	})
	out := make([]report.Latency, 0, len(sites))
	for _, s := range sites {
		out = append(out, report.Latency{
			Site: s, Plans: len(secs[s]),
			MedianSeconds: report.Percentile(secs[s], 0.5), P95Seconds: report.Percentile(secs[s], 0.95),
		})
	}
	return out, nil
}
