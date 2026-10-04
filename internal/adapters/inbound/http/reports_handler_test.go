package http_test

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	inboundhttp "github.com/claudioed/warehouse-planning/internal/adapters/inbound/http"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

func rt(day, hour, min, sec int) time.Time {
	return time.Date(2026, 10, day, hour, min, sec, 0, time.UTC)
}

var reportsNow = rt(6, 12, 0, 0)

type planSpec struct {
	id, wh, loc, step, constraint string
	shortage                      float64
	created, published            *time.Time
}

func tp(t time.Time) *time.Time { return &t }

// projectPlans feeds Created/Published events for the specs into a Memory
// store (the same projection the SQL store is contract-tested against).
func projectPlans(t *testing.T, plans ...planSpec) *analyticsstore.Memory {
	t.Helper()
	m := analyticsstore.NewMemory()
	for _, p := range plans {
		step, constraint, shortage := p.step, p.constraint, p.shortage
		if p.created != nil {
			e := report.PlanEvent{Kind: report.KindCreated, EventID: p.id + "-c", At: *p.created, PlanID: p.id, WarehouseID: p.wh, Location: p.loc,
				BottleneckStep: &step, Shortage: &shortage, CreatedAt: p.created}
			if _, err := m.Apply(context.Background(), e); err != nil {
				t.Fatal(err)
			}
		}
		if p.published != nil {
			e := report.PlanEvent{Kind: report.KindPublished, EventID: p.id + "-p", At: *p.published, PlanID: p.id, WarehouseID: p.wh, Location: p.loc,
				BottleneckStep: &step, BindingConstraint: &constraint, Shortage: &shortage, PublishedAt: p.published}
			if _, err := m.Apply(context.Background(), e); err != nil {
				t.Fatal(err)
			}
		}
	}
	return m
}

func reportsServer(r report.Reader) http.Handler {
	return inboundhttp.NewReportsRouter(&inboundhttp.ReportsServer{Reader: r, Now: func() time.Time { return reportsNow }})
}

func get(t *testing.T, h http.Handler, target string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, target, nil))
	return rec
}

func closeTo(a, b float64) bool { return math.Abs(a-b) < 1e-6 }

const explicitRange = "from=2026-10-05T00:00:00Z&to=2026-10-07T00:00:00Z"

func scenario(t *testing.T) http.Handler {
	return reportsServer(projectPlans(t,
		planSpec{"a1", "WH-1", "SIM1", "PACK", "STATION", 5600, tp(rt(5, 8, 0, 0)), tp(rt(5, 8, 10, 0))},
		planSpec{"a2", "WH-1", "SIM1", "PACK", "STATION", 0, tp(rt(5, 9, 0, 0)), tp(rt(5, 9, 3, 0))},
		planSpec{"a3", "WH-1", "SIM1", "REBIN", "LABOR", 40, tp(rt(5, 10, 0, 0)), tp(rt(5, 10, 30, 0))},
		planSpec{"a4", "WH-1", "SIM1", "PACK", "LABOR", 0, tp(rt(4, 23, 0, 0)), tp(rt(5, 0, 0, 0))},  // published exactly AT from
		planSpec{"a5", "WH-1", "SIM1", "PICK", "LABOR", 99, tp(rt(6, 23, 0, 0)), tp(rt(7, 0, 0, 0))}, // published exactly AT to
		planSpec{"b1", "WH-2", "SIM2", "PICK", "LABOR", 7, tp(rt(6, 1, 0, 0)), tp(rt(6, 1, 20, 0))},
	))
}

func TestReports_BottleneckFrequency(t *testing.T) {
	rec := get(t, scenario(t), "/reports/bottleneck-frequency?"+explicitRange)
	if rec.Code != http.StatusOK || rec.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("status %d, content-type %q: %s", rec.Code, rec.Header().Get("Content-Type"), rec.Body)
	}
	var got struct {
		From, To time.Time
		Rows     []struct {
			WarehouseID       string  `json:"warehouse_id"`
			Location          string  `json:"location"`
			BottleneckStep    string  `json:"bottleneck_step"`
			BindingConstraint string  `json:"binding_constraint"`
			Plans             int     `json:"plans"`
			Share             float64 `json:"share"`
		}
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if !got.From.Equal(rt(5, 0, 0, 0)) || !got.To.Equal(rt(7, 0, 0, 0)) {
		t.Errorf("range echo = %v..%v", got.From, got.To)
	}
	type row struct {
		wh, step, constraint string
		plans                int
		share                float64
	}
	want := []row{
		{"WH-1", "PACK", "STATION", 2, 0.5}, {"WH-1", "PACK", "LABOR", 1, 0.25}, {"WH-1", "REBIN", "LABOR", 1, 0.25},
		{"WH-2", "PICK", "LABOR", 1, 1},
	}
	if len(got.Rows) != len(want) {
		t.Fatalf("rows = %+v", got.Rows)
	}
	for i, w := range want {
		g := got.Rows[i]
		if g.WarehouseID != w.wh || g.BottleneckStep != w.step || g.BindingConstraint != w.constraint || g.Plans != w.plans || !closeTo(g.Share, w.share) {
			t.Errorf("row %d = %+v, want %+v (a5 published AT `to` must be excluded, a4 AT `from` included)", i, g, w)
		}
	}
}

func TestReports_ShortageTrend_CountsPlansWithoutShortageSoTheRateIsMeaningful(t *testing.T) {
	rec := get(t, scenario(t), "/reports/shortage-trend?"+explicitRange)
	type day struct {
		Day               string  `json:"day"`
		WarehouseID       string  `json:"warehouse_id"`
		Location          string  `json:"location"`
		PlansPublished    int     `json:"plans_published"`
		PlansWithShortage int     `json:"plans_with_shortage"`
		TotalShortage     float64 `json:"total_shortage"`
		ShortageRate      float64 `json:"shortage_rate"`
	}
	var got struct{ Days []day }
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("%d %v %s", rec.Code, err, rec.Body)
	}
	want := []day{
		{"2026-10-05", "WH-1", "SIM1", 4, 2, 5640, 0.5},
		{"2026-10-06", "WH-2", "SIM2", 1, 1, 7, 1},
	}
	if !reflect.DeepEqual(got.Days, want) {
		t.Fatalf("days = %+v, want %+v", got.Days, want)
	}
}

func TestReports_PlanThroughput_DaysAndLatencyBySite(t *testing.T) {
	rec := get(t, scenario(t), "/reports/plan-throughput?"+explicitRange)
	type day struct {
		Day            string `json:"day"`
		WarehouseID    string `json:"warehouse_id"`
		PlansCreated   int    `json:"plans_created"`
		PlansPublished int    `json:"plans_published"`
	}
	type lat struct {
		WarehouseID   string  `json:"warehouse_id"`
		Plans         int     `json:"plans"`
		MedianSeconds float64 `json:"median_seconds"`
		P95Seconds    float64 `json:"p95_seconds"`
	}
	var got struct {
		Days    []day
		Latency []lat
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != http.StatusOK {
		t.Fatalf("%d %v %s", rec.Code, err, rec.Body)
	}
	// Created in range: a1 a2 a3 (Oct 5, WH-1) and b1 (Oct 6, WH-2); a4 was created Oct 4 (out) and a5 on Oct 6 23:00 (WH-1, Oct 6).
	wantDays := []day{{"2026-10-05", "WH-1", 3, 4}, {"2026-10-06", "WH-1", 1, 0}, {"2026-10-06", "WH-2", 1, 1}}
	if !reflect.DeepEqual(got.Days, wantDays) {
		t.Fatalf("days = %+v, want %+v", got.Days, wantDays)
	}
	// WH-1 latencies (a1 600, a2 180, a3 1800, a4 3600): median 1200, p95 3330. WH-2: b1 1200.
	for i := range got.Latency {
		got.Latency[i].MedianSeconds = math.Round(got.Latency[i].MedianSeconds*1e6) / 1e6
		got.Latency[i].P95Seconds = math.Round(got.Latency[i].P95Seconds*1e6) / 1e6
	}
	wantLat := []lat{{"WH-1", 4, 1200, 3330}, {"WH-2", 1, 1200, 1200}}
	if !reflect.DeepEqual(got.Latency, wantLat) {
		t.Fatalf("latency = %+v, want %+v", got.Latency, wantLat)
	}
}

// Exact bytes for one plan per endpoint: field names, order and number
// formatting are part of the contract (apis/openapi.yaml).
func TestReports_ExactJSON(t *testing.T) {
	h := reportsServer(projectPlans(t,
		planSpec{"p1", "WH-1", "SIM1", "PACK", "STATION", 5600, tp(rt(5, 8, 0, 0)), tp(rt(5, 8, 10, 0))}))
	hdr := `"from":"2026-10-05T00:00:00Z","to":"2026-10-07T00:00:00Z"`
	cases := map[string]string{
		"/reports/bottleneck-frequency?" + explicitRange: `{` + hdr + `,"rows":[{"warehouse_id":"WH-1","location":"SIM1","bottleneck_step":"PACK","binding_constraint":"STATION","plans":1,"share":1}]}`,
		"/reports/shortage-trend?" + explicitRange:       `{` + hdr + `,"days":[{"day":"2026-10-05","warehouse_id":"WH-1","location":"SIM1","plans_published":1,"plans_with_shortage":1,"total_shortage":5600,"shortage_rate":1}]}`,
		"/reports/plan-throughput?" + explicitRange:      `{` + hdr + `,"days":[{"day":"2026-10-05","warehouse_id":"WH-1","location":"SIM1","plans_created":1,"plans_published":1}],"latency":[{"warehouse_id":"WH-1","location":"SIM1","plans":1,"median_seconds":600,"p95_seconds":600}]}`,
	}
	for target, want := range cases {
		if got := strings.TrimSpace(get(t, h, target).Body.String()); got != want {
			t.Errorf("%s\n got  %s\n want %s", target, got, want)
		}
	}
}

func TestReports_EmptyResultsAreEmptyArraysNotErrors(t *testing.T) {
	h := reportsServer(analyticsstore.NewMemory())
	for target, want := range map[string]string{
		"/reports/bottleneck-frequency?" + explicitRange: `"rows":[]`,
		"/reports/shortage-trend?" + explicitRange:       `"days":[]`,
		"/reports/plan-throughput?" + explicitRange:      `"days":[],"latency":[]`,
	} {
		rec := get(t, h, target)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), want) {
			t.Errorf("%s = %d %s, want 200 containing %s", target, rec.Code, rec.Body, want)
		}
	}
}

func TestReports_DefaultRangeIsTheThirtyDaysEndingNow(t *testing.T) {
	rec := get(t, reportsServer(analyticsstore.NewMemory()), "/reports/shortage-trend")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"from":"2026-09-06T12:00:00Z","to":"2026-10-06T12:00:00Z"`) {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestReports_InvalidRangeIsAProblemJSON400(t *testing.T) {
	h := reportsServer(analyticsstore.NewMemory())
	for _, q := range []string{"from=yesterday", "to=2026-13-40", "from=2026-10-02T00:00:00Z&to=2026-10-01T00:00:00Z", "from=2026-10-01T00:00:00Z&to=2026-10-01T00:00:00Z", "from=2020-01-01T00:00:00Z&to=2026-01-01T00:00:00Z"} {
		for _, path := range []string{"bottleneck-frequency", "shortage-trend", "plan-throughput"} {
			rec := get(t, h, "/reports/"+path+"?"+q)
			var p struct {
				Type, Title, Detail, Instance string
				Status                        int
			}
			if rec.Code != http.StatusBadRequest || rec.Header().Get("Content-Type") != "application/problem+json" {
				t.Fatalf("%s?%s = %d %q", path, q, rec.Code, rec.Header().Get("Content-Type"))
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil || p.Status != 400 ||
				p.Type != "https://errors.warehouse-planning.warehouse-systems.dev/invalid-report-range" ||
				p.Instance != "/reports/"+path || p.Detail == "" || p.Title == "" {
				t.Fatalf("problem = %+v, %v", p, err)
			}
		}
	}
}

type failingReader struct{ report.Reader }

func (failingReader) BottleneckCounts(context.Context, report.Range) ([]report.BottleneckCount, error) {
	return nil, errors.New("pq: password authentication failed for user planning_ro")
}
func (failingReader) ShortageDays(context.Context, report.Range) ([]report.ShortageDay, error) {
	return nil, errors.New("pq: secret detail")
}
func (failingReader) ThroughputDays(context.Context, report.Range) ([]report.ThroughputDay, error) {
	return nil, errors.New("pq: secret detail")
}

// A second reader failing only on Latencies, to cover the throughput endpoint's second query.
type failingLatency struct{ *analyticsstore.Memory }

func (failingLatency) Latencies(context.Context, report.Range) ([]report.Latency, error) {
	return nil, errors.New("pq: secret detail")
}

func TestReports_StoreFailureIsA500ProblemThatDoesNotLeakTheCause(t *testing.T) {
	cases := map[string]http.Handler{
		"/reports/bottleneck-frequency?" + explicitRange: reportsServer(failingReader{}),
		"/reports/shortage-trend?" + explicitRange:       reportsServer(failingReader{}),
		"/reports/plan-throughput?" + explicitRange:      reportsServer(failingReader{}),
	}
	cases["/reports/plan-throughput?"+explicitRange+"&x=latency"] = reportsServer(failingLatency{analyticsstore.NewMemory()})
	for target, h := range cases {
		rec := get(t, h, target)
		if rec.Code != http.StatusInternalServerError || rec.Header().Get("Content-Type") != "application/problem+json" ||
			!strings.Contains(rec.Body.String(), "/report-store-error") || strings.Contains(rec.Body.String(), "secret") || strings.Contains(rec.Body.String(), "planning_ro") {
			t.Errorf("%s = %d %s", target, rec.Code, rec.Body)
		}
	}
}

func TestReports_HealthzAndReadOnlyRouting(t *testing.T) {
	h := reportsServer(analyticsstore.NewMemory())
	if rec := get(t, h, "/healthz"); rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Fatalf("healthz = %d %s", rec.Code, rec.Body)
	}
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, httptest.NewRequest(method, "/reports/shortage-trend", nil))
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s /reports/shortage-trend = %d, want 405: the reports service is read-only", method, rec.Code)
		}
	}
	if rec := get(t, h, "/reports/unknown"); rec.Code != http.StatusNotFound {
		t.Errorf("unknown report = %d, want 404", rec.Code)
	}
}

// The default clock is the real one when none is injected.
func TestReports_NilNowUsesTheRealClock(t *testing.T) {
	h := inboundhttp.NewReportsRouter(&inboundhttp.ReportsServer{Reader: analyticsstore.NewMemory()})
	rec := get(t, h, "/reports/shortage-trend")
	var got struct{ From, To time.Time }
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil || rec.Code != 200 {
		t.Fatalf("%d %v", rec.Code, err)
	}
	if d := time.Since(got.To); d < 0 || d > time.Minute || got.To.Sub(got.From) != 30*24*time.Hour {
		t.Fatalf("range = %v..%v", got.From, got.To)
	}
}
