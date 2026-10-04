package http_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/analyticsstore"
	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

func TestReports_Freshness_NothingProjectedYetIsNull(t *testing.T) {
	rec := get(t, reportsServer(analyticsstore.NewMemory()), "/reports/freshness")
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"as_of":null,"lag_seconds":null}` {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

// reportsNow is Oct 6 12:00:00 UTC: the newest applied event (Oct 6 11:58:30)
// is 90 seconds behind.
func TestReports_Freshness_LagIsNowMinusTheNewestAppliedEvent(t *testing.T) {
	published := rt(6, 11, 58, 30)
	created := rt(6, 11, 0, 0)
	h := reportsServer(projectPlans(t, planSpec{"f1", "WH-1", "SIM1", "PACK", "LABOR", 0, &created, &published}))
	rec := get(t, h, "/reports/freshness")
	want := `{"as_of":"2026-10-06T11:58:30Z","lag_seconds":90}`
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != want {
		t.Fatalf("%d %s, want %s", rec.Code, rec.Body, want)
	}
}

type failingFreshness struct{ *analyticsstore.Memory }

func (failingFreshness) LastEventAt(context.Context) (*time.Time, error) {
	return nil, errors.New("pq: secret detail")
}

var _ report.Reader = failingFreshness{}

func TestReports_Freshness_StoreFailureIsA500ProblemWithoutTheCause(t *testing.T) {
	rec := get(t, reportsServer(failingFreshness{analyticsstore.NewMemory()}), "/reports/freshness")
	if rec.Code != http.StatusInternalServerError || rec.Header().Get("Content-Type") != "application/problem+json" ||
		!strings.Contains(rec.Body.String(), "/report-store-error") || strings.Contains(rec.Body.String(), "secret") {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
}

func TestReports_Freshness_NilNowUsesTheRealClock(t *testing.T) {
	m := analyticsstore.NewMemory()
	published := time.Now().UTC().Add(-time.Hour)
	created := published.Add(-time.Minute)
	for _, e := range []report.PlanEvent{
		{Kind: report.KindPublished, EventID: "p", At: published, PlanID: "x", WarehouseID: "WH", Location: "L", PublishedAt: &published},
		{Kind: report.KindCreated, EventID: "c", At: created, PlanID: "x", WarehouseID: "WH", Location: "L", CreatedAt: &created},
	} {
		if _, err := m.Apply(context.Background(), e); err != nil {
			t.Fatal(err)
		}
	}
	h := reportsServerRealClock(m)
	rec := get(t, h, "/reports/freshness")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"lag_seconds":3`) {
		t.Fatalf("%d %s, want a lag of about an hour (3600 s)", rec.Code, rec.Body)
	}
}
