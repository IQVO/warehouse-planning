package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/analytics/report"
)

// ReportsServer is the inbound HTTP adapter of cmd/planning-reports: the
// read-only analytics reports of ADR 0005. It depends only on the
// report.Reader port over the analytical database; it never touches the OLTP
// use cases, and it writes nothing.
type ReportsServer struct {
	Reader report.Reader
	// Now is the clock the default range ends at (time.Now when nil).
	Now func() time.Time
}

const dayLayout = "2006-01-02"

// bottleneckRowDTO, shortageDayDTO, throughputDayDTO and latencyDTO are the
// wire shapes (snake_case, like the rest of this service's API); days are
// calendar dates (UTC) and the report structs never leak onto the wire.
type bottleneckRowDTO struct {
	WarehouseID       string  `json:"warehouse_id"`
	Location          string  `json:"location"`
	BottleneckStep    string  `json:"bottleneck_step"`
	BindingConstraint string  `json:"binding_constraint"`
	Plans             int     `json:"plans"`
	Share             float64 `json:"share"`
}

type shortageDayDTO struct {
	Day               string  `json:"day"`
	WarehouseID       string  `json:"warehouse_id"`
	Location          string  `json:"location"`
	PlansPublished    int     `json:"plans_published"`
	PlansWithShortage int     `json:"plans_with_shortage"`
	TotalShortage     float64 `json:"total_shortage"`
	ShortageRate      float64 `json:"shortage_rate"`
}

type throughputDayDTO struct {
	Day            string `json:"day"`
	WarehouseID    string `json:"warehouse_id"`
	Location       string `json:"location"`
	PlansCreated   int    `json:"plans_created"`
	PlansPublished int    `json:"plans_published"`
}

type latencyDTO struct {
	WarehouseID   string  `json:"warehouse_id"`
	Location      string  `json:"location"`
	Plans         int     `json:"plans"`
	MedianSeconds float64 `json:"median_seconds"`
	P95Seconds    float64 `json:"p95_seconds"`
}

// rangeDTO echoes the effective half-open range [from, to) of a response.
type rangeDTO struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type bottleneckReportDTO struct {
	rangeDTO
	Rows []bottleneckRowDTO `json:"rows"`
}

type shortageReportDTO struct {
	rangeDTO
	Days []shortageDayDTO `json:"days"`
}

type throughputReportDTO struct {
	rangeDTO
	Days    []throughputDayDTO `json:"days"`
	Latency []latencyDTO       `json:"latency"`
}

// NewReportsRouter builds the router of cmd/planning-reports: /healthz and
// GET /reports/{bottleneck-frequency,shortage-trend,plan-throughput,freshness}. No
// auth middleware (fleet rule), no CORS: the service is cluster-internal.
func NewReportsRouter(s *ReportsServer) http.Handler {
	r := chi.NewRouter()
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	r.Get("/reports/bottleneck-frequency", s.handleBottleneckFrequency)
	r.Get("/reports/shortage-trend", s.handleShortageTrend)
	r.Get("/reports/plan-throughput", s.handlePlanThroughput)
	r.Get("/reports/freshness", s.handleFreshness)
	return r
}

// handleFreshness serves GET /reports/freshness: how far the projection is
// behind (now - the newest applied event time), for all three reports, which
// read one projection. Both fields are null until the first event is applied.
func (s *ReportsServer) handleFreshness(w http.ResponseWriter, r *http.Request) {
	asOf, err := s.Reader.LastEventAt(r.Context())
	if err != nil {
		writeReportError(w, r)
		return
	}
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	writeJSON(w, http.StatusOK, report.ComputeFreshness(asOf, now()))
}

// parseRange validates ?from=&to= (RFC 3339, both optional) and writes the
// RFC 7807 400 itself when they are unusable.
func (s *ReportsServer) parseRange(w http.ResponseWriter, r *http.Request) (report.Range, bool) {
	now := time.Now
	if s.Now != nil {
		now = s.Now
	}
	q := r.URL.Query()
	rg, err := report.ParseRange(q.Get("from"), q.Get("to"), now())
	if err != nil {
		writeProblem(w, http.StatusBadRequest,
			problemInfo{"invalid-report-range", "from and to must form a valid RFC 3339 range of at most 366 days"},
			err.Error(), r.URL.Path)
		return report.Range{}, false
	}
	return rg, true
}

// writeReportError is the 500 for a failed analytical query. The cause is
// not echoed: it can carry SQL and connection details.
func writeReportError(w http.ResponseWriter, r *http.Request) {
	writeProblem(w, http.StatusInternalServerError,
		problemInfo{"report-store-error", "The report could not be served"},
		"the analytical database query failed", r.URL.Path)
}

func (s *ReportsServer) handleBottleneckFrequency(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseRange(w, r)
	if !ok {
		return
	}
	counts, err := s.Reader.BottleneckCounts(r.Context(), rg)
	if err != nil {
		writeReportError(w, r)
		return
	}
	freqs := report.BottleneckFrequencies(counts)
	out := bottleneckReportDTO{rangeDTO: rangeDTO{rg.From, rg.To}, Rows: make([]bottleneckRowDTO, 0, len(freqs))}
	for _, f := range freqs {
		out.Rows = append(out.Rows, bottleneckRowDTO{
			WarehouseID: f.WarehouseID, Location: f.Location, BottleneckStep: f.BottleneckStep,
			BindingConstraint: f.BindingConstraint, Plans: f.Plans, Share: f.Share,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *ReportsServer) handleShortageTrend(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseRange(w, r)
	if !ok {
		return
	}
	days, err := s.Reader.ShortageDays(r.Context(), rg)
	if err != nil {
		writeReportError(w, r)
		return
	}
	trend := report.ShortageTrend(days)
	out := shortageReportDTO{rangeDTO: rangeDTO{rg.From, rg.To}, Days: make([]shortageDayDTO, 0, len(trend))}
	for _, d := range trend {
		out.Days = append(out.Days, shortageDayDTO{
			Day: d.Day.UTC().Format(dayLayout), WarehouseID: d.WarehouseID, Location: d.Location,
			PlansPublished: d.PlansPublished, PlansWithShortage: d.PlansWithShortage,
			TotalShortage: d.TotalShortage, ShortageRate: d.ShortageRate,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *ReportsServer) handlePlanThroughput(w http.ResponseWriter, r *http.Request) {
	rg, ok := s.parseRange(w, r)
	if !ok {
		return
	}
	days, err := s.Reader.ThroughputDays(r.Context(), rg)
	if err != nil {
		writeReportError(w, r)
		return
	}
	latencies, err := s.Reader.Latencies(r.Context(), rg)
	if err != nil {
		writeReportError(w, r)
		return
	}
	out := throughputReportDTO{
		rangeDTO: rangeDTO{rg.From, rg.To},
		Days:     make([]throughputDayDTO, 0, len(days)),
		Latency:  make([]latencyDTO, 0, len(latencies)),
	}
	for _, d := range days {
		out.Days = append(out.Days, throughputDayDTO{
			Day: d.Day.UTC().Format(dayLayout), WarehouseID: d.WarehouseID, Location: d.Location,
			PlansCreated: d.PlansCreated, PlansPublished: d.PlansPublished,
		})
	}
	for _, l := range latencies {
		out.Latency = append(out.Latency, latencyDTO{
			WarehouseID: l.WarehouseID, Location: l.Location, Plans: l.Plans,
			MedianSeconds: l.MedianSeconds, P95Seconds: l.P95Seconds,
		})
	}
	writeJSON(w, http.StatusOK, out)
}
