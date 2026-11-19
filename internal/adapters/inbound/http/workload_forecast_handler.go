package http

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// Problem categories of the forecast body that the adapter itself detects
// (the domain never sees a cpt string or an absent quantity).
var (
	problemMalformedCPT          = problemInfo{slug: "malformed-cpt", title: "cpt must be an RFC3339 timestamp"}
	problemMissingBucketQuantity = problemInfo{slug: "missing-bucket-quantity", title: "Every forecast bucket needs a quantity"}
)

// registerRequirementRoutes registers the docs/adr/0023 routes whose use
// cases are wired. The PUTs are naturally idempotent (the same body twice is
// the same resource state), so they are NOT behind the Idempotency-Key
// middleware, like every other PUT declaration of this service.
func (s *Server) registerRequirementRoutes(r chi.Router) {
	if s.ReceiveWorkloadForecast != nil {
		r.Put("/workload-forecasts/{site_id}/{path_id}", s.handleReceiveWorkloadForecast)
	}
	if s.GetWorkloadForecast != nil {
		r.Get("/workload-forecasts/{site_id}/{path_id}", s.handleGetWorkloadForecast)
	}
}

// receiveWorkloadForecastRequest is PUT /workload-forecasts/{site_id}/
// {path_id}'s body: the CPT buckets that replace the stored ones as a whole.
type receiveWorkloadForecastRequest struct {
	Buckets []forecastBucketRequest `json:"buckets"`
}

// forecastBucketRequest is one bucket as sent: quantity is a pointer so an
// absent one is refused instead of read as zero units.
type forecastBucketRequest struct {
	CPT      string `json:"cpt"`
	Quantity *int64 `json:"quantity"`
}

// forecastBucketResponse is one stored bucket.
type forecastBucketResponse struct {
	CPT      string `json:"cpt"`
	Quantity int64  `json:"quantity"`
}

// workloadForecastResponse is the stored forecast of (site_id, path_id).
type workloadForecastResponse struct {
	SiteID        string                   `json:"site_id"`
	PathID        string                   `json:"path_id"`
	Buckets       []forecastBucketResponse `json:"buckets"`
	TotalQuantity int64                    `json:"total_quantity"`
	ReceivedAt    string                   `json:"received_at"`
	Version       int64                    `json:"version"`
}

// handleReceiveWorkloadForecast backs PUT /workload-forecasts/{site_id}/
// {path_id} (docs/adr/0023 decision 1): 201 with version 1 for a first
// receipt, 200 when the buckets replaced the stored ones (version + 1) and
// 200 with the stored forecast, version and received_at unchanged, when the
// buckets equal the stored ones (no event). 400 for a malformed body or cpt,
// 422 for a forecast the aggregate refuses, 409 when the stored version moved
// twice under the request.
func (s *Server) handleReceiveWorkloadForecast(w http.ResponseWriter, r *http.Request) {
	var req receiveWorkloadForecastRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	buckets, ok := toForecastBuckets(w, r, req.Buckets)
	if !ok {
		return
	}
	res, err := s.ReceiveWorkloadForecast.Handle(r.Context(), usecases.ReceiveWorkloadForecastCommand{
		SiteID:  chi.URLParam(r, "site_id"),
		PathID:  chi.URLParam(r, "path_id"),
		Buckets: buckets,
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	status := http.StatusOK
	if res.Created {
		status = http.StatusCreated
	}
	writeJSON(w, status, toWorkloadForecastResponse(res.Forecast))
}

// handleGetWorkloadForecast backs GET /workload-forecasts/{site_id}/
// {path_id}: 200 with the stored forecast, 404 workload-forecast-not-found.
func (s *Server) handleGetWorkloadForecast(w http.ResponseWriter, r *http.Request) {
	f, err := s.GetWorkloadForecast.Handle(r.Context(), chi.URLParam(r, "site_id"), chi.URLParam(r, "path_id"))
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkloadForecastResponse(f))
}

// toForecastBuckets parses the request buckets. An empty cpt is left as the
// zero instant for the aggregate to refuse (422 missing-cpt); a cpt that is
// not RFC 3339 is 400 malformed-cpt and an absent quantity 422
// missing-bucket-quantity.
func toForecastBuckets(w http.ResponseWriter, r *http.Request, in []forecastBucketRequest) ([]workloadforecast.Bucket, bool) {
	out := make([]workloadforecast.Bucket, 0, len(in))
	for _, b := range in {
		var cpt time.Time
		if b.CPT != "" {
			parsed, err := time.Parse(time.RFC3339, b.CPT)
			if err != nil {
				writeProblem(w, http.StatusBadRequest, problemMalformedCPT, err.Error(), r.URL.Path)
				return nil, false
			}
			cpt = parsed
		}
		if b.Quantity == nil {
			writeProblem(w, http.StatusUnprocessableEntity, problemMissingBucketQuantity, "the bucket with cpt \""+b.CPT+"\" has no quantity", r.URL.Path)
			return nil, false
		}
		out = append(out, workloadforecast.Bucket{CPT: cpt, Quantity: *b.Quantity})
	}
	return out, true
}

func toWorkloadForecastResponse(f *workloadforecast.Forecast) workloadForecastResponse {
	buckets := f.Buckets()
	out := make([]forecastBucketResponse, 0, len(buckets))
	for _, b := range buckets {
		out = append(out, forecastBucketResponse{CPT: b.CPT.UTC().Format(time.RFC3339), Quantity: b.Quantity})
	}
	return workloadForecastResponse{
		SiteID:        f.SiteID(),
		PathID:        f.PathID(),
		Buckets:       out,
		TotalQuantity: f.TotalQuantity(),
		ReceivedAt:    f.ReceivedAt().UTC().Format(time.RFC3339),
		Version:       f.Version(),
	}
}
