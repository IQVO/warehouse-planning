package http

import (
	"net/http"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/demand"
)

// demandSourceOrderManagement labels where GET /demand's figures come from:
// the read model fed by order-management's published order events.
const demandSourceOrderManagement = "order-management"

// expectedDemandResponse is GET /demand's body. orders counts the orders
// whose promise cutoff falls in [window_start, window_end); released_lines
// sums the lines their latest allocation pass released (NOT units:
// order-management's events carry no quantities); as_of is the CloudEvents
// time of the newest order event the site's model reflects (null when the
// site has none).
type expectedDemandResponse struct {
	Location      string  `json:"location"`
	WindowStart   string  `json:"window_start"`
	WindowEnd     string  `json:"window_end"`
	Orders        int     `json:"orders"`
	ReleasedLines int     `json:"released_lines"`
	Source        string  `json:"source"`
	AsOf          *string `json:"as_of"`
}

func toExpectedDemandResponse(location string, start, end time.Time, s demand.Summary) expectedDemandResponse {
	resp := expectedDemandResponse{
		Location:      location,
		WindowStart:   start.UTC().Format(time.RFC3339),
		WindowEnd:     end.UTC().Format(time.RFC3339),
		Orders:        s.Orders,
		ReleasedLines: s.ReleasedLines,
		Source:        demandSourceOrderManagement,
	}
	if !s.AsOf.IsZero() {
		asOf := s.AsOf.UTC().Format(time.RFC3339)
		resp.AsOf = &asOf
	}
	return resp
}

// handleGetExpectedDemand backs GET /demand?location=&window_start=&window_end=:
// the expected demand of a site over a half-open window, read from the local
// read model (docs/adr/0004). 200 even when nothing is known: orders is 0 and
// as_of is null -- an empty answer, which POST /capacity-plans never turns
// into a zero-demand plan.
func (s *Server) handleGetExpectedDemand(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	location := q.Get("location")
	if location == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-location", "location is required"}, "the location (site code) query parameter must be provided", r.URL.Path)
		return
	}
	start, err := time.Parse(time.RFC3339, q.Get("window_start"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-window-start", "window_start must be an RFC3339 timestamp"}, err.Error(), r.URL.Path)
		return
	}
	end, err := time.Parse(time.RFC3339, q.Get("window_end"))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, problemInfo{"malformed-window-end", "window_end must be an RFC3339 timestamp"}, err.Error(), r.URL.Path)
		return
	}
	summary, err := s.GetExpectedDemand.Handle(r.Context(), location, start, end)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toExpectedDemandResponse(location, start, end, summary))
}
