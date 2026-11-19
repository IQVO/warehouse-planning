package http

import (
	"net/http"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// listPlanFilters are the query parameters GET /capacity-plans declares
// besides limit and cursor (docs/adr/0024 decision 2).
var listPlanFilters = []string{"location", "site_id", "path_id", "window_start", "window_end"}

// planListFilter reads the additive list filters of GET /capacity-plans. An
// omitted or empty parameter does not filter. window_start and window_end
// are RFC 3339 instants validated like every other window parameter: a
// malformed one is 400 malformed-window-start / malformed-window-end, and
// both sent with the end not after the start is 400 invalid-capacity-window.
// On a bad parameter the problem is written and ok is false.
func planListFilter(w http.ResponseWriter, r *http.Request) (filter capacityplan.ListFilter, ok bool) {
	q := r.URL.Query()
	filter = capacityplan.ListFilter{Location: q.Get("location"), SiteID: q.Get("site_id"), PathID: q.Get("path_id")}
	if filter.WindowStart, ok = optionalInstant(w, r, "window_start", problemMalformedWindowStart); !ok {
		return filter, false
	}
	if filter.WindowEnd, ok = optionalInstant(w, r, "window_end", problemMalformedWindowEnd); !ok {
		return filter, false
	}
	if filter.WindowStart != nil && filter.WindowEnd != nil {
		if _, err := processcapacity.NewCapacityWindow(*filter.WindowStart, *filter.WindowEnd); err != nil {
			writeError(w, r, err)
			return filter, false
		}
	}
	return filter, true
}

// optionalInstant parses the RFC 3339 query parameter name; empty is nil.
func optionalInstant(w http.ResponseWriter, r *http.Request, name string, malformed problemInfo) (*time.Time, bool) {
	raw := r.URL.Query().Get(name)
	if raw == "" {
		return nil, true
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, malformed, err.Error(), r.URL.Path)
		return nil, false
	}
	return &at, true
}
