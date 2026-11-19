package http

import (
	"net/http"
	"strings"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// planComparisonResponse is GET /capacity-plans/comparisons' body
// (docs/adr/0024 decision 4): the shared site and window, then one item per
// plan in the order of ids, exactly as stored.
type planComparisonResponse struct {
	SiteID      string             `json:"site_id"`
	WindowStart string             `json:"window_start"`
	WindowEnd   string             `json:"window_end"`
	Items       []comparedPlanItem `json:"items"`
}

// comparedPlanItem is one plan of a comparison: its label and status, the
// demand it was evaluated against and where that came from, the capacity
// and shortage it computed when it was created, and the ADR 0022 labour
// requirement. Nothing is recomputed.
type comparedPlanItem struct {
	PlanID               string                  `json:"plan_id"`
	Scenario             string                  `json:"scenario,omitempty"`
	Status               string                  `json:"status"`
	PathID               string                  `json:"path_id"`
	AssignedDemand       float64                 `json:"assigned_demand"`
	DemandSource         string                  `json:"demand_source"`
	PathCapacity         float64                 `json:"path_capacity"`
	CapacityOverWindow   float64                 `json:"capacity_over_window"`
	Shortage             float64                 `json:"shortage"`
	BottleneckStep       string                  `json:"bottleneck_step"`
	BottleneckConstraint string                  `json:"bottleneck_constraint"`
	LabourRequirement    []labourRequirementItem `json:"labour_requirement"`
}

// handleCompareCapacityPlans backs GET /capacity-plans/comparisons?ids=a,b:
// 2 to 5 distinct plans of one site and window, side by side. 422
// invalid-comparison-ids (count, blank or repeated id),
// comparison-site-mismatch, comparison-window-mismatch; 404
// capacity-plan-not-found for an unknown id; 400 unknown-query-parameter for
// anything but ids. A static route: chi never hands "comparisons" to
// /capacity-plans/{id}.
func (s *Server) handleCompareCapacityPlans(w http.ResponseWriter, r *http.Request) {
	for name := range r.URL.Query() {
		if name != "ids" {
			writeProblem(w, http.StatusBadRequest, problemInfo{"unknown-query-parameter", "Unknown query parameter"}, "query parameter \""+name+"\" is not declared by this read (it takes ids only)", r.URL.Path)
			return
		}
	}
	ids := strings.Split(r.URL.Query().Get("ids"), ",")
	for i := range ids {
		ids[i] = strings.TrimSpace(ids[i])
	}
	plans, err := s.CompareCapacityPlans.Handle(r.Context(), ids)
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, toComparisonResponse(plans))
}

func toComparisonResponse(plans []*capacityplan.CapacityPlan) planComparisonResponse {
	first := plans[0]
	out := planComparisonResponse{
		SiteID:      first.SiteID(),
		WindowStart: first.Window().Start().UTC().Format(time.RFC3339),
		WindowEnd:   first.Window().End().UTC().Format(time.RFC3339),
		Items:       make([]comparedPlanItem, 0, len(plans)),
	}
	for _, p := range plans {
		out.Items = append(out.Items, comparedPlanItem{
			PlanID: p.ID(), Scenario: p.Scenario(), Status: string(p.Status()), PathID: p.ProcessPathID(),
			AssignedDemand: p.AssignedDemand(), DemandSource: string(p.DemandSource()),
			PathCapacity: p.PathCapacity(), CapacityOverWindow: p.CapacityOverWindow(), Shortage: p.Shortage(),
			BottleneckStep: string(p.BottleneckStep()), BottleneckConstraint: string(p.BottleneckConstraint()),
			LabourRequirement: toLabourRequirementItems(p.LabourRequirement()),
		})
	}
	return out
}
