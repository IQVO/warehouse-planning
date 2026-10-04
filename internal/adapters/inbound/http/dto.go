// Package http is the inbound chi adapter: DTOs, handlers, routing, and
// domain-error-to-HTTP-status mapping. Domain structs never cross this
// boundary.
package http

// registerConstraintRequest is POST /process-capacities' request body.
type registerConstraintRequest struct {
	ProcessType    string  `json:"process_type"`
	Location       string  `json:"location"`
	WindowStart    string  `json:"window_start"`
	WindowEnd      string  `json:"window_end"`
	ConstraintType string  `json:"constraint_type"`
	Quantity       float64 `json:"quantity"`
	Unit           string  `json:"unit"`
	PeriodSeconds  float64 `json:"period_seconds"`
}

// effectiveCapacityResponse is POST and GET /process-capacities' shared
// response shape for the computed effective rate.
type effectiveCapacityResponse struct {
	EffectiveRate     float64              `json:"effective_rate"`
	EffectiveUnit     string               `json:"effective_unit"`
	BindingConstraint string               `json:"binding_constraint"`
	Constraints       []constraintResponse `json:"constraints,omitempty"`
}

// constraintResponse is one entry in GET /process-capacities' constraints
// list.
type constraintResponse struct {
	ConstraintType string  `json:"constraint_type"`
	Quantity       float64 `json:"quantity"`
	Unit           string  `json:"unit"`
	PeriodSeconds  float64 `json:"period_seconds"`
}

// registerProcessPathRequest is POST /process-paths' request body:
// id/name/an ordered, non-empty list of step process types.
type registerProcessPathRequest struct {
	ID    string   `json:"id"`
	Name  string   `json:"name"`
	Steps []string `json:"steps"`
}

// processPathResponse is POST /process-paths' response body, echoing back
// the registered path.
type processPathResponse struct {
	ID    string   `json:"id"`
	Name  string   `json:"name"`
	Steps []string `json:"steps"`
}

// processPathCapacityResponse is GET /process-paths/{id}/capacity's
// response body: the WorkloadProfile-normalized rate (always ORDER) and
// which step is the bottleneck.
type processPathCapacityResponse struct {
	NormalizedRate float64 `json:"normalized_rate"`
	NormalizedUnit string  `json:"normalized_unit"`
	BottleneckStep string  `json:"bottleneck_step"`
}

// createCapacityPlanRequest is POST /capacity-plans' request body.
// assigned_demand (orders) is a pointer so an absent field is rejected
// instead of silently meaning zero demand. units_per_order and
// packages_per_order are the WorkloadProfile's conversion factors, carried
// on the request exactly like Phase 2's path-capacity endpoint.
type createCapacityPlanRequest struct {
	WarehouseID      string   `json:"warehouse_id"`
	Location         string   `json:"location"`
	WindowStart      string   `json:"window_start"`
	WindowEnd        string   `json:"window_end"`
	PathID           string   `json:"path_id"`
	AssignedDemand   *float64 `json:"assigned_demand"`
	UnitsPerOrder    *float64 `json:"units_per_order"`
	PackagesPerOrder *float64 `json:"packages_per_order"`
}

// capacityPlanResponse is the CapacityPlan representation returned by all
// three /capacity-plans endpoints. Quantities are orders; path_capacity is
// ORDER per HOUR.
type capacityPlanResponse struct {
	ID                 string  `json:"id"`
	WarehouseID        string  `json:"warehouse_id"`
	Location           string  `json:"location"`
	WindowStart        string  `json:"window_start"`
	WindowEnd          string  `json:"window_end"`
	PathID             string  `json:"path_id"`
	AssignedDemand     float64 `json:"assigned_demand"`
	Status             string  `json:"status"`
	PathCapacity       float64 `json:"path_capacity"`
	BottleneckStep     string  `json:"bottleneck_step"`
	CapacityOverWindow float64 `json:"capacity_over_window"`
	Shortage           float64 `json:"shortage"`
	CreatedAt          string  `json:"created_at"`
	PublishedAt        *string `json:"published_at,omitempty"`
}
