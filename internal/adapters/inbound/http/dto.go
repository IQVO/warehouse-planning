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
//
// step_breakdown and warnings are additive (station capacity composition,
// ADR 0002): each step's normalized rate (ORDER per HOUR) and the constraint
// type binding it, and any composition warnings (never null).
type processPathCapacityResponse struct {
	NormalizedRate float64             `json:"normalized_rate"`
	NormalizedUnit string              `json:"normalized_unit"`
	BottleneckStep string              `json:"bottleneck_step"`
	StepBreakdown  []stepBreakdownItem `json:"step_breakdown"`
	Warnings       []string            `json:"warnings"`
}

// stepBreakdownItem is one path step's composed result.
type stepBreakdownItem struct {
	Step              string  `json:"step"`
	NormalizedRate    float64 `json:"normalized_rate"`
	BindingConstraint string  `json:"binding_constraint"`
}

// createCapacityPlanRequest is POST /capacity-plans' request body.
// assigned_demand (orders) is a pointer: present (even 0) it is used as
// stated; ABSENT it is defaulted from the order-management demand read
// model, or rejected with 422 missing-assigned-demand when that has no
// orders for the location and window -- absence never silently means zero
// demand. units_per_order and
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

	// DemandSource says where assigned_demand came from: "request" (stated
	// by the caller; also every plan created before demand ingestion) or
	// "orders" (defaulted from the order-management read model). Additive,
	// docs/adr/0004.
	DemandSource string `json:"demand_source"`
	// BottleneckConstraint (the constraint type binding the bottleneck
	// step, e.g. LABOR or STATION; empty for plans created before it was
	// recorded) and Warnings (never null) are additive, see ADR 0002.
	BottleneckConstraint string   `json:"bottleneck_constraint"`
	Warnings             []string `json:"warnings"`
}

// declareStationStandardRequest is PUT /station-standards/{location}/
// {process_type}'s request body: the throughput of ONE station.
type declareStationStandardRequest struct {
	Quantity      float64 `json:"quantity"`
	Unit          string  `json:"unit"`
	PeriodSeconds float64 `json:"period_seconds"`
}

// stationStandardResponse is one declared StationStandard.
type stationStandardResponse struct {
	Location      string  `json:"location"`
	ProcessType   string  `json:"process_type"`
	Quantity      float64 `json:"quantity"`
	Unit          string  `json:"unit"`
	PeriodSeconds float64 `json:"period_seconds"`
}

// stationStandardsResponse is GET /station-standards' body.
type stationStandardsResponse struct {
	Location  string                    `json:"location,omitempty"`
	Standards []stationStandardResponse `json:"standards"`
}

// storageCapacityResponse is GET /storage-capacity's read model of one site.
type storageCapacityResponse struct {
	Location         string                 `json:"location"`
	StoragePositions []storagePositionsItem `json:"storage_positions"`
	Stations         []zoneStationsItem     `json:"stations"`
}

type storagePositionsItem struct {
	ZoneID       string `json:"zone_id"`
	LocationType string `json:"location_type"`
	Positions    int    `json:"positions"`
}

type zoneStationsItem struct {
	ZoneID   string `json:"zone_id"`
	Activity string `json:"activity"`
	Stations int    `json:"stations"`
}
