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
