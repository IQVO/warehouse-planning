package postgres

import (
	"encoding/json"
	"fmt"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// labourRequirementRow is one element of capacity_plans.labour_requirement
// (JSONB, migration 0013): a plan step's labour requirement as computed at
// creation (docs/adr/0022). The JSON shape is this adapter's storage format
// only; the domain carries no tags.
type labourRequirementRow struct {
	ProcessType          string  `json:"process_type"`
	StandardSeconds      float64 `json:"standard_seconds"`
	StandardSiteScoped   bool    `json:"standard_site_scoped"`
	Tasks                float64 `json:"tasks"`
	RequiredLabourHours  float64 `json:"required_labour_hours"`
	RequiredAverageHeads float64 `json:"required_average_heads"`
	Known                bool    `json:"known"`
	Reason               string  `json:"reason,omitempty"`
}

// encodeLabourRequirement serializes a plan's requirement; an empty one is
// "[]" (the column is NOT NULL).
func encodeLabourRequirement(items []capacityplan.LabourRequirement) ([]byte, error) {
	rows := make([]labourRequirementRow, 0, len(items))
	for _, r := range items {
		rows = append(rows, labourRequirementRow{
			ProcessType:          string(r.ProcessType),
			StandardSeconds:      r.StandardSeconds,
			StandardSiteScoped:   r.StandardSiteScoped,
			Tasks:                r.Tasks,
			RequiredLabourHours:  r.RequiredLabourHours,
			RequiredAverageHeads: r.RequiredAverageHeads,
			Known:                r.Known,
			Reason:               string(r.Reason),
		})
	}
	return json.Marshal(rows)
}

// decodeLabourRequirement reads the column back; a plan stored before
// migration 0013 holds the default "[]" and reads back as no requirement.
func decodeLabourRequirement(raw []byte) ([]capacityplan.LabourRequirement, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var rows []labourRequirementRow
	if err := json.Unmarshal(raw, &rows); err != nil {
		return nil, fmt.Errorf("decode capacity_plans.labour_requirement: %w", err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	out := make([]capacityplan.LabourRequirement, 0, len(rows))
	for _, r := range rows {
		out = append(out, capacityplan.LabourRequirement{
			ProcessType:          processcapacity.ProcessType(r.ProcessType),
			StandardSeconds:      r.StandardSeconds,
			StandardSiteScoped:   r.StandardSiteScoped,
			Tasks:                r.Tasks,
			RequiredLabourHours:  r.RequiredLabourHours,
			RequiredAverageHeads: r.RequiredAverageHeads,
			Known:                r.Known,
			Reason:               capacityplan.LabourReason(r.Reason),
		})
	}
	return out, nil
}
