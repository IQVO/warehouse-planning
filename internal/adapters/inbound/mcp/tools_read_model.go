package mcp

import (
	"context"
	"time"
)

type getStorageCapacityInput struct {
	Location string `json:"location" jsonschema:"the site (building) code, e.g. SIM1; its zones are the facility zones whose id starts with SIM1-"`
}

type storagePositionsView struct {
	ZoneID       string `json:"zone_id"`
	LocationType string `json:"location_type"`
	Positions    int    `json:"positions"`
}

type zoneStationsView struct {
	ZoneID   string `json:"zone_id"`
	Activity string `json:"activity"`
	Stations int    `json:"stations"`
}

type storageCapacityOutput struct {
	Location         string                 `json:"location"`
	StoragePositions []storagePositionsView `json:"storage_positions"`
	Stations         []zoneStationsView     `json:"stations"`
}

func (d Deps) getStorageCapacity(ctx context.Context, in getStorageCapacityInput) (storageCapacityOutput, error) {
	if in.Location == "" {
		return storageCapacityOutput{}, toolError("missing-location", "location is required")
	}
	capacity, err := d.GetStorageCapacity.Handle(ctx, in.Location)
	if err != nil {
		return storageCapacityOutput{}, mapError(err)
	}
	out := storageCapacityOutput{
		Location:         capacity.Location,
		StoragePositions: make([]storagePositionsView, 0, len(capacity.StoragePositions)),
		Stations:         make([]zoneStationsView, 0, len(capacity.Stations)),
	}
	for _, p := range capacity.StoragePositions {
		out.StoragePositions = append(out.StoragePositions, storagePositionsView{ZoneID: p.ZoneID, LocationType: p.LocationType, Positions: p.Positions})
	}
	for _, s := range capacity.Stations {
		out.Stations = append(out.Stations, zoneStationsView{ZoneID: s.ZoneID, Activity: s.Activity, Stations: s.Stations})
	}
	return out, nil
}

type getExpectedDemandInput struct {
	Location    string `json:"location" jsonschema:"the site code the orders are attributed to, e.g. SIM1 (order-management orders carry no site, so all of them are attributed to one configured site)"`
	WindowStart string `json:"window_start" jsonschema:"window start, RFC3339 timestamp; inclusive: an order promised exactly at window_start counts"`
	WindowEnd   string `json:"window_end" jsonschema:"window end, RFC3339 timestamp; exclusive: an order promised exactly at window_end belongs to the next window; must be after window_start"`
}

// expectedDemandOutput is identical to the REST GET /demand body.
type expectedDemandOutput struct {
	Location      string  `json:"location"`
	WindowStart   string  `json:"window_start"`
	WindowEnd     string  `json:"window_end"`
	Orders        int     `json:"orders"`
	ReleasedLines int     `json:"released_lines"`
	Source        string  `json:"source"`
	AsOf          *string `json:"as_of"`
}

func (d Deps) getExpectedDemand(ctx context.Context, in getExpectedDemandInput) (expectedDemandOutput, error) {
	if in.Location == "" {
		return expectedDemandOutput{}, toolError("missing-location", "location is required")
	}
	start, end, err := parseWindow(in.WindowStart, in.WindowEnd)
	if err != nil {
		return expectedDemandOutput{}, err
	}
	summary, err := d.GetExpectedDemand.Handle(ctx, in.Location, start, end)
	if err != nil {
		return expectedDemandOutput{}, mapError(err)
	}
	out := expectedDemandOutput{
		Location:      in.Location,
		WindowStart:   start.UTC().Format(time.RFC3339),
		WindowEnd:     end.UTC().Format(time.RFC3339),
		Orders:        summary.Orders,
		ReleasedLines: summary.ReleasedLines,
		Source:        "order-management",
	}
	if !summary.AsOf.IsZero() {
		asOf := summary.AsOf.UTC().Format(time.RFC3339)
		out.AsOf = &asOf
	}
	return out, nil
}
