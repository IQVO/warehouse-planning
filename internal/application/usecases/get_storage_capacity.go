package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// StoragePositions is the number of storage positions facility-layout
// registered in one zone for one locationType.
type StoragePositions struct {
	ZoneID       string
	LocationType string
	Positions    int
}

// ZoneStations is the number of work-center stations facility-layout
// registered in one zone for one activity.
type ZoneStations struct {
	ZoneID   string
	Activity string
	Stations int
}

// StorageCapacity is the read model of one site: storage positions per
// (zone, locationType) and station counts per (zone, activity). It is NOT a
// throughput and has no "consumed" figure -- stock must never be read from
// inventory-storage (ADR 0002; design doc rule 1, sections 14-17).
type StorageCapacity struct {
	Location         string
	StoragePositions []StoragePositions
	Stations         []ZoneStations
}

// GetStorageCapacity reads the facility-layout tally of a site.
type GetStorageCapacity struct {
	Tally ports.StorageTallyReader
}

// Handle lists the site's tallied storage positions and stations, ordered by
// zone then key. An unknown or empty site yields empty lists, not an error.
func (uc *GetStorageCapacity) Handle(ctx context.Context, location string) (StorageCapacity, error) {
	buckets, err := uc.Tally.SiteBuckets(ctx, location)
	if err != nil {
		return StorageCapacity{}, err
	}
	out := StorageCapacity{Location: location, StoragePositions: []StoragePositions{}, Stations: []ZoneStations{}}
	for _, b := range buckets {
		switch b.TallyType {
		case tally.TypeLocation:
			out.StoragePositions = append(out.StoragePositions, StoragePositions{ZoneID: b.ZoneID, LocationType: b.TallyKey, Positions: b.Count})
		case tally.TypeStation:
			out.Stations = append(out.Stations, ZoneStations{ZoneID: b.ZoneID, Activity: b.TallyKey, Stations: b.Count})
		}
	}
	return out, nil
}
