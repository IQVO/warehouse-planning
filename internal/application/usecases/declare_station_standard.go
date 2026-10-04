package usecases

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// DeclareStationStandardCommand carries the operator's declaration of the
// throughput of ONE station of a process at a site (location), e.g. 180
// PACKAGE per 3600s for PACK at SIM1.
type DeclareStationStandardCommand struct {
	Location    string
	ProcessType processcapacity.ProcessType
	Quantity    float64
	Unit        processcapacity.CapacityUnit
	Period      time.Duration
}

// DeclareStationStandard upserts a StationStandard. It is a planning
// parameter owned by this context -- no upstream publishes per-station
// throughput -- so declaring it is an explicit operator action (ADR 0002).
type DeclareStationStandard struct {
	Repo ports.StationStandardRepository
}

// Handle validates the declaration (processcapacity.NewCapacityRate and
// NewStationStandard errors: negative quantity, non-positive period, zero
// throughput, un-normalizable unit, blank key), saves it and reports whether
// it created a new standard (true) or replaced an existing one (false).
func (uc *DeclareStationStandard) Handle(ctx context.Context, cmd DeclareStationStandardCommand) (processcapacity.StationStandard, bool, error) {
	rate, err := processcapacity.NewCapacityRate(cmd.Quantity, cmd.Unit, cmd.Period)
	if err != nil {
		return processcapacity.StationStandard{}, false, err
	}
	standard, err := processcapacity.NewStationStandard(cmd.Location, cmd.ProcessType, rate)
	if err != nil {
		return processcapacity.StationStandard{}, false, err
	}
	existing, err := uc.Repo.Find(ctx, cmd.Location, cmd.ProcessType)
	if err != nil {
		return processcapacity.StationStandard{}, false, err
	}
	if err := uc.Repo.Save(ctx, standard); err != nil {
		return processcapacity.StationStandard{}, false, err
	}
	return standard, existing == nil, nil
}
