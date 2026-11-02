package mcp

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

type declareStationStandardInput struct {
	Location      string  `json:"location" jsonschema:"the site (building) code the standard applies to, e.g. SIM1; equals the first dash-separated segment of the facility zone ids"`
	ProcessType   string  `json:"process_type" jsonschema:"the process the stations perform, e.g. PACK; matches a facility work-center activity and a process path step"`
	Quantity      float64 `json:"quantity" jsonschema:"the throughput of ONE station per period; must be positive"`
	Unit          string  `json:"unit" jsonschema:"the process's natural unit: UNIT, PACKAGE or ORDER (LINE cannot be normalized and is rejected)"`
	PeriodSeconds float64 `json:"period_seconds" jsonschema:"the period the quantity is measured over, in seconds; must be positive (3600 = per hour)"`
}

type stationStandardView struct {
	Location      string  `json:"location"`
	ProcessType   string  `json:"process_type"`
	Quantity      float64 `json:"quantity"`
	Unit          string  `json:"unit"`
	PeriodSeconds float64 `json:"period_seconds"`
}

type declareStationStandardOutput struct {
	Location      string  `json:"location"`
	ProcessType   string  `json:"process_type"`
	Quantity      float64 `json:"quantity"`
	Unit          string  `json:"unit"`
	PeriodSeconds float64 `json:"period_seconds"`
	Created       bool    `json:"created" jsonschema:"true when a new standard was declared, false when an existing one was replaced"`
}

func toStationStandardView(s processcapacity.StationStandard) stationStandardView {
	rate := s.PerStation()
	return stationStandardView{
		Location:      s.Location(),
		ProcessType:   string(s.ProcessType()),
		Quantity:      rate.Quantity(),
		Unit:          string(rate.Unit()),
		PeriodSeconds: rate.Period().Seconds(),
	}
}

func (d Deps) declareStationStandard(ctx context.Context, in declareStationStandardInput) (declareStationStandardOutput, error) {
	standard, created, err := d.DeclareStationStandard.Handle(ctx, usecases.DeclareStationStandardCommand{
		Location:    in.Location,
		ProcessType: processcapacity.ProcessType(in.ProcessType),
		Quantity:    in.Quantity,
		Unit:        processcapacity.CapacityUnit(in.Unit),
		Period:      time.Duration(in.PeriodSeconds * float64(time.Second)),
	})
	if err != nil {
		return declareStationStandardOutput{}, mapError(err)
	}
	v := toStationStandardView(standard)
	return declareStationStandardOutput{
		Location: v.Location, ProcessType: v.ProcessType, Quantity: v.Quantity, Unit: v.Unit, PeriodSeconds: v.PeriodSeconds,
		Created: created,
	}, nil
}

type listStationStandardsInput struct {
	Location string `json:"location,omitempty" jsonschema:"optional site code to filter by; omit to list every declared standard"`
}

type listStationStandardsOutput struct {
	Location  string                `json:"location,omitempty"`
	Standards []stationStandardView `json:"standards"`
}

func (d Deps) listStationStandards(ctx context.Context, in listStationStandardsInput) (listStationStandardsOutput, error) {
	standards, err := d.StationStandards.List(ctx, in.Location)
	if err != nil {
		return listStationStandardsOutput{}, mapError(err)
	}
	out := make([]stationStandardView, 0, len(standards))
	for _, s := range standards {
		out = append(out, toStationStandardView(s))
	}
	return listStationStandardsOutput{Location: in.Location, Standards: out}, nil
}
