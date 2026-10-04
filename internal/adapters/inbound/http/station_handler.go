package http

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// handleDeclareStationStandard backs PUT /station-standards/{location}/{process_type}:
// the operator declares the throughput of ONE station of a process at a site
// (e.g. 180 PACKAGE per 3600 s for PACK at SIM1). 201 when the standard is
// new, 200 when it replaced an existing one, 422 when invalid. Station counts
// are tallied from facility-layout; count x this standard is composed with
// LABOR at read time (ADR 0002).
func (s *Server) handleDeclareStationStandard(w http.ResponseWriter, r *http.Request) {
	var req declareStationStandardRequest
	if !decodeJSON(w, r, &req) {
		return
	}

	standard, created, err := s.DeclareStationStandard.Handle(r.Context(), usecases.DeclareStationStandardCommand{
		Location:    chi.URLParam(r, "location"),
		ProcessType: processcapacity.ProcessType(chi.URLParam(r, "process_type")),
		Quantity:    req.Quantity,
		Unit:        processcapacity.CapacityUnit(req.Unit),
		Period:      secondsToDuration(req.PeriodSeconds),
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	writeJSON(w, status, toStationStandardResponse(standard))
}

// handleListStationStandards backs GET /station-standards?location=: the
// declared standards of one site, or every standard when location is absent.
func (s *Server) handleListStationStandards(w http.ResponseWriter, r *http.Request) {
	location := r.URL.Query().Get("location")
	standards, err := s.StationStandards.List(r.Context(), location)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]stationStandardResponse, 0, len(standards))
	for _, standard := range standards {
		out = append(out, toStationStandardResponse(standard))
	}
	writeJSON(w, http.StatusOK, stationStandardsResponse{Location: location, Standards: out})
}

// handleGetStorageCapacity backs GET /storage-capacity?location=: the READ
// MODEL of one site -- storage positions per (zone, locationType) and station
// counts per (zone, activity), tallied from facility-layout. It is not a
// throughput and carries no consumed figure (ADR 0002). 200 even when the
// site has nothing tallied.
func (s *Server) handleGetStorageCapacity(w http.ResponseWriter, r *http.Request) {
	location := r.URL.Query().Get("location")
	if location == "" {
		writeProblem(w, http.StatusBadRequest, problemInfo{"missing-location", "location is required"}, "the location (site code) query parameter must be provided", r.URL.Path)
		return
	}
	capacity, err := s.GetStorageCapacity.Handle(r.Context(), location)
	if err != nil {
		writeError(w, r, err)
		return
	}
	resp := storageCapacityResponse{
		Location:         capacity.Location,
		StoragePositions: make([]storagePositionsItem, 0, len(capacity.StoragePositions)),
		Stations:         make([]zoneStationsItem, 0, len(capacity.Stations)),
	}
	for _, p := range capacity.StoragePositions {
		resp.StoragePositions = append(resp.StoragePositions, storagePositionsItem{ZoneID: p.ZoneID, LocationType: p.LocationType, Positions: p.Positions})
	}
	for _, st := range capacity.Stations {
		resp.Stations = append(resp.Stations, zoneStationsItem{ZoneID: st.ZoneID, Activity: st.Activity, Stations: st.Stations})
	}
	writeJSON(w, http.StatusOK, resp)
}

func toStationStandardResponse(s processcapacity.StationStandard) stationStandardResponse {
	rate := s.PerStation()
	return stationStandardResponse{
		Location:      s.Location(),
		ProcessType:   string(s.ProcessType()),
		Quantity:      rate.Quantity(),
		Unit:          string(rate.Unit()),
		PeriodSeconds: rate.Period().Seconds(),
	}
}
