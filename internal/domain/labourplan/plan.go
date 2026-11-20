// Package labourplan holds the LabourPlan aggregate (docs/adr/0023): the
// heads a process path needs for one shift at one site, at a planned rate
// for a number of hours. It is wes-work-planning's PathPlan moved here with
// its invariants unchanged (planned heads <= installed stations, hours > 0, a
// finite rate x heads x hours) and a site and a shift added to its identity.
//
// A plan whose fields equal the stored ones changes nothing: no event, no
// version bump. The drift between this plan and the heads workforce-management
// committed (drift.go) moved here with it.
//
// The path id of a plan, of a commitment and of every event is held in its
// canonical form (CanonicalPathID: trimmed, upper case), the form the
// workload forecast keys on, so a plan PUT as "pick", a commitment for "PICK"
// and a forecast for " Pick " name one path.
//
// The package performs NO I/O and imports nothing from this module but the
// workloadforecast package's canonical path code.
package labourplan

import (
	"errors"
	"math"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// CanonicalPathID is workloadforecast.CanonicalPathID, the one form of a
// process-path code on the requirement side: trimmed, upper case. A code
// blank after trimming stays blank and is refused with ErrRequiredField.
func CanonicalPathID(pathID string) string { return workloadforecast.CanonicalPathID(pathID) }

// HeadsSource says where planned_heads came from.
type HeadsSource string

const (
	// HeadsFromRequest: the caller sent planned_heads.
	HeadsFromRequest HeadsSource = "request"
	// HeadsFromForecast: planned_heads was derived from the stored workload
	// forecast with DeriveHeads.
	HeadsFromForecast HeadsSource = "forecast"
)

// MaxCount is the largest head or station count a plan holds (wes's
// StationCount bound).
const MaxCount = math.MaxInt32

var (
	// ErrRequiredField is returned when the site, shift or path id is blank
	// (the path id after trimming).
	ErrRequiredField = errors.New("labourplan: site id, shift id and path id are required")

	// ErrInvalidCount is returned for planned heads or installed stations
	// below zero or above MaxCount.
	ErrInvalidCount = errors.New("labourplan: planned heads and installed stations must be whole numbers from 0 to 2147483647")

	// ErrHeadsExceedStations is wes's plan.ErrHeadsExceedStations, moved
	// verbatim.
	ErrHeadsExceedStations = errors.New("labourplan: planned heads exceeds installed stations")

	// ErrInvalidRate is returned when the rate is not above zero.
	ErrInvalidRate = errors.New("labourplan: rate must be positive")

	// ErrInvalidHours is returned when the hours are not above zero.
	ErrInvalidHours = errors.New("labourplan: hours must be positive")

	// ErrThroughputNotFinite is wes's plan.ErrThroughputNotFinite, moved
	// verbatim: rate x heads x hours must be a finite number.
	ErrThroughputNotFinite = errors.New("labourplan: planned throughput is not finite: rate x heads x hours overflows")

	// ErrUnknownHeadsSource is returned for a heads source other than
	// request and forecast.
	ErrUnknownHeadsSource = errors.New("labourplan: heads source must be request or forecast")

	// ErrIdentityMismatch is returned when a revision for another
	// (site, shift, path) is applied to a plan.
	ErrIdentityMismatch = errors.New("labourplan: a revision can only change the plan of the same site, shift and path")

	// ErrInvalidVersion is returned when a stored plan is rehydrated with a
	// version below 1.
	ErrInvalidVersion = errors.New("labourplan: a stored plan has version 1 or more")
)

// Spec is the content of a plan: everything but its version.
type Spec struct {
	SiteID            string
	ShiftID           string
	PathID            string
	PlannedHeads      int
	RateUnitsPerHour  float64
	Hours             float64
	InstalledStations int
	// ShiftStart is optional; deriving heads from the forecast needs it.
	ShiftStart  *time.Time
	HeadsSource HeadsSource
}

// Plan is the LabourPlan aggregate. Build it with New (a first commit) or
// Rehydrate (a stored row); change it with Revise.
type Plan struct {
	spec    Spec
	version int64
}

// New validates a first plan and returns it at version 1 with its
// LabourPlanCommitted event. The path id is stored and published in its
// canonical form.
func New(s Spec, at time.Time) (*Plan, Committed, error) {
	s = s.canonical()
	if err := s.validate(); err != nil {
		return nil, Committed{}, err
	}
	p := &Plan{spec: s.clone(), version: 1}
	return p, p.committed(at), nil
}

// Rehydrate rebuilds a stored plan, re-checking every invariant and holding
// the path id in its canonical form.
func Rehydrate(s Spec, version int64) (*Plan, error) {
	s = s.canonical()
	if err := s.validate(); err != nil {
		return nil, err
	}
	if version < 1 {
		return nil, ErrInvalidVersion
	}
	return &Plan{spec: s.clone(), version: version}, nil
}

// Revise applies a new spec for the same (site, shift, path), the path ids
// compared in their canonical form. A spec equal to the stored one changes
// nothing (changed=false, no event, same version); any other valid spec
// replaces it and bumps the version.
func (p *Plan) Revise(s Spec, at time.Time) (Committed, bool, error) {
	s = s.canonical()
	if err := s.validate(); err != nil {
		return Committed{}, false, err
	}
	if s.SiteID != p.spec.SiteID || s.ShiftID != p.spec.ShiftID || s.PathID != p.spec.PathID {
		return Committed{}, false, ErrIdentityMismatch
	}
	if s.equal(p.spec) {
		return Committed{}, false, nil
	}
	p.spec = s.clone()
	p.version++
	return p.committed(at), true, nil
}

// Spec returns a copy of the plan's content.
func (p *Plan) Spec() Spec { return p.spec.clone() }

// Version starts at 1 and goes up by one per revision that changed the plan.
func (p *Plan) Version() int64 { return p.version }

// PlannedThroughput is rate x heads x hours, in units.
func (p *Plan) PlannedThroughput() float64 { return p.spec.throughput() }

// ShiftEnd is ShiftStart + Hours; ok is false when the plan has no shift
// start.
func (p *Plan) ShiftEnd() (time.Time, bool) {
	if p.spec.ShiftStart == nil {
		return time.Time{}, false
	}
	return ShiftEnd(*p.spec.ShiftStart, p.spec.Hours), true
}

// ShiftEnd is start + hours, the end of the shift window [start, end) the
// forecast is read over when heads are derived.
func ShiftEnd(start time.Time, hours float64) time.Time {
	return start.Add(time.Duration(hours * float64(time.Hour)))
}

// DeriveHeads is the derivation of docs/adr/0023 decision 2:
// ceil(units due in the shift / (rate x hours)). The caller decides that
// there IS forecast data in the shift; zero buckets is no data, never zero
// heads. The rate and hours rules are the plan's own.
func DeriveHeads(unitsDue int64, rateUnitsPerHour, hours float64) (int, error) {
	if !(rateUnitsPerHour > 0) || math.IsInf(rateUnitsPerHour, 1) {
		return 0, ErrInvalidRate
	}
	if !(hours > 0) || math.IsInf(hours, 1) {
		return 0, ErrInvalidHours
	}
	if unitsDue < 0 {
		return 0, ErrInvalidCount
	}
	heads := math.Ceil(float64(unitsDue) / (rateUnitsPerHour * hours))
	if heads > MaxCount {
		return 0, ErrInvalidCount
	}
	return int(heads), nil
}

func (p *Plan) committed(at time.Time) Committed {
	s := p.spec.clone()
	return Committed{
		SiteID:            s.SiteID,
		ShiftID:           s.ShiftID,
		PathID:            s.PathID,
		PlannedHeads:      s.PlannedHeads,
		RateUnitsPerHour:  s.RateUnitsPerHour,
		Hours:             s.Hours,
		InstalledStations: s.InstalledStations,
		ShiftStart:        s.ShiftStart,
		HeadsSource:       s.HeadsSource,
		Version:           p.version,
		At:                at,
	}
}

func (s Spec) validate() error {
	if s.SiteID == "" || s.ShiftID == "" || s.PathID == "" {
		return ErrRequiredField
	}
	if s.PlannedHeads < 0 || s.PlannedHeads > MaxCount || s.InstalledStations < 0 || s.InstalledStations > MaxCount {
		return ErrInvalidCount
	}
	if s.PlannedHeads > s.InstalledStations {
		return ErrHeadsExceedStations
	}
	if !(s.RateUnitsPerHour > 0) {
		return ErrInvalidRate
	}
	if !(s.Hours > 0) {
		return ErrInvalidHours
	}
	if t := s.throughput(); math.IsInf(t, 0) || math.IsNaN(t) {
		return ErrThroughputNotFinite
	}
	if s.HeadsSource != HeadsFromRequest && s.HeadsSource != HeadsFromForecast {
		return ErrUnknownHeadsSource
	}
	return nil
}

func (s Spec) throughput() float64 {
	return s.RateUnitsPerHour * float64(s.PlannedHeads) * s.Hours
}

// canonical returns s with its path id in the canonical form.
func (s Spec) canonical() Spec {
	s.PathID = CanonicalPathID(s.PathID)
	return s
}

func (s Spec) clone() Spec {
	if s.ShiftStart != nil {
		start := *s.ShiftStart
		s.ShiftStart = &start
	}
	return s
}

func (s Spec) equal(o Spec) bool {
	sameStart := (s.ShiftStart == nil && o.ShiftStart == nil) ||
		(s.ShiftStart != nil && o.ShiftStart != nil && s.ShiftStart.Equal(*o.ShiftStart))
	a, b := s, o
	a.ShiftStart, b.ShiftStart = nil, nil
	return sameStart && a == b
}
