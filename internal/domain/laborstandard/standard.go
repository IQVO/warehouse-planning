// Package laborstandard holds the local copy of labor-performance's
// engineered labour standards (docs/adr/0022): one Standard FACT per
// labor-performance standard id, fed only by its LaborStandardDefined /
// LaborStandardRevised integration events, and the ONE pure rule (Resolve)
// that picks the standard in force for a plan's site, task type and window.
//
// The copy stores facts, not a current value: a revision in labor-performance
// closes the prior record and opens a new one under a new id, so the standard
// that applies is decided at READ time from every stored record, and the
// arrival order of the events never matters.
//
// The package performs NO I/O and imports nothing from this module: the
// memory and Postgres adapters both call Resolve over the rows they hold.
package laborstandard

import (
	"errors"
	"strings"
	"time"
)

// HandlingUnit is the optional handling unit a standard is declared for
// (labor-performance ADR 0035). The empty value is a task-type-only standard,
// the only kind a capacity plan uses: plan demand has no handling-unit split.
type HandlingUnit string

// The handling units labor-performance publishes, plus the task-type-only
// empty value.
const (
	HandlingUnitNone   HandlingUnit = ""
	HandlingUnitEach   HandlingUnit = "EACH"
	HandlingUnitCase   HandlingUnit = "CASE"
	HandlingUnitPallet HandlingUnit = "PALLET"
)

var (
	// ErrRequiredField is returned when the standard id, the task type, the
	// effective-from instant or the as-of time (the CloudEvents `time`) is
	// missing.
	ErrRequiredField = errors.New("laborstandard: standard id, task type, effective_from and the event time are required")

	// ErrNonPositiveExpectedSeconds is returned when expected seconds is not
	// above zero: an engineered standard always takes some time.
	ErrNonPositiveExpectedSeconds = errors.New("laborstandard: expected_seconds must be greater than zero")

	// ErrNegativeTravelComponent is returned for a travel component below
	// zero.
	ErrNegativeTravelComponent = errors.New("laborstandard: travel_component_seconds must not be negative")

	// ErrUnknownHandlingUnit is returned for a handling unit outside EACH,
	// CASE and PALLET (or the empty task-type-only value).
	ErrUnknownHandlingUnit = errors.New("laborstandard: handling_unit must be EACH, CASE or PALLET when present")
)

// Standard is one engineered labour standard record as labor-performance
// published it: the expected seconds of one task of a task type, optionally
// for one handling unit and one site, in force from EffectiveFrom. AsOf is
// the time of the event that carried it.
type Standard struct {
	id              string
	taskType        string
	handlingUnit    HandlingUnit
	siteCode        string
	expectedSeconds int64
	travelSeconds   *int64
	effectiveFrom   time.Time
	asOf            time.Time
}

// Params carries everything New needs.
type Params struct {
	// StandardID is labor-performance's standard id; a revision mints a new
	// one, so every record of a standard's history has its own.
	StandardID string
	// TaskType is the task type the standard measures (PICK, PACK, SLAM on
	// the producer today); it names a plan step of the same process type.
	TaskType string
	// HandlingUnit is empty for a task-type-only standard.
	HandlingUnit HandlingUnit
	// SiteCode is a facility-layout site code; empty means every site.
	SiteCode string
	// ExpectedSeconds is the engineered duration of one task (> 0).
	ExpectedSeconds int64
	// TravelComponentSeconds is the optional part of ExpectedSeconds the
	// producer attributes to travel; nil when it declared none.
	TravelComponentSeconds *int64
	// EffectiveFrom is when the standard came into force.
	EffectiveFrom time.Time
	// AsOf is the CloudEvents `time` of the event that carried the record.
	AsOf time.Time
}

// New validates p and builds a Standard. Both instants are normalized to UTC.
func New(p Params) (Standard, error) {
	if p.StandardID == "" || p.TaskType == "" || p.EffectiveFrom.IsZero() || p.AsOf.IsZero() {
		return Standard{}, ErrRequiredField
	}
	if p.ExpectedSeconds <= 0 {
		return Standard{}, ErrNonPositiveExpectedSeconds
	}
	if p.TravelComponentSeconds != nil && *p.TravelComponentSeconds < 0 {
		return Standard{}, ErrNegativeTravelComponent
	}
	switch p.HandlingUnit {
	case HandlingUnitNone, HandlingUnitEach, HandlingUnitCase, HandlingUnitPallet:
	default:
		return Standard{}, ErrUnknownHandlingUnit
	}
	s := Standard{
		id:              p.StandardID,
		taskType:        p.TaskType,
		handlingUnit:    p.HandlingUnit,
		siteCode:        p.SiteCode,
		expectedSeconds: p.ExpectedSeconds,
		effectiveFrom:   p.EffectiveFrom.UTC(),
		asOf:            p.AsOf.UTC(),
	}
	if p.TravelComponentSeconds != nil {
		travel := *p.TravelComponentSeconds
		s.travelSeconds = &travel
	}
	return s, nil
}

// ID returns labor-performance's standard id.
func (s Standard) ID() string { return s.id }

// TaskType returns the task type the standard measures.
func (s Standard) TaskType() string { return s.taskType }

// HandlingUnit returns the handling unit; empty for a task-type-only standard.
func (s Standard) HandlingUnit() HandlingUnit { return s.handlingUnit }

// SiteCode returns the site the standard is scoped to; empty for every site.
func (s Standard) SiteCode() string { return s.siteCode }

// SiteScoped reports whether the standard is declared for one site rather
// than for every site.
func (s Standard) SiteScoped() bool { return s.siteCode != "" }

// ExpectedSeconds returns the engineered seconds of one task.
func (s Standard) ExpectedSeconds() int64 { return s.expectedSeconds }

// TravelComponentSeconds returns the declared travel part of ExpectedSeconds,
// or nil when the producer declared none. The result is a copy.
func (s Standard) TravelComponentSeconds() *int64 {
	if s.travelSeconds == nil {
		return nil
	}
	travel := *s.travelSeconds
	return &travel
}

// EffectiveFrom returns when the standard came into force (UTC).
func (s Standard) EffectiveFrom() time.Time { return s.effectiveFrom }

// AsOf returns the time of the event that carried the record (UTC).
func (s Standard) AsOf() time.Time { return s.asOf }

// Resolve returns the standard in force for a plan of site siteCode, task
// type taskType and a window starting at windowStart (docs/adr/0022 decision
// 3), and false when none applies. A candidate applies when its task type is
// taskType, it is task-type-only (no handling unit), its site is siteCode or
// every site (empty), and it came into force at or before windowStart. Among
// those:
//
//   - a standard scoped to the plan's site wins over a site-less one;
//   - then the latest EffectiveFrom (a revision closes the prior record and
//     opens a new one, so the latest start before the window is in force);
//   - then the latest AsOf;
//   - then the greatest standard id, so the answer never depends on the
//     order the candidates are given in.
func Resolve(candidates []Standard, siteCode, taskType string, windowStart time.Time) (Standard, bool) {
	var best Standard
	found := false
	for _, c := range candidates {
		if !c.appliesTo(siteCode, taskType, windowStart) {
			continue
		}
		if !found || c.beats(best) {
			best, found = c, true
		}
	}
	return best, found
}

// appliesTo is Resolve's filter.
func (s Standard) appliesTo(siteCode, taskType string, windowStart time.Time) bool {
	return s.taskType == taskType &&
		s.handlingUnit == HandlingUnitNone &&
		(s.siteCode == siteCode || s.siteCode == "") &&
		s.effectiveFrom.Compare(windowStart) <= 0
}

// beats is Resolve's precedence between two applicable standards. Every
// candidate's site is the plan's or empty, so "scoped" is "has a site".
func (s Standard) beats(other Standard) bool {
	if s.SiteScoped() != other.SiteScoped() {
		return s.SiteScoped()
	}
	if c := s.effectiveFrom.Compare(other.effectiveFrom); c != 0 {
		return c == 1
	}
	if c := s.asOf.Compare(other.asOf); c != 0 {
		return c == 1
	}
	return strings.Compare(s.id, other.id) == 1
}
