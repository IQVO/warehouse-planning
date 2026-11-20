// Package processflow holds the local copy of process-path-management's
// ProcessFlow (docs/adr/0025, producer ADR 0042): one Flow per flow id, the
// ordered stages a workload passes through, fed only by its
// ProcessFlowDefined / ProcessFlowRevised / ProcessFlowRetired integration
// events. Every event carries the FULL state and the aggregate version, so
// the one rule of the copy is Supersedes: a message is applied only when its
// version is greater than the stored one, and replay or reordering is
// harmless.
//
// The package performs NO I/O and imports nothing from this module.
package processflow

import (
	"errors"
	"time"
)

// ProcessType is the process type of one stage. The set is the closed one
// process-path-management publishes (the task types fulfillment-execution
// publishes).
type ProcessType string

// The closed set of stage process types.
const (
	Pick      ProcessType = "PICK"
	Rebin     ProcessType = "REBIN"
	Pack      ProcessType = "PACK"
	Slam      ProcessType = "SLAM"
	Dispatch  ProcessType = "DISPATCH"
	Arrival   ProcessType = "ARRIVAL"
	Relocate  ProcessType = "RELOCATE"
	Replenish ProcessType = "REPLENISH"
)

// Status is the lifecycle state of a flow.
type Status string

// The two states a flow message carries.
const (
	StatusActive  Status = "ACTIVE"
	StatusRetired Status = "RETIRED"
)

var (
	// ErrRequiredField is returned when the flow id, a stage's path id or the
	// event time is missing.
	ErrRequiredField = errors.New("processflow: flow_id, every stage's path_id and the event time are required")

	// ErrNoStages is returned for a flow without stages: a flow with no
	// stage has no steps.
	ErrNoStages = errors.New("processflow: stages must not be empty")

	// ErrUnknownProcessType is returned for a stage whose process_type is
	// outside the closed set.
	ErrUnknownProcessType = errors.New("processflow: process_type must be one of PICK, REBIN, PACK, SLAM, DISPATCH, ARRIVAL, RELOCATE, REPLENISH")

	// ErrUnknownStatus is returned for a status other than ACTIVE or RETIRED.
	ErrUnknownStatus = errors.New("processflow: status must be ACTIVE or RETIRED")

	// ErrNonPositiveVersion is returned for a version below 1.
	ErrNonPositiveVersion = errors.New("processflow: version must be at least 1")
)

// Stage is one stage of a flow: the process-path-management path it names and
// the process type that becomes the plan step.
type Stage struct {
	PathID      string
	ProcessType ProcessType
}

// Flow is one process flow at one version, as process-path-management
// published it.
type Flow struct {
	id        string
	name      string
	stages    []Stage
	status    Status
	version   int64
	updatedAt time.Time
}

// Params carries everything New needs.
type Params struct {
	FlowID  string
	Name    string
	Stages  []Stage
	Status  Status
	Version int64
	// UpdatedAt is the CloudEvents `time` of the event that carried the state.
	UpdatedAt time.Time
}

// New validates p and builds a Flow. The stage order is kept, the time is
// normalized to UTC.
func New(p Params) (Flow, error) {
	if p.FlowID == "" || p.UpdatedAt.IsZero() {
		return Flow{}, ErrRequiredField
	}
	if len(p.Stages) == 0 {
		return Flow{}, ErrNoStages
	}
	for _, s := range p.Stages {
		if s.PathID == "" {
			return Flow{}, ErrRequiredField
		}
		if !s.ProcessType.known() {
			return Flow{}, ErrUnknownProcessType
		}
	}
	if p.Status != StatusActive && p.Status != StatusRetired {
		return Flow{}, ErrUnknownStatus
	}
	if p.Version < 1 {
		return Flow{}, ErrNonPositiveVersion
	}
	stages := make([]Stage, len(p.Stages))
	copy(stages, p.Stages)
	return Flow{
		id: p.FlowID, name: p.Name, stages: stages, status: p.Status,
		version: p.Version, updatedAt: p.UpdatedAt.UTC(),
	}, nil
}

func (t ProcessType) known() bool {
	switch t {
	case Pick, Rebin, Pack, Slam, Dispatch, Arrival, Relocate, Replenish:
		return true
	}
	return false
}

// ID returns the flow id, the path_id plans and path-capacity reads use.
func (f Flow) ID() string { return f.id }

// Name returns the flow's human-readable name.
func (f Flow) Name() string { return f.name }

// Stages returns a copy of the ordered stages.
func (f Flow) Stages() []Stage {
	out := make([]Stage, len(f.stages))
	copy(out, f.stages)
	return out
}

// Steps returns the stages' process types in order: the steps a plan prices.
func (f Flow) Steps() []ProcessType {
	out := make([]ProcessType, 0, len(f.stages))
	for _, s := range f.stages {
		out = append(out, s.ProcessType)
	}
	return out
}

// Status returns ACTIVE or RETIRED.
func (f Flow) Status() Status { return f.status }

// Active reports whether the flow resolves a path id (ACTIVE only).
func (f Flow) Active() bool { return f.status == StatusActive }

// Version returns the aggregate version the state was published at.
func (f Flow) Version() int64 { return f.version }

// UpdatedAt returns the time of the event that carried the state (UTC).
func (f Flow) UpdatedAt() time.Time { return f.updatedAt }

// Supersedes is the version guard of the copy: a message replaces the stored
// state only when its version is strictly greater. storedVersion is 0 when
// the flow is unknown, so the first message of any type is stored. An equal or
// lower version (a redelivery, the startup republish of a caught-up flow, a
// reordered message) changes nothing.
func (f Flow) Supersedes(storedVersion int64) bool { return f.version > storedVersion }
