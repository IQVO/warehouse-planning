// Package processpath holds ProcessPath, the ordered sequence of process
// steps a workload flows through (Pick -> Rebin -> Pack). A path is either
// declared locally (POST /process-paths, source local) or the view of a
// process flow consumed from process-path-management (source flow, ADR 0025,
// which wins over a local path with the same id). Before ADR 0025 the two
// contexts shared only the `path_id` string (ADR 0001 Addendum).
// ProcessPath is a value-like read model: it offers construction only and
// no update/mutation method; a changed path replaces the old one wholesale.
package processpath

import "errors"

// ProcessType names a warehouse process step (e.g. "PICK", "PACK",
// "REBIN"). This mirrors processcapacity.ProcessType's shape -- both are
// thin, context-local string identifiers for the same
// process-path-management concept -- but is declared separately in this
// package rather than imported from processcapacity: the
// ComputeProcessPathCapacity domain service (in internal/domain/
// processcapacity) needs to import processpath.ProcessPath, so processpath
// must not import back or the two packages would form an import cycle.
// Callers convert between the two with a plain string cast (see
// ComputeProcessPathCapacity's doc comment).
type ProcessType string

// ErrEmptySteps is returned when a ProcessPath is constructed with no
// steps. A path with no steps can have neither a bottleneck nor a
// capacity, so it is rejected at construction rather than left to fail
// confusingly wherever it is first used.
var ErrEmptySteps = errors.New("processpath: steps must not be empty")

// ProcessPath is an ordered sequence of ProcessTypes a given workload must
// flow through (e.g. Pick -> Rebin -> Pack). It is locally owned and
// operator-declared in warehouse-planning (ADR 0001 Addendum); it is not
// derived from any process-path-management event. THIS TYPE DELIBERATELY
// EXPOSES NO MUTATION/UPDATE METHOD beyond construction: a changed path is
// a brand new ProcessPath that replaces the old one wholesale via
// RegisterProcessPath/Save, never an in-place edit of this struct.
type ProcessPath struct {
	id     string
	name   string
	steps  []ProcessType
	source Source
}

// Source says where a ProcessPath came from (docs/adr/0025): a process flow
// consumed from process-path-management, or a path declared locally with
// POST /process-paths. It is read-only: reads report it, nothing sets it.
type Source string

// The two sources that coexist until the local declaration retires.
const (
	SourceFlow  Source = "flow"
	SourceLocal Source = "local"
)

// NewProcessPath constructs a locally declared ProcessPath (source local),
// rejecting an empty step list.
func NewProcessPath(id, name string, steps []ProcessType) (ProcessPath, error) {
	return build(id, name, steps, SourceLocal)
}

// NewFlowProcessPath constructs the ProcessPath a process flow resolves to
// (source flow): the flow's id and name, the stages' process types as steps.
func NewFlowProcessPath(id, name string, steps []ProcessType) (ProcessPath, error) {
	return build(id, name, steps, SourceFlow)
}

func build(id, name string, steps []ProcessType, source Source) (ProcessPath, error) {
	if len(steps) == 0 {
		return ProcessPath{}, ErrEmptySteps
	}
	stepsCopy := make([]ProcessType, len(steps))
	copy(stepsCopy, steps)
	return ProcessPath{id: id, name: name, steps: stepsCopy, source: source}, nil
}

// Source reports where the path came from: SourceFlow or SourceLocal.
func (p ProcessPath) Source() Source { return p.source }

// ID returns the path's identifier.
func (p ProcessPath) ID() string { return p.id }

// Name returns the path's human-readable name.
func (p ProcessPath) Name() string { return p.name }

// Steps returns the path's ordered steps. Returns a defensive copy so a
// caller mutating the returned slice can never corrupt this ProcessPath's
// internal state.
func (p ProcessPath) Steps() []ProcessType {
	out := make([]ProcessType, len(p.steps))
	copy(out, p.steps)
	return out
}
