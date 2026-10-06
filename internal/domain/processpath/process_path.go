// Package processpath holds ProcessPath, the ordered sequence of process
// steps a workload flows through (Pick -> Rebin -> Pack). It is a LOCALLY
// OWNED, operator-declared concept of warehouse-planning (declared via
// POST /process-paths): process-path-management publishes only routing and
// capability metadata, never a step sequence, so nothing is consumed from
// it and this is NOT a Conformist copy (ADR 0001 Addendum). The two
// contexts only share the `path_id` string as a loose cross-reference.
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
	id    string
	name  string
	steps []ProcessType
}

// NewProcessPath constructs a ProcessPath, rejecting an empty step list.
func NewProcessPath(id, name string, steps []ProcessType) (ProcessPath, error) {
	if len(steps) == 0 {
		return ProcessPath{}, ErrEmptySteps
	}
	stepsCopy := make([]ProcessType, len(steps))
	copy(stepsCopy, steps)
	return ProcessPath{id: id, name: name, steps: stepsCopy}, nil
}

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
