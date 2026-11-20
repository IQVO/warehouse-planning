package processpath

import (
	"errors"
	"testing"
)

func TestSourceOfAPath(t *testing.T) {
	steps := []ProcessType{"PICK", "PACK"}
	local, err := NewProcessPath("p", "n", steps)
	if err != nil || local.Source() != SourceLocal || SourceLocal != "local" {
		t.Fatalf("a declared path must be source local, got %q (%v)", local.Source(), err)
	}
	flow, err := NewFlowProcessPath("p", "n", steps)
	if err != nil || flow.Source() != SourceFlow || SourceFlow != "flow" {
		t.Fatalf("a flow's path must be source flow, got %q (%v)", flow.Source(), err)
	}
	if flow.ID() != "p" || flow.Name() != "n" || len(flow.Steps()) != 2 {
		t.Fatalf("flow path lost its identity or steps: %+v", flow)
	}
}

func TestNewFlowProcessPath_RejectsEmptySteps(t *testing.T) {
	if _, err := NewFlowProcessPath("p", "n", nil); !errors.Is(err, ErrEmptySteps) {
		t.Fatalf("err = %v, want ErrEmptySteps", err)
	}
}
