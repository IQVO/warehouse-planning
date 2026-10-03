package processpath

import (
	"errors"
	"testing"
)

func TestNewProcessPath_RejectsNilSteps(t *testing.T) {
	_, err := NewProcessPath("path-1", "Pick-Rebin-Pack", nil)
	if !errors.Is(err, ErrEmptySteps) {
		t.Fatalf("expected ErrEmptySteps, got %v", err)
	}
}

func TestNewProcessPath_RejectsEmptySteps(t *testing.T) {
	_, err := NewProcessPath("path-1", "Pick-Rebin-Pack", []ProcessType{})
	if !errors.Is(err, ErrEmptySteps) {
		t.Fatalf("expected ErrEmptySteps, got %v", err)
	}
}

func TestNewProcessPath_ExposesIdentityAndSteps(t *testing.T) {
	path, err := NewProcessPath("path-1", "Pick-Rebin-Pack", []ProcessType{"PICK", "REBIN", "PACK"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if path.ID() != "path-1" {
		t.Fatalf("expected id path-1, got %v", path.ID())
	}
	if path.Name() != "Pick-Rebin-Pack" {
		t.Fatalf("expected name Pick-Rebin-Pack, got %v", path.Name())
	}

	steps := path.Steps()
	want := []ProcessType{"PICK", "REBIN", "PACK"}
	if len(steps) != len(want) {
		t.Fatalf("expected %d steps, got %d", len(want), len(steps))
	}
	for i := range want {
		if steps[i] != want[i] {
			t.Fatalf("expected step %d to be %v, got %v", i, want[i], steps[i])
		}
	}
}

// TestProcessPath_Steps_ReturnsDefensiveCopy proves a caller mutating the
// slice Steps() returns can never corrupt the ProcessPath's own state --
// without this test, removing the defensive copy in Steps() would still
// pass every other test here.
func TestProcessPath_Steps_ReturnsDefensiveCopy(t *testing.T) {
	path, err := NewProcessPath("path-1", "Pick-Rebin-Pack", []ProcessType{"PICK", "REBIN", "PACK"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	steps := path.Steps()
	steps[0] = "MUTATED"

	if got := path.Steps()[0]; got != "PICK" {
		t.Fatalf("expected internal steps to be unaffected by mutating the returned slice, got %v", got)
	}
}

// TestNewProcessPath_CopiesInputSteps proves the constructor copies its
// input slice too -- a caller mutating the slice it passed in after
// construction must never affect the stored ProcessPath.
func TestNewProcessPath_CopiesInputSteps(t *testing.T) {
	input := []ProcessType{"PICK", "REBIN", "PACK"}
	path, err := NewProcessPath("path-1", "Pick-Rebin-Pack", input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	input[0] = "MUTATED"

	if got := path.Steps()[0]; got != "PICK" {
		t.Fatalf("expected the stored steps to be unaffected by mutating the input slice, got %v", got)
	}
}
