package architecture

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestNoBuildOutputTrackedAtRepoRoot fails when a file named after a cmd/*
// binary is tracked at the repository root. `go build ./cmd/<name>` writes
// its output there, and a 20 MB Mach-O `api` binary sat in git for weeks
// before anyone noticed (INCIDENTS.md). The names are also in .gitignore; this
// sensor catches a `git add -f` or a new binary nobody added to it.
func TestNoBuildOutputTrackedAtRepoRoot(t *testing.T) {
	out, err := exec.Command("git", "-C", "../..", "ls-files").Output()
	if err != nil {
		t.Skipf("git ls-files unavailable (not a git checkout?): %v", err)
	}
	entries, err := os.ReadDir("../../cmd")
	if err != nil {
		t.Fatalf("read cmd/: %v", err)
	}
	var binaries []string
	for _, e := range entries {
		if e.IsDir() {
			binaries = append(binaries, e.Name())
		}
	}
	if len(binaries) == 0 {
		t.Fatal("found no cmd/* directories: the sensor is pointed at the wrong place")
	}
	for _, v := range trackedRootBuildOutputs(strings.Split(strings.TrimSpace(string(out)), "\n"), binaries) {
		t.Error(v)
	}
}

// trackedRootBuildOutputs returns one violation per tracked root-level path
// that carries the name of a cmd/* binary.
func trackedRootBuildOutputs(tracked, binaries []string) []string {
	var violations []string
	for _, p := range tracked {
		if strings.Contains(p, "/") {
			continue
		}
		if slices.Contains(binaries, filepath.Base(p)) {
			violations = append(violations, "build output "+p+" is tracked at the repo root: git rm --cached it (it is gitignored)")
		}
	}
	return violations
}

// TestRootBuildOutputSensorBites proves the sensor can fail.
func TestRootBuildOutputSensorBites(t *testing.T) {
	bins := []string{"api", "mcp"}
	if got := trackedRootBuildOutputs([]string{"api", "go.mod", "cmd/api/main.go"}, bins); len(got) != 1 {
		t.Fatalf("a tracked root binary: got %v, want one violation", got)
	}
	if got := trackedRootBuildOutputs([]string{"go.mod", "cmd/api/main.go", "docs/api"}, bins); len(got) != 0 {
		t.Fatalf("no root binary: got %v, want none", got)
	}
}
