package usecases

import (
	"context"
	"errors"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

func TestRegisterProcessPath_PersistsAndReturnsThePath(t *testing.T) {
	repo := memory.NewProcessPathRepo()
	uc := &RegisterProcessPath{Repo: repo}
	ctx := context.Background()

	got, err := uc.Handle(ctx, RegisterProcessPathCommand{
		ID:    "pick-rebin-pack",
		Name:  "Pick-Rebin-Pack",
		Steps: []processpath.ProcessType{"PICK", "REBIN", "PACK"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID() != "pick-rebin-pack" || got.Name() != "Pick-Rebin-Pack" {
		t.Fatalf("unexpected returned path: %+v", got)
	}

	stored, err := repo.FindByID(ctx, "pick-rebin-pack")
	if err != nil {
		t.Fatalf("unexpected error reading back: %v", err)
	}
	if stored == nil {
		t.Fatal("expected the path to have been persisted")
	}
	if len(stored.Steps()) != 3 {
		t.Fatalf("expected 3 persisted steps, got %d", len(stored.Steps()))
	}
}

func TestRegisterProcessPath_RejectsEmptySteps(t *testing.T) {
	repo := memory.NewProcessPathRepo()
	uc := &RegisterProcessPath{Repo: repo}

	_, err := uc.Handle(context.Background(), RegisterProcessPathCommand{
		ID:   "empty-path",
		Name: "Empty",
	})
	if !errors.Is(err, processpath.ErrEmptySteps) {
		t.Fatalf("expected ErrEmptySteps, got %v", err)
	}

	stored, err := repo.FindByID(context.Background(), "empty-path")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stored != nil {
		t.Fatal("expected nothing to have been persisted for a rejected command")
	}
}

// TestRegisterProcessPath_UpsertReplacesThePriorPath proves re-registering
// an existing id replaces the stored path wholesale rather than failing or
// merging -- ProcessPath has no in-place mutation, so this is the only way
// an id's path can ever change.
func TestRegisterProcessPath_UpsertReplacesThePriorPath(t *testing.T) {
	repo := memory.NewProcessPathRepo()
	uc := &RegisterProcessPath{Repo: repo}
	ctx := context.Background()

	if _, err := uc.Handle(ctx, RegisterProcessPathCommand{
		ID:    "path-1",
		Name:  "Original",
		Steps: []processpath.ProcessType{"PICK"},
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := uc.Handle(ctx, RegisterProcessPathCommand{
		ID:    "path-1",
		Name:  "Replacement",
		Steps: []processpath.ProcessType{"PICK", "PACK"},
	}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stored, err := repo.FindByID(ctx, "path-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stored.Name() != "Replacement" {
		t.Fatalf("expected the replacement to have won, got %v", stored.Name())
	}
	if len(stored.Steps()) != 2 {
		t.Fatalf("expected 2 steps after replacement, got %d", len(stored.Steps()))
	}
}
