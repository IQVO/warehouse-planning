package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// RegisterProcessPathCommand carries everything needed to seed one
// ProcessPath read model. There is no event-driven sync from
// process-path-management yet (a later phase); this is a deliberate Phase
// 2 simplification -- see GetProcessPathCapacity's doc comment.
type RegisterProcessPathCommand struct {
	ID    string
	Name  string
	Steps []processpath.ProcessType
}

// RegisterProcessPath constructs a ProcessPath from cmd and persists it,
// creating the read model on first registration or replacing it wholesale
// on a later call with the same id (ProcessPath has no in-place mutation,
// see its own doc comment).
type RegisterProcessPath struct {
	Repo ports.ProcessPathRepository
}

// Handle validates and persists the ProcessPath described by cmd, and
// returns it.
func (uc *RegisterProcessPath) Handle(ctx context.Context, cmd RegisterProcessPathCommand) (processpath.ProcessPath, error) {
	path, err := processpath.NewProcessPath(cmd.ID, cmd.Name, cmd.Steps)
	if err != nil {
		return processpath.ProcessPath{}, err
	}

	if err := uc.Repo.Save(ctx, path); err != nil {
		return processpath.ProcessPath{}, err
	}

	return path, nil
}
