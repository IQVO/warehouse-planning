package usecases

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// pathOfFlow is the anti-corruption layer of docs/adr/0025 decision 4: a flow
// becomes the ProcessPath the path capacity composes, under the flow's id, its
// steps the stages' process types in order, marked source flow. Since stage 3
// dropped the local declaration, flows are the ONLY source.
func pathOfFlow(f processflow.Flow) (processpath.ProcessPath, error) {
	steps := make([]processpath.ProcessType, 0, len(f.Steps()))
	for _, s := range f.Steps() {
		steps = append(steps, processpath.ProcessType(s))
	}
	return processpath.NewFlowProcessPath(f.ID(), f.Name(), steps)
}

// FlowPathFinder resolves a path id the way docs/adr/0025 says: the ACTIVE
// flow with that id (source flow); a RETIRED or unknown flow does not resolve.
type FlowPathFinder struct {
	Flows ports.ProcessFlowRepository
}

var _ ports.ProcessPathFinder = (*FlowPathFinder)(nil)

// FindByID implements ports.ProcessPathFinder.
func (r *FlowPathFinder) FindByID(ctx context.Context, id string) (*processpath.ProcessPath, error) {
	flow, err := r.Flows.FindActive(ctx, id)
	if err != nil {
		return nil, err
	}
	if flow == nil {
		return nil, nil
	}
	path, err := pathOfFlow(*flow)
	if err != nil {
		return nil, err
	}
	return &path, nil
}

// FlowPathLister lists the ACTIVE flows as the path list, ordered by id, one
// exact keyset page at a time (docs/adr/0025).
type FlowPathLister struct {
	Flows ports.ProcessFlowRepository
}

var _ ports.ProcessPathLister = (*FlowPathLister)(nil)

// ListPage implements ports.ProcessPathLister.
func (l *FlowPathLister) ListPage(ctx context.Context, after *pagecursor.Cursor, limit int) ([]processpath.ProcessPath, error) {
	flows, err := l.Flows.ListActivePage(ctx, after, limit)
	if err != nil {
		return nil, err
	}
	return pathsOfFlows(flows)
}

func pathsOfFlows(flows []processflow.Flow) ([]processpath.ProcessPath, error) {
	out := make([]processpath.ProcessPath, 0, len(flows))
	for _, f := range flows {
		p, err := pathOfFlow(f)
		if err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

// GetProcessPath returns the ProcessPath a path id resolves to (its ACTIVE
// process flow) with its source, for GET /process-paths/{id}. An id no ACTIVE
// flow has is ErrProcessPathNotFound (404).
type GetProcessPath struct {
	Paths ports.ProcessPathFinder
}

// Handle resolves id.
func (uc *GetProcessPath) Handle(ctx context.Context, id string) (processpath.ProcessPath, error) {
	path, err := uc.Paths.FindByID(ctx, id)
	if err != nil {
		return processpath.ProcessPath{}, err
	}
	if path == nil {
		return processpath.ProcessPath{}, ErrProcessPathNotFound
	}
	return *path, nil
}
