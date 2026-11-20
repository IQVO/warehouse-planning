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
// steps the stages' process types in order, marked source flow.
func pathOfFlow(f processflow.Flow) (processpath.ProcessPath, error) {
	steps := make([]processpath.ProcessType, 0, len(f.Steps()))
	for _, s := range f.Steps() {
		steps = append(steps, processpath.ProcessType(s))
	}
	return processpath.NewFlowProcessPath(f.ID(), f.Name(), steps)
}

// FlowFirstPathFinder resolves a path id the way docs/adr/0025 decision 2 and 3
// say: an ACTIVE flow with that id first (source flow); a RETIRED or unknown
// flow does not resolve, so the locally declared path of that id (source local)
// is used. A nil Flows is the pre-ADR-0025 behaviour (local only).
type FlowFirstPathFinder struct {
	Flows ports.ProcessFlowRepository
	Local ports.ProcessPathFinder
}

var _ ports.ProcessPathFinder = (*FlowFirstPathFinder)(nil)

// FindByID implements ports.ProcessPathFinder.
func (r *FlowFirstPathFinder) FindByID(ctx context.Context, id string) (*processpath.ProcessPath, error) {
	if r.Flows != nil {
		flow, err := r.Flows.FindActive(ctx, id)
		if err != nil {
			return nil, err
		}
		if flow != nil {
			path, err := pathOfFlow(*flow)
			if err != nil {
				return nil, err
			}
			return &path, nil
		}
	}
	return r.Local.FindByID(ctx, id)
}

// FlowFirstPathLister lists the locally declared paths and the ACTIVE flows as
// one list ordered by id (docs/adr/0025 decision 3): a flow shows up even when
// no local path exists, and wins over a local path of the same id. Keyset
// paging is exact: neither source is read past the id up to which both are
// known, so a page is never short while more follows. A nil Flows lists the
// local paths only.
type FlowFirstPathLister struct {
	Flows ports.ProcessFlowRepository
	Local ports.ProcessPathLister
}

var _ ports.ProcessPathLister = (*FlowFirstPathLister)(nil)

// ListPage implements ports.ProcessPathLister.
func (l *FlowFirstPathLister) ListPage(ctx context.Context, after *pagecursor.Cursor, limit int) ([]processpath.ProcessPath, error) {
	if l.Flows == nil {
		return l.Local.ListPage(ctx, after, limit)
	}
	out := make([]processpath.ProcessPath, 0, limit)
	cursor := after
	for len(out) < limit {
		local, err := l.Local.ListPage(ctx, cursor, limit)
		if err != nil {
			return nil, err
		}
		flows, err := l.Flows.ListActivePage(ctx, cursor, limit)
		if err != nil {
			return nil, err
		}
		fromFlows, err := pathsOfFlows(flows)
		if err != nil {
			return nil, err
		}
		// Both sources are complete up to bound (when a source filled its
		// page, nothing past its last id is known).
		bound, bounded := knownUpTo(local, fromFlows, limit)
		for _, p := range mergeFlowWins(local, fromFlows) {
			if bounded && p.ID() > bound {
				break
			}
			out = append(out, p)
		}
		if !bounded {
			break
		}
		cursor = &pagecursor.Cursor{Sort: bound, ID: bound}
	}
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
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

// knownUpTo returns the greatest id up to which both pages are complete, and
// false when neither page is full (so both are complete without limit).
func knownUpTo(local, flows []processpath.ProcessPath, limit int) (string, bool) {
	bound, bounded := "", false
	for _, page := range [][]processpath.ProcessPath{local, flows} {
		if len(page) < limit || len(page) == 0 {
			continue
		}
		if last := page[len(page)-1].ID(); !bounded || last < bound {
			bound, bounded = last, true
		}
	}
	return bound, bounded
}

// mergeFlowWins merges two id-ordered pages; for an id in both, the flow's path.
func mergeFlowWins(local, flows []processpath.ProcessPath) []processpath.ProcessPath {
	out := make([]processpath.ProcessPath, 0, len(local)+len(flows))
	i, j := 0, 0
	for i < len(local) && j < len(flows) {
		switch {
		case local[i].ID() < flows[j].ID():
			out = append(out, local[i])
			i++
		case local[i].ID() > flows[j].ID():
			out = append(out, flows[j])
			j++
		default:
			out = append(out, flows[j])
			i++
			j++
		}
	}
	out = append(out, local[i:]...)
	return append(out, flows[j:]...)
}

// GetProcessPath returns the ProcessPath a path id resolves to (flow first, then
// the locally declared one) with its source, for GET /process-paths/{id}. A
// flow with no local path is a 200; an id neither source knows is
// ErrProcessPathNotFound (404).
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
