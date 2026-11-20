package usecases

import (
	"context"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// seedFlow records an ACTIVE process flow whose id the path reads resolve
// (ADR 0025 stage 3: flows are the only source); each stage's path id is its
// own process type, the shape the producer's examples use.
func seedFlow(t *testing.T, ctx context.Context, flows *memory.ProcessFlowRepo, id string, at time.Time, steps ...processpath.ProcessType) {
	t.Helper()
	stages := make([]processflow.Stage, 0, len(steps))
	for _, s := range steps {
		stages = append(stages, processflow.Stage{PathID: string(s), ProcessType: processflow.ProcessType(s)})
	}
	f, err := processflow.New(processflow.Params{FlowID: id, Name: "Flow " + id, Stages: stages, Status: processflow.StatusActive, Version: 1, UpdatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := flows.Record(ctx, f); err != nil {
		t.Fatal(err)
	}
}
