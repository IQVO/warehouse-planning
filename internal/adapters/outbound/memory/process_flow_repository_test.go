package memory_test

import (
	"testing"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/repocontract"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

var _ ports.ProcessFlowRepository = (*memory.ProcessFlowRepo)(nil)

// The in-memory double shows the behaviour the Postgres adapter shows
// (postgres/process_flow_integration_test.go runs the same suite).
func TestProcessFlowDoubleSatisfiesTheRepositoryContract(t *testing.T) {
	repocontract.RunProcessFlows(t, func(*testing.T) ports.ProcessFlowRepository { return memory.NewProcessFlowRepo() })
}
