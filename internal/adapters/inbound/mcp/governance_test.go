package mcp_test

import (
	"context"
	"errors"
	"regexp"
	"testing"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// failingPlans is a CapacityPlanRepository whose reads fail with an
// infrastructure error carrying a would-be secret.
type failingPlans struct{}

func (failingPlans) Save(context.Context, *capacityplan.CapacityPlan) error {
	return errors.New("boom")
}
func (failingPlans) FindByID(context.Context, string) (*capacityplan.CapacityPlan, error) {
	return nil, errors.New("postgres dsn secret unreachable")
}

func callParams(name string, args map[string]any) *sdk.CallToolParams {
	return &sdk.CallToolParams{Name: name, Arguments: args}
}

var wantTools = map[string]bool{ // name -> read-only
	"register_process_capacity_constraint": false,
	"get_effective_process_capacity":       true,
	"register_process_path":                false,
	"get_process_path_capacity":            true,
	"create_capacity_plan":                 false,
	"publish_capacity_plan":                false,
	"get_capacity_plan":                    true,
	"declare_station_standard":             false,
	"list_station_standards":               true,
	"get_storage_capacity":                 true,
	"get_expected_demand":                  true,
}

// maxTools is the tool budget. It was 8; the station-capacity composition
// (ADR 0002) adds declare_station_standard, list_station_standards and
// get_storage_capacity, so it became 10; demand ingestion (ADR 0004) adds
// get_expected_demand, so it is now 11. Raising it is a deliberate,
// reviewed act: TestToolSurface still pins the exact curated set.
const maxTools = 11

// The MCP governance charter's mechanical gate: the advertised tool set is
// exactly the curated one, within the tool budget, snake_case verb_noun,
// annotated (writes destructive, reads read-only), described, and every
// argument is snake_case and documented.
func TestToolSurface(t *testing.T) {
	h := newHarness(t)
	res, err := h.session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if len(res.Tools) != len(wantTools) || len(res.Tools) > maxTools {
		t.Fatalf("advertised %d tools, want exactly %d (budget %d)", len(res.Tools), len(wantTools), maxTools)
	}
	for _, tool := range res.Tools {
		readOnly, known := wantTools[tool.Name]
		if !known {
			t.Errorf("unexpected tool %q", tool.Name)
			continue
		}
		checkToolMetadata(t, tool, readOnly)
		checkToolArguments(t, tool)
	}
}

var (
	toolNaming = regexp.MustCompile(`^[a-z]+(_[a-z]+)+$`)
	argNaming  = regexp.MustCompile(`^[a-z][a-z0-9]*(_[a-z0-9]+)*$`)
)

func checkToolMetadata(t *testing.T, tool *sdk.Tool, readOnly bool) {
	t.Helper()
	if !toolNaming.MatchString(tool.Name) {
		t.Errorf("tool %q is not snake_case verb_noun", tool.Name)
	}
	if tool.Description == "" {
		t.Errorf("tool %q has no description", tool.Name)
	}
	if tool.Annotations == nil || tool.Annotations.ReadOnlyHint != readOnly {
		t.Errorf("tool %q read-only annotation = %+v, want ReadOnlyHint=%v", tool.Name, tool.Annotations, readOnly)
		return
	}
	if !readOnly && (tool.Annotations.DestructiveHint == nil || !*tool.Annotations.DestructiveHint) {
		t.Errorf("write tool %q is not annotated destructive", tool.Name)
	}
}

func checkToolArguments(t *testing.T, tool *sdk.Tool) {
	t.Helper()
	schema, isMap := tool.InputSchema.(map[string]any)
	if !isMap {
		t.Errorf("tool %q input schema = %T", tool.Name, tool.InputSchema)
		return
	}
	props, _ := schema["properties"].(map[string]any)
	if len(props) == 0 {
		t.Errorf("tool %q has no arguments in its schema", tool.Name)
	}
	for arg, p := range props {
		if !argNaming.MatchString(arg) {
			t.Errorf("tool %q argument %q is not snake_case", tool.Name, arg)
		}
		if d, _ := p.(map[string]any)["description"].(string); d == "" {
			t.Errorf("tool %q argument %q has no description", tool.Name, arg)
		}
	}
}
