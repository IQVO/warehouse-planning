package mcp_test

import (
	"context"
	"reflect"
	"strconv"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// seedStations tallies n work-center slots for activity in zone, the way the
// facility consumer would.
func (h *harness) seedStations(t *testing.T, zone, activity string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		code := zone + "-" + activity + "-" + strconv.Itoa(i)
		if _, err := h.tally.RegisterSlot(context.Background(), code, zone, tally.TypeStation, []string{activity}); err != nil {
			t.Fatal(err)
		}
	}
}

func (h *harness) seedPositions(t *testing.T, zone, locationType string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		code := zone + "-" + locationType + "-" + strconv.Itoa(i)
		if _, err := h.tally.RegisterSlot(context.Background(), code, zone, tally.TypeLocation, []string{locationType}); err != nil {
			t.Fatal(err)
		}
	}
}

func standardArgs(location, process string, quantity float64, unit string) map[string]any {
	return map[string]any{"location": location, "process_type": process, "quantity": quantity, "unit": unit, "period_seconds": 3600}
}

func TestDeclareAndListStationStandards(t *testing.T) {
	h := newHarness(t)

	got := h.ok(t, "declare_station_standard", standardArgs("SIM1", "PACK", 180, "PACKAGE"))
	want := map[string]any{"location": "SIM1", "process_type": "PACK", "quantity": 180.0, "unit": "PACKAGE", "period_seconds": 3600.0, "created": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("declare = %v, want %v", got, want)
	}
	// Replacing the same key reports created=false and replaces the throughput.
	got = h.ok(t, "declare_station_standard", standardArgs("SIM1", "PACK", 200, "PACKAGE"))
	if got["created"] != false || got["quantity"] != 200.0 {
		t.Fatalf("redeclare = %v, want created=false quantity=200", got)
	}
	h.ok(t, "declare_station_standard", standardArgs("SIM1", "SORT", 90, "UNIT"))
	h.ok(t, "declare_station_standard", standardArgs("SIM2", "PACK", 75, "ORDER"))

	list := h.ok(t, "list_station_standards", map[string]any{"location": "SIM1"})
	standards, _ := list["standards"].([]any)
	if list["location"] != "SIM1" || len(standards) != 2 {
		t.Fatalf("list SIM1 = %v, want 2 standards", list)
	}
	if first := standards[0].(map[string]any); first["process_type"] != "PACK" || first["quantity"] != 200.0 {
		t.Fatalf("first SIM1 standard = %v, want PACK 200 (ordered by process type)", first)
	}
	all := h.ok(t, "list_station_standards", map[string]any{})
	if n := len(all["standards"].([]any)); n != 3 {
		t.Fatalf("list all = %d standards, want 3", n)
	}
	empty := h.ok(t, "list_station_standards", map[string]any{"location": "NOWHERE"})
	if s, ok := empty["standards"].([]any); !ok || len(s) != 0 {
		t.Fatalf("empty list = %v, want standards: []", empty)
	}
}

func TestDeclareStationStandardRejections(t *testing.T) {
	h := newHarness(t)
	h.fail(t, "declare_station_standard", standardArgs("SIM1", "PACK", 0, "PACKAGE"), "non-positive-station-standard")
	h.fail(t, "declare_station_standard", standardArgs("SIM1", "PACK", -5, "PACKAGE"), "negative-quantity")
	h.fail(t, "declare_station_standard", standardArgs("SIM1", "PACK", 180, "LINE"), "unsupported-normalization-unit")
	h.fail(t, "declare_station_standard", standardArgs("", "PACK", 180, "PACKAGE"), "missing-station-standard-field")
	bad := standardArgs("SIM1", "PACK", 180, "PACKAGE")
	bad["period_seconds"] = 0
	h.fail(t, "declare_station_standard", bad, "non-positive-period")
}

func TestGetStorageCapacity(t *testing.T) {
	h := newHarness(t)
	h.fail(t, "get_storage_capacity", map[string]any{"location": ""}, "missing-location")

	empty := h.ok(t, "get_storage_capacity", map[string]any{"location": "SIM1"})
	if !reflect.DeepEqual(empty["storage_positions"], []any{}) || !reflect.DeepEqual(empty["stations"], []any{}) {
		t.Fatalf("empty site = %v, want empty lists", empty)
	}

	h.seedStations(t, "SIM1-OPS-WC", "PACK", 11)
	h.seedPositions(t, "SIM1-STOR-AMB", "SimShelf", 24)
	h.seedStations(t, "SIM2-OPS-WC", "PACK", 3) // another site
	got := h.ok(t, "get_storage_capacity", map[string]any{"location": "SIM1"})
	wantStations := []any{map[string]any{"zone_id": "SIM1-OPS-WC", "activity": "PACK", "stations": 11.0}}
	wantPositions := []any{map[string]any{"zone_id": "SIM1-STOR-AMB", "location_type": "SimShelf", "positions": 24.0}}
	if got["location"] != "SIM1" || !reflect.DeepEqual(got["stations"], wantStations) || !reflect.DeepEqual(got["storage_positions"], wantPositions) {
		t.Fatalf("storage capacity = %v", got)
	}
}

// Fixtures A and B through MCP: 10 PACK stations, 180 PACKAGE/h each, LABOR
// 2500 PACKAGE/h -> PACK 1800; with Rebin raised to 6000 UNIT/h the path is
// 1800 ORDER/h, bottleneck PACK bound by STATION, in the path capacity AND in
// the stored plan (shortage 20000 - 1800*8 = 5600).
func TestStationCompositionThroughMCPTools(t *testing.T) {
	h := newHarness(t)
	h.ok(t, "register_process_capacity_constraint", constraintArgs("PICK", "SIM1", 8000, "UNIT"))
	h.ok(t, "register_process_capacity_constraint", constraintArgs("REBIN", "SIM1", 2500, "UNIT"))
	h.ok(t, "register_process_capacity_constraint", constraintArgs("PACK", "SIM1", 2500, "PACKAGE"))
	h.ok(t, "register_process_path", map[string]any{"id": "tote-path", "name": "Tote path", "steps": []string{"PICK", "REBIN", "PACK"}})
	h.seedStations(t, "SIM1-OPS-WC", "PACK", 10)
	pathArgs := map[string]any{
		"id": "tote-path", "location": "SIM1", "window_start": winStart, "window_end": winEnd,
		"units_per_order": 2.5, "packages_per_order": 1,
	}

	// Fixture C first: stations tallied, no standard -> labor only + warning.
	c := h.ok(t, "get_process_path_capacity", pathArgs)
	if c["bottleneck_step"] != "REBIN" || c["normalized_rate"] != 1000.0 {
		t.Fatalf("no standard: %v, want REBIN 1000", c)
	}
	warnings, _ := c["warnings"].([]any)
	if len(warnings) != 1 || warnings[0] == "" {
		t.Fatalf("no standard: warnings = %v, want exactly one", c["warnings"])
	}

	h.ok(t, "declare_station_standard", standardArgs("SIM1", "PACK", 180, "PACKAGE"))
	b := h.ok(t, "get_process_path_capacity", pathArgs)
	assertBreakdown(t, b, []stepWant{{"PICK", 3200, "LABOR"}, {"REBIN", 1000, "LABOR"}, {"PACK", 1800, "STATION"}})
	if b["bottleneck_step"] != "REBIN" || b["normalized_rate"] != 1000.0 || !reflect.DeepEqual(b["warnings"], []any{}) {
		t.Fatalf("fixture B step 1: %v", b)
	}

	h.ok(t, "register_process_capacity_constraint", constraintArgs("REBIN", "SIM1", 6000, "UNIT"))
	b = h.ok(t, "get_process_path_capacity", pathArgs)
	assertBreakdown(t, b, []stepWant{{"PICK", 3200, "LABOR"}, {"REBIN", 2400, "LABOR"}, {"PACK", 1800, "STATION"}})
	if b["bottleneck_step"] != "PACK" || b["normalized_rate"] != 1800.0 || b["normalized_unit"] != "ORDER" {
		t.Fatalf("fixture B step 2: %v", b)
	}

	plan := h.ok(t, "create_capacity_plan", map[string]any{
		"warehouse_id": "WH-1", "location": "SIM1", "window_start": winStart, "window_end": winEnd,
		"path_id": "tote-path", "assigned_demand": 20000, "units_per_order": 2.5, "packages_per_order": 1,
	})
	if plan["path_capacity"] != 1800.0 || plan["bottleneck_step"] != "PACK" || plan["bottleneck_constraint"] != "STATION" ||
		plan["capacity_over_window"] != 14400.0 || plan["shortage"] != 5600.0 || !reflect.DeepEqual(plan["warnings"], []any{}) {
		t.Fatalf("plan = %v", plan)
	}
	// get_capacity_plan surfaces the stored composition outcome.
	got := h.ok(t, "get_capacity_plan", map[string]any{"id": plan["id"]})
	if got["bottleneck_constraint"] != "STATION" || got["bottleneck_step"] != "PACK" {
		t.Fatalf("get_capacity_plan = %v", got)
	}
}

type stepWant struct {
	step    string
	rate    float64
	binding string
}

func assertBreakdown(t *testing.T, got map[string]any, want []stepWant) {
	t.Helper()
	items, _ := got["step_breakdown"].([]any)
	if len(items) != len(want) {
		t.Fatalf("step_breakdown = %v, want %d items", got["step_breakdown"], len(want))
	}
	for i, w := range want {
		item := items[i].(map[string]any)
		if item["step"] != w.step || item["normalized_rate"] != w.rate || item["binding_constraint"] != w.binding {
			t.Errorf("step %d = %v, want %+v", i, item, w)
		}
	}
}
