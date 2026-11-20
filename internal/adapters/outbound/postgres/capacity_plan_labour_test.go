package postgres

import (
	"reflect"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
)

// The JSONB storage format of capacity_plans.labour_requirement round-trips
// every field; the database-backed path is covered by the integration test.
func TestLabourRequirementJSONRoundTrip(t *testing.T) {
	items := []capacityplan.LabourRequirement{
		{ProcessType: "PICK", StandardSeconds: 12, StandardSiteScoped: true, Tasks: 2400, RequiredLabourHours: 8, RequiredAverageHeads: 1, Known: true},
		{ProcessType: "PACK", StandardSeconds: 90, Reason: capacityplan.ReasonNoTaskFactor},
	}
	raw, err := encodeLabourRequirement(items)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeLabourRequirement(raw)
	if err != nil || !reflect.DeepEqual(got, items) {
		t.Fatalf("round trip = %+v, %v; want %+v", got, err, items)
	}
}

func TestLabourRequirementJSON_EmptyIsAnEmptyArrayAndReadsBackAsNone(t *testing.T) {
	raw, err := encodeLabourRequirement(nil)
	if err != nil || string(raw) != "[]" {
		t.Fatalf("encode(nil) = %q, %v; want [] (the column is NOT NULL)", raw, err)
	}
	for _, stored := range []string{"", "[]"} {
		got, err := decodeLabourRequirement([]byte(stored))
		if err != nil || got != nil {
			t.Fatalf("decode(%q) = %+v, %v; want none", stored, got, err)
		}
	}
}

func TestLabourRequirementJSON_CorruptColumnIsAnError(t *testing.T) {
	if _, err := decodeLabourRequirement([]byte(`{"not":"a list"}`)); err == nil {
		t.Fatal("decode of a non-array succeeded")
	}
}
