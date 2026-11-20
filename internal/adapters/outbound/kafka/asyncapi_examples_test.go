package kafka

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// The AsyncAPI examples of the three ADR 0023 messages are what consumers'
// contract tests read (workforce-management builds its LabourPlanCommitted
// consumer on them), so each must be the golden wire minus the id: every
// CloudEvents attribute but `id`, and the whole `data`, equal the encoder's
// bytes in testdata/golden.

func TestAsyncAPIExamplesAreTheGoldenWire(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "apis", "asyncapi.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Components struct {
			Messages map[string]struct {
				Examples []struct {
					Name    string         `yaml:"name"`
					Payload map[string]any `yaml:"payload"`
				} `yaml:"examples"`
			} `yaml:"messages"`
		} `yaml:"components"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		message string
		example int
		golden  string
	}{
		{"WorkloadForecastReceived", 0, "WorkloadForecastReceived.events.json"},
		{"LabourPlanCommitted", 0, "LabourPlanCommitted.events.json"},
		{"LabourPlanCommitted", 1, "LabourPlanCommitted-without-shift-start.events.json"},
		{"LabourPlanDriftDetected", 0, "LabourPlanDriftDetected.events.json"},
		{"WorkloadForecastReceivedAnalytics", 0, "WorkloadForecastReceived.analytics.json"},
		{"LabourPlanCommittedAnalytics", 0, "LabourPlanCommitted.analytics.json"},
		{"LabourPlanDriftDetectedAnalytics", 0, "LabourPlanDriftDetected.analytics.json"},
		// docs/adr/0024 decision 3: order-management builds its consumer on it.
		{"CapacityPlanSuperseded", 0, "CapacityPlanSuperseded.events.json"},
		{"CapacityPlanSupersededAnalytics", 0, "CapacityPlanSuperseded.analytics.json"},
	}
	for _, tc := range cases {
		examples := doc.Components.Messages[tc.message].Examples
		if len(examples) <= tc.example {
			t.Errorf("%s: no example %d", tc.message, tc.example)
			continue
		}
		got, want := asJSONMap(t, examples[tc.example].Payload), asJSONMap(t, nil)
		if err := json.Unmarshal(readGolden(t, tc.golden), &want); err != nil {
			t.Fatal(err)
		}
		delete(got, "id")
		delete(want, "id")
		if g, w := mustJSON(t, got), mustJSON(t, want); g != w {
			t.Errorf("%s example %q differs from testdata/golden/%s (id aside):\n%s\nwant\n%s", tc.message, examples[tc.example].Name, tc.golden, g, w)
		}
	}
}

// asJSONMap normalises a YAML-decoded value through JSON (ints and floats
// become JSON numbers), so it compares equal to a decoded golden file.
func asJSONMap(t *testing.T, v map[string]any) map[string]any {
	t.Helper()
	out := map[string]any{}
	if v == nil {
		return out
	}
	if err := json.Unmarshal([]byte(mustJSON(t, v)), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
