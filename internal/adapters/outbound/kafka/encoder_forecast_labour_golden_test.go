package kafka

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
)

// The golden files under testdata/golden pin the EXACT bytes of the three
// ADR 0023 types on BOTH streams (one file per type per stream, plus a
// LabourPlanCommitted without a shift start): every CloudEvents attribute
// and the snake_case `data`. workforce-management consumes
// LabourPlanCommitted against this wire, so any change here is a contract
// change (breaking => a new .v2 type and dataschema). The files end in one
// newline, which is not part of the message value.

type goldenFile struct {
	file   string
	stream string // events | analytics
	encode func(requirementEncoder) ([]outbox.Message, error)
}

func requirementGoldenFiles() []goldenFile {
	var files []goldenFile
	for _, tc := range requirementCases() {
		files = append(files,
			goldenFile{tc.name + ".events.json", "events", tc.encode},
			goldenFile{tc.name + ".analytics.json", "analytics", tc.encode})
	}
	withoutStart := func(e requirementEncoder) ([]outbox.Message, error) {
		return e.EncodeLabourPlanCommitted(goldenLabourPlanCommittedWithoutShiftStart())
	}
	return append(files, goldenFile{"LabourPlanCommitted-without-shift-start.events.json", "events", withoutStart})
}

func readGolden(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "golden", name))
	if err != nil {
		t.Fatalf("read golden %s: %v", name, err)
	}
	return bytes.TrimSuffix(raw, []byte("\n"))
}

func TestGolden_RequirementEventsOnBothStreams(t *testing.T) {
	for _, g := range requirementGoldenFiles() {
		t.Run(g.file, func(t *testing.T) {
			var enc requirementEncoder = goldenEncoder()
			if g.stream == "analytics" {
				enc = goldenAnalyticsEncoder()
			}
			msgs, err := g.encode(enc)
			if err != nil || len(msgs) != 1 {
				t.Fatalf("Encode = %d msgs, %v", len(msgs), err)
			}
			if want := readGolden(t, g.file); !bytes.Equal(msgs[0].Value, want) {
				t.Errorf("value =\n%s\nwant (testdata/golden/%s)\n%s", msgs[0].Value, g.file, want)
			}
			assertGoldenHeaderAndDecode(t, msgs[0])
		})
	}
}

// shift_start is OMITTED (not null) when the plan has none, and present as
// an RFC 3339 UTC instant when it has one.
func TestGolden_LabourPlanCommittedShiftStartIsAbsentWhenThePlanHasNone(t *testing.T) {
	for file, wantStart := range map[string]any{
		"LabourPlanCommitted.events.json":                     "2026-10-05T08:00:00Z",
		"LabourPlanCommitted.analytics.json":                  "2026-10-05T08:00:00Z",
		"LabourPlanCommitted-without-shift-start.events.json": nil,
	} {
		var ev struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(readGolden(t, file), &ev); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		got, present := ev.Data["shift_start"]
		if wantStart == nil && present {
			t.Errorf("%s: shift_start = %v, want the key absent", file, got)
		}
		if wantStart != nil && got != wantStart {
			t.Errorf("%s: shift_start = %v, want %v", file, got, wantStart)
		}
	}
}

// The fan-out writes exactly the two golden files of each type, in order.
func TestGolden_FanoutWritesTheTwoGoldenMessages(t *testing.T) {
	for _, tc := range requirementCases() {
		msgs, err := tc.encode(goldenFanout())
		if err != nil || len(msgs) != 2 {
			t.Fatalf("%s: Fanout = %d, %v", tc.name, len(msgs), err)
		}
		if !bytes.Equal(msgs[0].Value, readGolden(t, tc.name+".events.json")) ||
			!bytes.Equal(msgs[1].Value, readGolden(t, tc.name+".analytics.json")) {
			t.Errorf("%s: fan-out bytes differ from the golden files", tc.name)
		}
	}
}
