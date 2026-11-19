package kafka

import (
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/outbox"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// Fixtures of the three ADR 0023 events, shared with the golden tests
// (encoder_forecast_labour_golden_test.go). Times are given in BRT (UTC-3)
// to prove every instant is emitted in UTC.

const (
	goldenForecastKey = "SIM1/PICK"
	goldenPlanKey     = "SIM1/SHIFT-AM/PICK"
)

func goldenForecastReceived() workloadforecast.Received {
	return workloadforecast.Received{
		SiteID: "SIM1", PathID: "PICK", TotalQuantity: 12000, BucketCount: 3,
		ReceivedAt: goldenAt, Version: 2,
	}
}

// goldenLabourPlanCommitted is a forecast-derived plan with a shift start
// (05:00 BRT == 08:00Z): ceil(12000 / (112.5 x 7.5)) = 15 heads.
func goldenLabourPlanCommitted() labourplan.Committed {
	start := time.Date(2026, 10, 5, 5, 0, 0, 0, time.FixedZone("BRT", -3*3600))
	return labourplan.Committed{
		SiteID: "SIM1", ShiftID: "SHIFT-AM", PathID: "PICK",
		PlannedHeads: 15, RateUnitsPerHour: 112.5, Hours: 7.5, InstalledStations: 16,
		ShiftStart: &start, HeadsSource: labourplan.HeadsFromForecast, Version: 3, At: goldenAt,
	}
}

// goldenLabourPlanCommittedWithoutShiftStart is a plan whose caller sent the
// heads and no shift start: shift_start is absent from the payload.
func goldenLabourPlanCommittedWithoutShiftStart() labourplan.Committed {
	return labourplan.Committed{
		SiteID: "SIM1", ShiftID: "SHIFT-AM", PathID: "PICK",
		PlannedHeads: 12, RateUnitsPerHour: 112.5, Hours: 7.5, InstalledStations: 16,
		HeadsSource: labourplan.HeadsFromRequest, Version: 1, At: goldenAt,
	}
}

// goldenLabourPlanDrift: workforce committed 12 heads against a plan of 15.
func goldenLabourPlanDrift() labourplan.DriftDetected {
	return labourplan.CompareHeads(15, 12).Event("SIM1", "SHIFT-AM", "PICK", goldenAt)
}

// requirementEncoder is the three ADR 0023 methods behind one call shape.
type requirementEncoder interface {
	EncodeWorkloadForecastEvents(events ...workloadforecast.Received) ([]outbox.Message, error)
	EncodeLabourPlanCommitted(events ...labourplan.Committed) ([]outbox.Message, error)
	EncodeLabourPlanDriftDetected(events ...labourplan.DriftDetected) ([]outbox.Message, error)
}

// requirementCase encodes one fixture through any requirementEncoder.
type requirementCase struct {
	name   string // the event name
	key    string // Kafka key and CloudEvents subject
	entity string
	encode func(requirementEncoder) ([]outbox.Message, error)
}

func requirementCases() []requirementCase {
	return []requirementCase{
		{"WorkloadForecastReceived", goldenForecastKey, "workloadforecast", func(e requirementEncoder) ([]outbox.Message, error) {
			return e.EncodeWorkloadForecastEvents(goldenForecastReceived())
		}},
		{"LabourPlanCommitted", goldenPlanKey, "labourplan", func(e requirementEncoder) ([]outbox.Message, error) {
			return e.EncodeLabourPlanCommitted(goldenLabourPlanCommitted())
		}},
		{"LabourPlanDriftDetected", goldenPlanKey, "labourplan", func(e requirementEncoder) ([]outbox.Message, error) {
			return e.EncodeLabourPlanDriftDetected(goldenLabourPlanDrift())
		}},
	}
}

func goldenFanout() *FanoutEncoder {
	f := NewFanoutEncoder()
	f.NewID = func() string { return goldenEvtID }
	return f
}

// Every ADR 0023 occurrence becomes TWO messages under ONE id and ONE type:
// the integration message first, then its analytics copy, each with its own
// topic and dataschema stream, the same key/subject and the same payload.
func TestFanoutEncoder_RequirementEventsGoToBothTopicsUnderOneID(t *testing.T) {
	for _, tc := range requirementCases() {
		t.Run(tc.name, func(t *testing.T) {
			msgs, err := tc.encode(goldenFanout())
			if err != nil || len(msgs) != 2 {
				t.Fatalf("Encode = %d msgs, %v; want integration + analytics", len(msgs), err)
			}
			for i, w := range []struct{ topic, stream string }{{Topic, "events"}, {AnalyticsTopic, "analytics"}} {
				assertRequirementMessage(t, tc, msgs[i], w.topic, w.stream)
			}
			if dataOf(t, msgs[0]) != dataOf(t, msgs[1]) {
				t.Errorf("analytics payload differs from the integration one:\n%s\nvs\n%s", dataOf(t, msgs[1]), dataOf(t, msgs[0]))
			}
		})
	}
}

// assertRequirementMessage pins one ADR 0023 message's attributes: topic,
// golden id, full type, the stream's dataschema, key/subject, header.
func assertRequirementMessage(t *testing.T, tc requirementCase, m outbox.Message, topic, stream string) {
	t.Helper()
	if m.Topic != topic || m.EventID != goldenEvtID {
		t.Errorf("%s topic/id = %q/%q, want %q/%q", stream, m.Topic, m.EventID, topic, goldenEvtID)
	}
	if want := "com.warehouse.wes.warehouse-planning." + tc.entity + "." + tc.name; m.EventType != want {
		t.Errorf("%s type = %q, want %q", stream, m.EventType, want)
	}
	if want := "urn:warehouse:warehouse-planning:" + stream + ":" + tc.name + ":v1"; m.DataSchema != want {
		t.Errorf("%s dataschema = %q, want %q", stream, m.DataSchema, want)
	}
	if m.Subject != tc.key || string(m.Key) != tc.key {
		t.Errorf("%s subject/key = %q/%q, want %q", stream, m.Subject, m.Key, tc.key)
	}
	assertGoldenHeaderAndDecode(t, m)
}

// The fan-out's two halves are byte-identical to what the integration-only
// Encoder and the analytics-only AnalyticsEncoder produce for the same id.
func TestFanoutEncoder_RequirementHalvesMatchTheSingleStreamEncoders(t *testing.T) {
	for _, tc := range requirementCases() {
		fan, err := tc.encode(goldenFanout())
		if err != nil || len(fan) != 2 {
			t.Fatalf("%s: Fanout = %d, %v", tc.name, len(fan), err)
		}
		plain, err := tc.encode(goldenEncoder())
		if err != nil || len(plain) != 1 || !equalMessages(fan[0], plain[0]) {
			t.Errorf("%s: integration half differs from Encoder's (%v)", tc.name, err)
		}
		analytics, err := tc.encode(goldenAnalyticsEncoder())
		if err != nil || len(analytics) != 1 || !equalMessages(fan[1], analytics[0]) {
			t.Errorf("%s: analytics half differs from AnalyticsEncoder's (%v)", tc.name, err)
		}
	}
}

func TestFanoutEncoder_RequirementEventsMintOneIDPerOccurrence(t *testing.T) {
	msgs, err := NewFanoutEncoder().EncodeLabourPlanCommitted(goldenLabourPlanCommitted(), goldenLabourPlanCommittedWithoutShiftStart())
	if err != nil || len(msgs) != 4 {
		t.Fatalf("Encode = %d, %v", len(msgs), err)
	}
	if msgs[0].EventID != msgs[1].EventID || msgs[2].EventID != msgs[3].EventID || msgs[0].EventID == msgs[2].EventID {
		t.Errorf("ids = %q %q %q %q: want one id per occurrence, shared by its two messages", msgs[0].EventID, msgs[1].EventID, msgs[2].EventID, msgs[3].EventID)
	}
	if len(msgs[0].EventID) != 36 {
		t.Errorf("id %q is not a UUID", msgs[0].EventID)
	}
	if msgs[0].Topic != Topic || msgs[1].Topic != AnalyticsTopic || msgs[2].Topic != Topic || msgs[3].Topic != AnalyticsTopic {
		t.Errorf("topics = %q %q %q %q: want integration then analytics per occurrence", msgs[0].Topic, msgs[1].Topic, msgs[2].Topic, msgs[3].Topic)
	}
}

func TestRequirementEncoders_NoEventsNoMessages(t *testing.T) {
	for _, enc := range []requirementEncoder{NewEncoder(), &AnalyticsEncoder{}, NewFanoutEncoder()} {
		a, errA := enc.EncodeWorkloadForecastEvents()
		b, errB := enc.EncodeLabourPlanCommitted()
		c, errC := enc.EncodeLabourPlanDriftDetected()
		if len(a)+len(b)+len(c) != 0 || errA != nil || errB != nil || errC != nil {
			t.Errorf("%T: no events = %d/%d/%d msgs, %v %v %v", enc, len(a), len(b), len(c), errA, errB, errC)
		}
	}
}

func TestRequirementEncoders_TopicOverrideAndRandomIDs(t *testing.T) {
	ev := goldenLabourPlanDrift()
	in, err := (&Encoder{Topic: "unique.events"}).EncodeLabourPlanDriftDetected(ev, ev)
	if err != nil || len(in) != 2 || in[0].Topic != "unique.events" || in[0].EventID == in[1].EventID || len(in[0].EventID) != 36 {
		t.Fatalf("Encoder override = %+v, %v", in, err)
	}
	an, err := (&AnalyticsEncoder{Topic: "unique.analytics"}).EncodeWorkloadForecastEvents(goldenForecastReceived(), goldenForecastReceived())
	if err != nil || len(an) != 2 || an[1].Topic != "unique.analytics" || an[0].EventID == an[1].EventID || len(an[0].EventID) != 36 {
		t.Fatalf("AnalyticsEncoder override = %+v, %v", an, err)
	}
}

// dataOf returns the `data` member of a message's CloudEvents JSON.
func dataOf(t *testing.T, m outbox.Message) string {
	t.Helper()
	v := string(m.Value)
	i := strings.Index(v, `"data":`)
	if i < 0 {
		t.Fatalf("no data member in %s", v)
	}
	return v[i:]
}
