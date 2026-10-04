package report

import (
	"errors"
	"math"
	"reflect"
	"testing"
	"time"
)

var now = time.Date(2026, 10, 4, 12, 0, 0, 0, time.UTC)

func TestParseRange_DefaultsToTheThirtyDaysEndingNow(t *testing.T) {
	got, err := ParseRange("", "", now)
	if err != nil {
		t.Fatal(err)
	}
	want := Range{From: time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC), To: now}
	if got != want {
		t.Fatalf("range = %v, want %v", got, want)
	}
}

func TestParseRange_OnlyToDefaultsFromThirtyDaysBefore(t *testing.T) {
	got, err := ParseRange("", "2026-08-31T00:00:00Z", now)
	if err != nil {
		t.Fatal(err)
	}
	want := Range{From: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 8, 31, 0, 0, 0, 0, time.UTC)}
	if got != want {
		t.Fatalf("range = %v, want %v", got, want)
	}
}

func TestParseRange_OnlyFromEndsNow(t *testing.T) {
	got, err := ParseRange("2026-10-01T06:30:00Z", "", now)
	if err != nil {
		t.Fatal(err)
	}
	if want := (Range{From: time.Date(2026, 10, 1, 6, 30, 0, 0, time.UTC), To: now}); got != want {
		t.Fatalf("range = %v, want %v", got, want)
	}
}

func TestParseRange_ConvertsOffsetsToUTC(t *testing.T) {
	got, err := ParseRange("2026-10-01T03:00:00-03:00", "2026-10-02T03:00:00+03:00", now)
	if err != nil {
		t.Fatal(err)
	}
	want := Range{From: time.Date(2026, 10, 1, 6, 0, 0, 0, time.UTC), To: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)}
	if got != want || got.From.Location() != time.UTC || got.To.Location() != time.UTC {
		t.Fatalf("range = %v, want %v in UTC", got, want)
	}
}

func TestParseRange_Rejections(t *testing.T) {
	cases := []struct {
		name, from, to string
		want           error
	}{
		{"garbage from", "yesterday", "", ErrInvalidFrom},
		{"garbage to", "", "2026-13-01", ErrInvalidTo},
		{"date only is not RFC 3339", "2026-10-01", "", ErrInvalidFrom},
		{"from equals to", "2026-10-01T00:00:00Z", "2026-10-01T00:00:00Z", ErrEmptyRange},
		{"from after to", "2026-10-02T00:00:00Z", "2026-10-01T00:00:00Z", ErrEmptyRange},
		{"only from in the future", "2026-10-05T00:00:00Z", "", ErrEmptyRange},
		{"one second over 366 days", "2025-09-30T00:00:00Z", "2026-10-01T00:00:01Z", ErrRangeTooLarge},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseRange(tc.from, tc.to, now); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestParseRange_ExactlyMaxWindowIsAllowedAndOneSecondLessToo(t *testing.T) {
	if _, err := ParseRange("2025-09-30T00:00:00Z", "2026-10-01T00:00:00Z", now); err != nil {
		t.Fatalf("exactly 366 days rejected: %v", err)
	}
	if _, err := ParseRange("2025-09-30T00:00:01Z", "2026-10-01T00:00:00Z", now); err != nil {
		t.Fatalf("366 days minus 1s rejected: %v", err)
	}
	if _, err := ParseRange("2026-10-01T00:00:00Z", "2026-10-01T00:00:01Z", now); err != nil {
		t.Fatalf("one second range rejected: %v", err)
	}
}

func TestBottleneckFrequencies_ShareIsPerSiteNotGlobal(t *testing.T) {
	a := Site{WarehouseID: "WH-1", Location: "SIM1"}
	b := Site{WarehouseID: "WH-2", Location: "SIM2"}
	got := BottleneckFrequencies([]BottleneckCount{
		{Site: a, BottleneckStep: "PACK", BindingConstraint: "STATION", Plans: 3},
		{Site: a, BottleneckStep: "REBIN", BindingConstraint: "LABOR", Plans: 1},
		{Site: b, BottleneckStep: "PICK", BindingConstraint: "LABOR", Plans: 7},
	})
	wantShares := []float64{0.75, 0.25, 1}
	if len(got) != 3 {
		t.Fatalf("rows = %d", len(got))
	}
	for i, w := range wantShares {
		if got[i].Share != w {
			t.Errorf("row %d share = %v, want %v", i, got[i].Share, w)
		}
	}
	if got[2].Plans != 7 || got[2].BottleneckStep != "PICK" || got[0].BindingConstraint != "STATION" {
		t.Errorf("rows were reordered or lost fields: %+v", got)
	}
}

func TestBottleneckFrequencies_SameWarehouseDifferentLocationAreDifferentSites(t *testing.T) {
	got := BottleneckFrequencies([]BottleneckCount{
		{Site: Site{"WH-1", "SIM1"}, BottleneckStep: "PACK", Plans: 1},
		{Site: Site{"WH-1", "SIM2"}, BottleneckStep: "PACK", Plans: 4},
	})
	if got[0].Share != 1 || got[1].Share != 1 {
		t.Fatalf("shares = %v, %v; a location is a site of its own", got[0].Share, got[1].Share)
	}
}

func TestEmptyInputsGiveEmptyNonNilSlices(t *testing.T) {
	if got := BottleneckFrequencies(nil); got == nil || len(got) != 0 {
		t.Errorf("BottleneckFrequencies(nil) = %#v", got)
	}
	if got := ShortageTrend(nil); got == nil || len(got) != 0 {
		t.Errorf("ShortageTrend(nil) = %#v", got)
	}
}

func TestShortageTrend_RateCountsPlansWithoutShortageToo(t *testing.T) {
	day := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	got := ShortageTrend([]ShortageDay{
		{Site: Site{"WH-1", "SIM1"}, Day: day, PlansPublished: 8, PlansWithShortage: 2, TotalShortage: 5600},
		{Site: Site{"WH-1", "SIM1"}, Day: day.AddDate(0, 0, 1), PlansPublished: 3, PlansWithShortage: 3, TotalShortage: 90},
		{Site: Site{"WH-1", "SIM1"}, Day: day.AddDate(0, 0, 2), PlansPublished: 5, PlansWithShortage: 0},
	})
	want := []float64{0.25, 1, 0}
	for i, w := range want {
		if got[i].ShortageRate != w {
			t.Errorf("day %d rate = %v, want %v", i, got[i].ShortageRate, w)
		}
	}
	if got[0].TotalShortage != 5600 || got[0].PlansPublished != 8 || !got[1].Day.Equal(day.AddDate(0, 0, 1)) {
		t.Errorf("fields lost: %+v", got)
	}
}

func TestRate(t *testing.T) {
	cases := []struct {
		num, den int
		want     float64
	}{{1, 4, 0.25}, {4, 4, 1}, {0, 4, 0}, {5, 0, 0}, {5, -1, 0}, {1, 3, 1.0 / 3}}
	for _, tc := range cases {
		if got := Rate(tc.num, tc.den); got != tc.want {
			t.Errorf("Rate(%d,%d) = %v, want %v", tc.num, tc.den, got, tc.want)
		}
	}
}

func TestPercentile_MatchesPostgresPercentileCont(t *testing.T) {
	cases := []struct {
		name   string
		values []float64
		p      float64
		want   float64
	}{
		{"empty", nil, 0.5, 0},
		{"single", []float64{42}, 0.95, 42},
		{"odd median is the middle", []float64{900, 60, 300}, 0.5, 300},
		{"even median interpolates", []float64{600, 60, 300, 120}, 0.5, 210},
		{"p95 of four interpolates 0.85 of the way", []float64{10, 20, 30, 40}, 0.95, 38.5},
		{"p95 of twenty-one", seq(21), 0.95, 19},
		{"p0 is the minimum", []float64{5, 3, 9}, 0, 3},
		{"p100 is the maximum", []float64{5, 3, 9}, 1, 9},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := Percentile(tc.values, tc.p); math.Abs(got-tc.want) > 1e-9 {
				t.Fatalf("Percentile(%v, %v) = %v, want %v", tc.values, tc.p, got, tc.want)
			}
		})
	}
}

func TestPercentile_DoesNotMutateItsInput(t *testing.T) {
	in := []float64{30, 10, 20}
	Percentile(in, 0.5)
	if !reflect.DeepEqual(in, []float64{30, 10, 20}) {
		t.Fatalf("input was reordered: %v", in)
	}
}

// seq returns 0..n-1 reversed, so sorting matters.
func seq(n int) []float64 {
	out := make([]float64, n)
	for i := range out {
		out[i] = float64(n - 1 - i)
	}
	return out
}
