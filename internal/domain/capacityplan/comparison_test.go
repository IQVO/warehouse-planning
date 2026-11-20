package capacityplan

import (
	"errors"
	"testing"
	"time"
)

func TestValidateComparisonIDs(t *testing.T) {
	for _, ok := range [][]string{{"a", "b"}, {"a", "b", "c", "d", "e"}} {
		if err := ValidateComparisonIDs(ok); err != nil {
			t.Errorf("%v: %v, want nil", ok, err)
		}
	}
	for _, bad := range [][]string{nil, {"a"}, {"a", "b", "c", "d", "e", "f"}, {"a", "a"}, {"a", " "}, {"", "b"}} {
		if err := ValidateComparisonIDs(bad); !errors.Is(err, ErrInvalidComparison) {
			t.Errorf("%q: %v, want ErrInvalidComparison", bad, err)
		}
	}
}

func TestCheckComparable(t *testing.T) {
	plan := func(id, site string, start, end time.Time) *CapacityPlan {
		return Rehydrate(RehydrateParams{ID: id, SiteID: site, Window: mustWindow(t, start, end), ProcessPathID: id})
	}
	base := plan("a", "SIM1", windowStart, windowEnd)
	sameInZone := plan("b", "SIM1", windowStart.In(time.FixedZone("BRT", -3*60*60)), windowEnd)
	cases := []struct {
		name  string
		plans []*CapacityPlan
		want  error
	}{
		{"one site and window, other paths", []*CapacityPlan{base, sameInZone, plan("c", "SIM1", windowStart, windowEnd)}, nil},
		{"another site", []*CapacityPlan{base, plan("c", "SIM2", windowStart, windowEnd)}, ErrComparisonSiteMismatch},
		{"another start", []*CapacityPlan{base, plan("c", "SIM1", windowStart.Add(time.Hour), windowEnd)}, ErrComparisonWindowMismatch},
		{"another end", []*CapacityPlan{base, plan("c", "SIM1", windowStart, windowEnd.Add(-time.Hour))}, ErrComparisonWindowMismatch},
		{"the third plan differs", []*CapacityPlan{base, sameInZone, plan("c", "SIM2", windowStart, windowEnd)}, ErrComparisonSiteMismatch},
		{"no plan", nil, ErrInvalidComparison},
	}
	for _, c := range cases {
		err := CheckComparable(c.plans)
		if !errors.Is(err, c.want) || (c.want == nil && err != nil) {
			t.Errorf("%s: %v, want %v", c.name, err, c.want)
		}
	}
}
