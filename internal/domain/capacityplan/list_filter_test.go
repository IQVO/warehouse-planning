package capacityplan

import (
	"testing"
	"time"
)

func TestListFilter_MatchesEveryFieldThatIsSet(t *testing.T) {
	plan := Rehydrate(RehydrateParams{
		ID: planID, SiteID: "SIM1", Location: "FC01", ProcessPathID: "tote",
		Window: mustWindow(t, windowStart, windowEnd),
	})
	start, end := windowStart, windowEnd
	inZone := windowStart.In(time.FixedZone("BRT", -3*60*60))
	other := windowStart.Add(time.Second)
	cases := []struct {
		name   string
		filter ListFilter
		want   bool
	}{
		{"the zero filter", ListFilter{}, true},
		{"location", ListFilter{Location: "FC01"}, true},
		{"another location", ListFilter{Location: "FC02"}, false},
		{"site", ListFilter{SiteID: "SIM1"}, true},
		{"another site", ListFilter{SiteID: "SIM2"}, false},
		{"path", ListFilter{PathID: "tote"}, true},
		{"another path", ListFilter{PathID: "TOTE"}, false},
		{"window start", ListFilter{WindowStart: &start}, true},
		{"window start in another zone", ListFilter{WindowStart: &inZone}, true},
		{"another window start", ListFilter{WindowStart: &other}, false},
		{"window end", ListFilter{WindowEnd: &end}, true},
		{"window end given as the start", ListFilter{WindowEnd: &start}, false},
		{"every field", ListFilter{Location: "FC01", SiteID: "SIM1", PathID: "tote", WindowStart: &start, WindowEnd: &end}, true},
		{"every field but one", ListFilter{Location: "FC01", SiteID: "SIM1", PathID: "tote", WindowStart: &start, WindowEnd: &other}, false},
	}
	for _, c := range cases {
		if got := c.filter.Matches(plan); got != c.want {
			t.Errorf("%s: Matches = %v, want %v", c.name, got, c.want)
		}
	}
}
