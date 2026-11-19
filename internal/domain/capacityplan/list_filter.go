package capacityplan

import "time"

// ListFilter restricts a list of plans (docs/adr/0024 decision 2: the filters
// are additive). A plan passes only when it matches EVERY field that is set;
// the zero value passes every plan. Location, SiteID and PathID compare
// exactly; WindowStart and WindowEnd compare the instants of the plan's
// window (equal instants match whatever their zone).
type ListFilter struct {
	Location    string
	SiteID      string
	PathID      string
	WindowStart *time.Time
	WindowEnd   *time.Time
}

// Matches reports whether p passes every field of the filter that is set.
func (f ListFilter) Matches(p *CapacityPlan) bool {
	return textMatches(f.Location, p.location) && textMatches(f.SiteID, p.siteID) &&
		textMatches(f.PathID, p.processPathID) &&
		instantMatches(f.WindowStart, p.window.Start()) && instantMatches(f.WindowEnd, p.window.End())
}

func textMatches(want, got string) bool { return want == "" || want == got }

func instantMatches(want *time.Time, got time.Time) bool { return want == nil || want.Equal(got) }
