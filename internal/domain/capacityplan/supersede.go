package capacityplan

import (
	"errors"
	"time"
)

var (
	// ErrSuperseded is returned by Publish and Supersede on a SUPERSEDED
	// plan: the status is terminal (docs/adr/0024 decision 3), so a
	// superseded plan is never published again nor superseded twice.
	ErrSuperseded = errors.New("capacityplan: capacity plan is superseded")

	// ErrNotSupersedable is returned by Supersede on a plan that is not
	// PUBLISHED (a DRAFT announced nothing, so nothing replaces it), and when
	// the newer plan's id is blank or the plan's own id.
	ErrNotSupersedable = errors.New("capacityplan: only a PUBLISHED plan can be superseded, and only by another plan")
)

// SupersedeKey is what makes two plans the same plan for publication
// (docs/adr/0024 decision 3): the site, the location, the process path and
// the window. At most one plan per key is PUBLISHED at any time; publishing a
// plan supersedes the one published before it under the same key.
type SupersedeKey struct {
	SiteID      string
	Location    string
	PathID      string
	WindowStart time.Time
	WindowEnd   time.Time
}

// SupersedeKey returns the plan's key.
func (c *CapacityPlan) SupersedeKey() SupersedeKey {
	return SupersedeKey{
		SiteID: c.siteID, Location: c.location, PathID: c.processPathID,
		WindowStart: c.window.Start(), WindowEnd: c.window.End(),
	}
}

// Matches reports whether p has key k. The window bounds compare as
// instants (equal instants match whatever their zone).
func (k SupersedeKey) Matches(p *CapacityPlan) bool {
	other := p.SupersedeKey()
	return k.SiteID == other.SiteID && k.Location == other.Location && k.PathID == other.PathID &&
		k.WindowStart.Equal(other.WindowStart) && k.WindowEnd.Equal(other.WindowEnd)
}

// Supersede moves a PUBLISHED plan to SUPERSEDED because the plan with id by
// was published under the same SupersedeKey at at, and records
// CapacityPlanSuperseded (subject: this, the OLDER, plan). It returns
// ErrSuperseded on a SUPERSEDED plan and ErrNotSupersedable on a DRAFT, a
// blank by or by equal to the plan's own id, recording nothing. The plan
// keeps its published_at and every computed figure: only the status moves.
func (c *CapacityPlan) Supersede(by string, at time.Time) error {
	if c.status == StatusSuperseded {
		return ErrSuperseded
	}
	if c.status != StatusPublished || by == "" || by == c.id {
		return ErrNotSupersedable
	}
	c.status = StatusSuperseded
	c.supersededBy = by
	c.record(CapacityPlanSuperseded{
		Header:       Header{PlanID: c.id, At: at},
		SupersededBy: by,
		WarehouseID:  c.warehouseID,
		SiteID:       c.siteID,
		Location:     c.location,
		PathID:       c.processPathID,
		WindowStart:  c.window.Start(),
		WindowEnd:    c.window.End(),
	})
	return nil
}

// SupersededBy returns the id of the plan whose publication superseded this
// one; empty unless the status is SUPERSEDED.
func (c *CapacityPlan) SupersededBy() string { return c.supersededBy }
