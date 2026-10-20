package ports

import (
	"context"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

// InboundAppointmentRepository is the local inbound-demand read model fed by
// inbound-receiving's DockAppointmentBooked / DockAppointmentCancelled events
// (docs/adr/0017). One row per appointment id; it is never read from or
// written to inbound-receiving directly.
//
// Inside a UnitOfWork the repository joins the transaction carried in ctx, so
// the consumer's processed-event claim and the Apply commit together.
type InboundAppointmentRepository interface {
	// Apply stores next under its appointment id as decided by
	// inbound.Resolve against the stored row (nil when the id is new): the
	// ordering guard that makes Booked and Cancelled order-independent. It
	// reports whether the model changed.
	Apply(ctx context.Context, next inbound.Appointment) (changed bool, err error)

	// Demand summarizes the BOOKED appointments of the site `location`
	// whose door window starts in the half-open window [start, end), with
	// the ASNs they cover and the newest event time the site's model
	// reflects. A site with no appointment yields the zero Demand, not an
	// error.
	Demand(ctx context.Context, location string, start, end time.Time) (inbound.Demand, error)
}

// InboundStandardRepository persists the operator-declared inbound labour
// standards, keyed by (location, process path id). Declaring an existing key
// replaces the figures wholesale.
type InboundStandardRepository interface {
	// Save upserts the standard under its (location, process path) key.
	Save(ctx context.Context, standard inbound.LabourStandard) error
	// Find returns the standard declared for location + processPathID, or
	// (nil, nil) if none has been declared.
	Find(ctx context.Context, location, processPathID string) (*inbound.LabourStandard, error)
	// List returns the standards declared for location ordered by process
	// path id, or every standard (ordered by location, then path id) when
	// location is empty.
	List(ctx context.Context, location string) ([]inbound.LabourStandard, error)
}
