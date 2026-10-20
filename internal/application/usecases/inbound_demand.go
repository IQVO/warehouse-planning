package usecases

import (
	"context"
	"fmt"
	"time"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// RecordInboundAppointment writes one inbound-receiving dock-appointment event
// (Booked or Cancelled) into the inbound demand read model. The processed-event
// claim and the ordering-guarded write are ONE ports.UnitOfWork.Do, so a
// failure after the claim rolls the claim back and the redelivery is processed
// instead of skipped (docs/adr/0017 decision 5).
type RecordInboundAppointment struct {
	UoW             ports.UnitOfWork
	ProcessedEvents ports.ProcessedEventRepository
	Appointments    ports.InboundAppointmentRepository
}

// Handle claims (consumer, eventID) and applies a atomically. OutcomeApplied
// means the model changed, OutcomeStale that the event was older than (or
// equal and weaker than) what is stored, OutcomeDuplicate that the CloudEvents
// id was already processed. It returns a non-nil error ONLY for infrastructure
// failures; everything deterministic was rejected before the appointment was
// built.
func (uc *RecordInboundAppointment) Handle(ctx context.Context, consumer, eventID string, a inbound.Appointment) (RecordOutcome, error) {
	outcome := OutcomeDuplicate
	err := uc.UoW.Do(ctx, func(ctx context.Context) error {
		claimed, err := uc.ProcessedEvents.Claim(ctx, consumer, eventID)
		if err != nil {
			return err
		}
		if !claimed {
			return nil
		}
		changed, err := uc.Appointments.Apply(ctx, a)
		if err != nil {
			return err
		}
		outcome = OutcomeStale
		if changed {
			outcome = OutcomeApplied
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return outcome, nil
}

// DeclareInboundLabourStandardCommand carries the operator's declaration of the
// labour a dock appointment costs at a site for a process path.
type DeclareInboundLabourStandardCommand struct {
	Location              string
	ProcessPathID         string
	MinutesPerAppointment float64
	MinutesPerASN         float64
}

// DeclareInboundLabourStandard upserts an inbound.LabourStandard. It is a
// planning parameter owned by this context -- no upstream publishes it -- so
// declaring it is an explicit operator action (docs/adr/0017 decision 2).
type DeclareInboundLabourStandard struct {
	Repo ports.InboundStandardRepository
}

// Handle validates the declaration (inbound.NewLabourStandard errors), saves it
// and reports whether it created a new standard (true) or replaced an existing
// one (false).
func (uc *DeclareInboundLabourStandard) Handle(ctx context.Context, cmd DeclareInboundLabourStandardCommand) (inbound.LabourStandard, bool, error) {
	standard, err := inbound.NewLabourStandard(cmd.Location, cmd.ProcessPathID, cmd.MinutesPerAppointment, cmd.MinutesPerASN)
	if err != nil {
		return inbound.LabourStandard{}, false, err
	}
	existing, err := uc.Repo.Find(ctx, cmd.Location, cmd.ProcessPathID)
	if err != nil {
		return inbound.LabourStandard{}, false, err
	}
	if err := uc.Repo.Save(ctx, standard); err != nil {
		return inbound.LabourStandard{}, false, err
	}
	return standard, existing == nil, nil
}

// InboundDemandResult is the inbound demand of one site and process path over
// one window, with the labour the declared standard attributes to it.
type InboundDemandResult struct {
	// Demand is the BOOKED appointments starting in the window, the ASNs they
	// cover and the site's as-of time.
	Demand inbound.Demand
	// StandardDeclared is true when a standard exists for the requested
	// (location, process path).
	StandardDeclared bool
	// LabourMinutes is the standard's labour for Demand; 0 when no standard
	// is declared (never a guessed default).
	LabourMinutes float64
	// AverageHeads is LabourMinutes over the window length, in people.
	AverageHeads float64
	// StandardMissing is true when appointments exist but no standard is
	// declared for the requested path: they contribute no labour demand.
	StandardMissing bool
	// SiteHasNoStandard is true when the site declared no inbound standard for
	// ANY path. A plan only warns when this holds as well as StandardMissing,
	// so an outbound plan at a site that declared an inbound path elsewhere
	// is not nagged.
	SiteHasNoStandard bool
}

// GetInboundDemand reads the inbound demand of a site over a window from the
// local read model and prices it with the declared labour standard
// (docs/adr/0017). The labour is composed at READ time: nothing derived is
// stored with an appointment.
type GetInboundDemand struct {
	Appointments ports.InboundAppointmentRepository
	Standards    ports.InboundStandardRepository
}

// Handle returns the InboundDemandResult. It fails with
// processcapacity.ErrInvalidWindow when end is not strictly after start. A site
// or window without appointments is an empty result, not an error.
func (uc *GetInboundDemand) Handle(ctx context.Context, location, processPathID string, start, end time.Time) (InboundDemandResult, error) {
	window, err := processcapacity.NewCapacityWindow(start, end)
	if err != nil {
		return InboundDemandResult{}, err
	}
	demand, err := uc.Appointments.Demand(ctx, location, start, end)
	if err != nil {
		return InboundDemandResult{}, fmt.Errorf("read inbound demand: %w", err)
	}
	res := InboundDemandResult{Demand: demand}
	standard, err := uc.Standards.Find(ctx, location, processPathID)
	if err != nil {
		return InboundDemandResult{}, fmt.Errorf("read inbound standard: %w", err)
	}
	if standard != nil {
		res.StandardDeclared = true
		res.LabourMinutes = standard.LabourMinutes(demand)
		res.AverageHeads = inbound.AverageHeads(res.LabourMinutes, window.Duration())
		return res, nil
	}
	res.StandardMissing = demand.Appointments > 0
	if res.StandardMissing {
		site, err := uc.Standards.List(ctx, location)
		if err != nil {
			return InboundDemandResult{}, fmt.Errorf("list inbound standards: %w", err)
		}
		res.SiteHasNoStandard = len(site) == 0
	}
	return res, nil
}
