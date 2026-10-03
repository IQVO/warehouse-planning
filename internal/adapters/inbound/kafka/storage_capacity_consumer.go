package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/application/usecases"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

// FacilityTopic is facility-layout's integration topic. This service has
// no business knowing anything else about that context beyond this topic
// name and the CloudEvents types/payload shapes below.
const FacilityTopic = "warehouse.facility.events"

// Confirmed facility-layout `type` strings (docs/adr/0001-...'s
// Addendum). Dispatch is on the FULL, byte-identical type string.
const (
	typeLocationSlotRegistered     = "com.warehouse.wms.facility-layout.locationslot.LocationSlotRegistered"
	typeLocationSlotDecommissioned = "com.warehouse.wms.facility-layout.locationslot.LocationSlotDecommissioned"
)

// storageConsumerName namespaces this consumer's rows in
// ports.ProcessedEventRepository.
const storageConsumerName = "storage-capacity-consumer"

// roleStorage / roleWorkCenter are facility-layout's `role` values this
// consumer acts on. An absent/empty role defaults to roleStorage (the
// Addendum's confirmed default).
const (
	roleStorage    = "Storage"
	roleWorkCenter = "WorkCenter"
)

// storageProcessType is the sentinel ProcessType this phase registers a
// bare storage-position tally under. A raw location-slot count has no
// naturally implied ProcessType the way a WorkCenter activity does (PACK,
// SORT, ...) -- see this file's package doc and the design note in the
// final delivery report: forcing it onto ProcessCapacity's
// (ProcessType, Location, Window) identity is a pragmatic Phase 3 choice,
// not a clean domain fit.
const storageProcessType = processcapacity.ProcessType("STORAGE")

// tallyRateUnit is the native CapacityUnit a LOCATION/STATION constraint
// derived from a position/station COUNT (not a throughput) is registered
// under. None of the domain's four units (UNIT/LINE/ORDER/PACKAGE)
// naturally represents "a count of physical positions or stations" --
// UnitLine is this phase's documented nearest-fit choice (a generic
// countable unit), paired with a 1-hour period purely to satisfy
// CapacityRate's required period, not because this is actually a
// per-hour throughput figure.
const tallyRateUnit = processcapacity.UnitLine
const tallyRatePeriod = time.Hour

// StandingWindowStart / StandingWindowEnd: the fixed, deterministic
// CapacityWindow this phase registers every LOCATION/STATION tally
// constraint under. A position/station count is a STANDING structural
// fact, not something naturally sliced into time windows the way labor
// capacity is -- using a single wide, constant window lets repeated
// registrations for the same (zone, key) keep landing on the SAME
// ProcessCapacity aggregate (so AddConstraint upserts in place) instead
// of inventing a new window per event.
var (
	StandingWindowStart = time.Unix(0, 0).UTC()
	StandingWindowEnd   = StandingWindowStart.AddDate(100, 0, 0)
)

// locationSlotRegisteredData mirrors facility-layout's
// LocationSlotRegistered payload (hand-mirrored locally, never imported
// from that service's own Go types).
type locationSlotRegisteredData struct {
	LocationCode string   `json:"locationCode"`
	ZoneID       string   `json:"zoneId"`
	LocationType string   `json:"locationType"`
	Role         string   `json:"role"`
	Activities   []string `json:"activities"`
}

// locationSlotDecommissionedData mirrors facility-layout's
// LocationSlotDecommissioned payload.
type locationSlotDecommissionedData struct {
	LocationCode string `json:"locationCode"`
}

// StorageCapacityConsumer tallies facility-layout's location-slot stream
// into LOCATION (role=Storage) and STATION (role=WorkCenter)
// CapacityConstraints. See docs/adr/0001-...'s Addendum and this file's
// storageProcessType/tallyRateUnit doc comments for the keying/unit
// decisions this phase made where the upstream contract and the domain
// model don't cleanly line up.
//
// UoW is REQUIRED: the processed-event claim, the tally mutation and every
// ProcessCapacity constraint upsert for ONE message run inside a single
// UoW.Do, so they commit or roll back together -- a failure between the
// tally step and the constraint step can never leave the two out of sync.
// Retry tunes the run loop's backoff (zero value = defaults).
type StorageCapacityConsumer struct {
	Reader          Reader
	Register        *usecases.RegisterProcessCapacityConstraint
	Tally           ports.StorageTallyRepository
	ProcessedEvents ports.ProcessedEventRepository
	UoW             ports.UnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewStorageCapacityConsumer constructs a StorageCapacityConsumer reading
// FacilityTopic from brokers under groupID (env-configured, never a
// literal).
func NewStorageCapacityConsumer(
	brokers []string,
	groupID string,
	register *usecases.RegisterProcessCapacityConstraint,
	tallyRepo ports.StorageTallyRepository,
	processedEvents ports.ProcessedEventRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) *StorageCapacityConsumer {
	return &StorageCapacityConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, FacilityTopic, groupID)),
		Register:        register,
		Tally:           tallyRepo,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
	}
}

// Run consumes FacilityTopic until ctx is cancelled or the reader fails. It
// is at-least-once: a message's offset is committed only after
// HandleMessage returned nil, and a transient failure retries the SAME
// message with capped exponential backoff (see consumeLoop) -- it is never
// skipped.
func (c *StorageCapacityConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "storage capacity consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	return loop.run(ctx)
}

// Close releases the underlying Kafka reader.
func (c *StorageCapacityConsumer) Close() error {
	return c.Reader.Close()
}

// HandleMessage decodes one CloudEvents 1.0 message and dispatches it to
// the matching handler.
//
// It returns nil for anything deterministic -- failed CloudEvents
// validation, an unrecognized `type`, a malformed payload, missing fields,
// an already-processed event id, an untracked decommission, a domain
// validation rejection (all logged) -- because retrying those can never
// succeed. It returns a non-nil error ONLY for transient/infrastructure
// failures, after the unit of work has rolled back (claim, tally and
// constraint changes all undone), so the caller may retry the same message.
func (c *StorageCapacityConsumer) HandleMessage(ctx context.Context, value []byte) error {
	e, err := cloudevents.Decode(value)
	if err != nil {
		c.Logger.WarnContext(ctx, "skipping non-CloudEvents facility message", "error", err)
		return nil
	}

	switch e.Type() {
	case typeLocationSlotRegistered:
		return c.handleRegistered(ctx, e)
	case typeLocationSlotDecommissioned:
		return c.handleDecommissioned(ctx, e)
	default:
		return nil
	}
}

// eventDecoder is the subset of ce.Event HandleMessage's handlers need,
// so they can be unit tested without re-decoding.
type eventDecoder interface {
	ID() string
	DataAs(obj any) error
}

// claim records this event as processed inside the current unit of work.
// alreadyProcessed=true means a previous, COMMITTED handling exists (a
// rolled-back one never counts), so the caller must skip.
func (c *StorageCapacityConsumer) claim(ctx context.Context, e eventDecoder, eventName string) (alreadyProcessed bool, err error) {
	claimed, err := c.ProcessedEvents.Claim(ctx, storageConsumerName, e.ID())
	if err != nil {
		return false, fmt.Errorf("claim processed event: %w", err)
	}
	if !claimed {
		c.Logger.InfoContext(ctx, "skipping already-processed "+eventName+" event", "event_id", e.ID())
	}
	return !claimed, nil
}

// slotRegistration is a validated LocationSlotRegistered, ready to apply.
type slotRegistration struct {
	locationCode string
	zoneID       string
	tallyType    string
	tallyKeys    []string
}

func (c *StorageCapacityConsumer) handleRegistered(ctx context.Context, e eventDecoder) error {
	// Pure validation first (no DB): a deterministic bad payload returns
	// nil without ever opening a transaction.
	reg, ok := c.parseRegistered(ctx, e)
	if !ok {
		return nil
	}

	return c.UoW.Do(ctx, func(ctx context.Context) error {
		if skip, err := c.claim(ctx, e, "LocationSlotRegistered"); err != nil || skip {
			return err
		}
		updates, err := c.Tally.RegisterSlot(ctx, reg.locationCode, reg.zoneID, reg.tallyType, reg.tallyKeys)
		if err != nil {
			return fmt.Errorf("register slot: %w", err)
		}
		if updates == nil {
			// The slot is already tallied. Safe under redelivery now that
			// claim + tally + constraint are one transaction: a previous
			// attempt that got this far also committed the constraint, and
			// one that failed rolled the tally back too, so this branch
			// can never mask a stale constraint.
			c.Logger.InfoContext(ctx, "skipping already-registered locationCode", "location_code", reg.locationCode)
			return nil
		}
		return c.applyTallyUpdates(ctx, updates)
	})
}

// parseRegistered decodes and validates a LocationSlotRegistered payload.
// ok=false means "deterministically unusable: warn and skip".
func (c *StorageCapacityConsumer) parseRegistered(ctx context.Context, e eventDecoder) (slotRegistration, bool) {
	var data locationSlotRegisteredData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed LocationSlotRegistered payload", "error", err, "event_id", e.ID())
		return slotRegistration{}, false
	}
	if data.LocationCode == "" || data.ZoneID == "" {
		c.Logger.WarnContext(ctx, "skipping LocationSlotRegistered with missing locationCode/zoneId", "event_id", e.ID())
		return slotRegistration{}, false
	}

	role := data.Role
	if role == "" {
		role = roleStorage
	}
	reg := slotRegistration{locationCode: data.LocationCode, zoneID: data.ZoneID}

	switch role {
	case roleStorage:
		// One tally bucket, keyed by locationType.
		if data.LocationType == "" {
			c.Logger.WarnContext(ctx, "skipping Storage LocationSlotRegistered with empty locationType", "event_id", e.ID())
			return slotRegistration{}, false
		}
		reg.tallyType = tally.TypeLocation
		reg.tallyKeys = []string{data.LocationType}
	case roleWorkCenter:
		// One tally bucket per activity, all keyed by zoneID.
		if len(data.Activities) == 0 {
			c.Logger.WarnContext(ctx, "skipping WorkCenter LocationSlotRegistered with no activities", "event_id", e.ID())
			return slotRegistration{}, false
		}
		reg.tallyType = tally.TypeStation
		for _, a := range data.Activities {
			reg.tallyKeys = append(reg.tallyKeys, strings.ToUpper(a))
		}
	default:
		c.Logger.WarnContext(ctx, "skipping LocationSlotRegistered with unrecognized role", "role", role, "event_id", e.ID())
		return slotRegistration{}, false
	}
	return reg, true
}

func (c *StorageCapacityConsumer) handleDecommissioned(ctx context.Context, e eventDecoder) error {
	var data locationSlotDecommissionedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed LocationSlotDecommissioned payload", "error", err, "event_id", e.ID())
		return nil
	}
	if data.LocationCode == "" {
		c.Logger.WarnContext(ctx, "skipping LocationSlotDecommissioned with empty locationCode", "event_id", e.ID())
		return nil
	}

	return c.UoW.Do(ctx, func(ctx context.Context) error {
		if skip, err := c.claim(ctx, e, "LocationSlotDecommissioned"); err != nil || skip {
			return err
		}
		updates, found, err := c.Tally.DecommissionSlot(ctx, data.LocationCode)
		if err != nil {
			return fmt.Errorf("decommission slot: %w", err)
		}
		if !found {
			// Never crash, never go negative: a decommission for a slot
			// this consumer never saw registered is logged and skipped.
			c.Logger.WarnContext(ctx, "decommission received for an untracked slot", "location_code", data.LocationCode, "event_id", e.ID())
			return nil
		}
		return c.applyTallyUpdates(ctx, updates)
	})
}

// applyTallyUpdates re-registers the ProcessCapacity CapacityConstraint
// matching each updated tally bucket with its new count, inside the
// caller's unit of work. A domain-validation rejection is logged and
// skipped; any other error (repository/infrastructure) is returned. See
// storageProcessType/tallyRateUnit's doc comments for the keying
// decisions:
//   - TypeLocation (role=Storage): ProcessType=storageProcessType
//     (sentinel), Location="<zoneID>:<locationType>" -- a composite key
//     folding locationType into Location because ConstraintType=LOCATION
//     is a single fixed vocabulary entry, not parameterized per
//     locationType, so two distinct locationTypes in the same zone would
//     otherwise overwrite each other's LOCATION constraint on the same
//     aggregate.
//   - TypeStation (role=WorkCenter): ProcessType=the activity itself
//     (already uppercased), Location=zoneID -- a clean fit, no composite
//     key needed.
func (c *StorageCapacityConsumer) applyTallyUpdates(ctx context.Context, updates []tally.Update) error {
	for _, u := range updates {
		var (
			processType    processcapacity.ProcessType
			location       string
			constraintType processcapacity.ConstraintType
		)
		switch u.TallyType {
		case tally.TypeLocation:
			processType = storageProcessType
			location = u.ZoneID + ":" + u.TallyKey
			constraintType = processcapacity.ConstraintLocation
		case tally.TypeStation:
			processType = processcapacity.ProcessType(u.TallyKey)
			location = u.ZoneID
			constraintType = processcapacity.ConstraintStation
		default:
			continue
		}

		_, err := c.Register.Handle(ctx, usecases.RegisterProcessCapacityConstraintCommand{
			ProcessType:    processType,
			Location:       location,
			WindowStart:    StandingWindowStart,
			WindowEnd:      StandingWindowEnd,
			ConstraintType: constraintType,
			Quantity:       float64(u.Count),
			Unit:           tallyRateUnit,
			Period:         tallyRatePeriod,
		})
		if err != nil {
			if usecases.IsDomainValidationError(err) {
				// Deterministic rejection: retrying can never help, and it
				// must not roll back the tally or fail the message.
				c.Logger.WarnContext(ctx, "skipping tally update that failed domain validation", "error", err, "zone_id", u.ZoneID, "tally_type", u.TallyType, "tally_key", u.TallyKey)
				continue
			}
			// Infrastructure failure: surface it so the enclosing unit of
			// work rolls the tally back too and the message is retried.
			return fmt.Errorf("register %s constraint for %s/%s: %w", u.TallyType, u.ZoneID, u.TallyKey, err)
		}
	}
	return nil
}
