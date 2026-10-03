package kafka

import (
	"context"
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
type StorageCapacityConsumer struct {
	Reader          Reader
	Register        *usecases.RegisterProcessCapacityConstraint
	Tally           ports.StorageTallyRepository
	ProcessedEvents ports.ProcessedEventRepository
	Logger          *slog.Logger
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
	logger *slog.Logger,
) *StorageCapacityConsumer {
	return &StorageCapacityConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, FacilityTopic, groupID)),
		Register:        register,
		Tally:           tallyRepo,
		ProcessedEvents: processedEvents,
		Logger:          defaultLogger(logger),
	}
}

// Run consumes FacilityTopic until ctx is cancelled or the reader fails.
func (c *StorageCapacityConsumer) Run(ctx context.Context) error {
	for {
		msg, err := c.Reader.ReadMessage(ctx)
		if err != nil {
			return err
		}
		if err := c.HandleMessage(ctx, msg.Value); err != nil {
			c.Logger.ErrorContext(ctx, "storage capacity event handling failed",
				"error", err, "partition", msg.Partition, "offset", msg.Offset)
		}
	}
}

// Close releases the underlying Kafka reader.
func (c *StorageCapacityConsumer) Close() error {
	return c.Reader.Close()
}

// HandleMessage decodes one CloudEvents 1.0 message and dispatches it to
// the matching handler. Anything that fails CloudEvents validation,
// carries an unrecognized `type`, or has a malformed payload is logged
// and skipped -- never an error that would stop the consumer.
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

func (c *StorageCapacityConsumer) handleRegistered(ctx context.Context, e eventDecoder) error {
	claimed, err := c.ProcessedEvents.Claim(ctx, storageConsumerName, e.ID())
	if err != nil {
		return err
	}
	if !claimed {
		c.Logger.InfoContext(ctx, "skipping already-processed LocationSlotRegistered event", "event_id", e.ID())
		return nil
	}

	var data locationSlotRegisteredData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed LocationSlotRegistered payload", "error", err, "event_id", e.ID())
		return nil
	}
	if data.LocationCode == "" || data.ZoneID == "" {
		c.Logger.WarnContext(ctx, "skipping LocationSlotRegistered with missing locationCode/zoneId", "event_id", e.ID())
		return nil
	}

	role := data.Role
	if role == "" {
		role = roleStorage
	}

	switch role {
	case roleStorage:
		return c.registerStorageSlot(ctx, e, data)
	case roleWorkCenter:
		return c.registerWorkCenterSlot(ctx, e, data)
	default:
		c.Logger.WarnContext(ctx, "skipping LocationSlotRegistered with unrecognized role", "role", role, "event_id", e.ID())
		return nil
	}
}

// registerStorageSlot handles the role=Storage (or absent) branch of
// handleRegistered: one tally bucket, keyed by locationType.
func (c *StorageCapacityConsumer) registerStorageSlot(ctx context.Context, e eventDecoder, data locationSlotRegisteredData) error {
	if data.LocationType == "" {
		c.Logger.WarnContext(ctx, "skipping Storage LocationSlotRegistered with empty locationType", "event_id", e.ID())
		return nil
	}
	updates, err := c.Tally.RegisterSlot(ctx, data.LocationCode, data.ZoneID, tally.TypeLocation, []string{data.LocationType})
	if err != nil {
		return err
	}
	if updates == nil {
		c.Logger.InfoContext(ctx, "skipping already-registered locationCode", "location_code", data.LocationCode)
		return nil
	}
	return c.applyTallyUpdates(ctx, updates)
}

// registerWorkCenterSlot handles the role=WorkCenter branch of
// handleRegistered: one tally bucket per activity, all keyed by zoneID.
func (c *StorageCapacityConsumer) registerWorkCenterSlot(ctx context.Context, e eventDecoder, data locationSlotRegisteredData) error {
	if len(data.Activities) == 0 {
		c.Logger.WarnContext(ctx, "skipping WorkCenter LocationSlotRegistered with no activities", "event_id", e.ID())
		return nil
	}
	keys := make([]string, 0, len(data.Activities))
	for _, a := range data.Activities {
		keys = append(keys, strings.ToUpper(a))
	}
	updates, err := c.Tally.RegisterSlot(ctx, data.LocationCode, data.ZoneID, tally.TypeStation, keys)
	if err != nil {
		return err
	}
	if updates == nil {
		c.Logger.InfoContext(ctx, "skipping already-registered locationCode", "location_code", data.LocationCode)
		return nil
	}
	return c.applyTallyUpdates(ctx, updates)
}

func (c *StorageCapacityConsumer) handleDecommissioned(ctx context.Context, e eventDecoder) error {
	claimed, err := c.ProcessedEvents.Claim(ctx, storageConsumerName, e.ID())
	if err != nil {
		return err
	}
	if !claimed {
		c.Logger.InfoContext(ctx, "skipping already-processed LocationSlotDecommissioned event", "event_id", e.ID())
		return nil
	}

	var data locationSlotDecommissionedData
	if err := e.DataAs(&data); err != nil {
		c.Logger.WarnContext(ctx, "skipping malformed LocationSlotDecommissioned payload", "error", err, "event_id", e.ID())
		return nil
	}
	if data.LocationCode == "" {
		c.Logger.WarnContext(ctx, "skipping LocationSlotDecommissioned with empty locationCode", "event_id", e.ID())
		return nil
	}

	updates, found, err := c.Tally.DecommissionSlot(ctx, data.LocationCode)
	if err != nil {
		return err
	}
	if !found {
		// Never crash, never go negative: a decommission for a slot
		// this consumer never saw registered is logged and skipped.
		c.Logger.WarnContext(ctx, "decommission received for an untracked slot", "location_code", data.LocationCode, "event_id", e.ID())
		return nil
	}
	return c.applyTallyUpdates(ctx, updates)
}

// applyTallyUpdates re-registers the ProcessCapacity CapacityConstraint
// matching each updated tally bucket with its new count. See
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
			c.Logger.WarnContext(ctx, "skipping tally update that failed domain validation", "error", err, "zone_id", u.ZoneID, "tally_type", u.TallyType, "tally_key", u.TallyKey)
			continue
		}
	}
	return nil
}
