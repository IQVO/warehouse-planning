package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/claudioed/warehouse-planning/internal/adapters/kafka/cloudevents"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
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

// StorageCapacityConsumer is a pure TALLY MAINTAINER: it folds facility-layout's
// location-slot stream into the position/station tally
// (ports.StorageTallyRepository) and does nothing else. It registers no
// ProcessCapacity constraint -- a position/station COUNT is not a throughput.
// Station counts are composed with LABOR and the operator-declared
// StationStandard at READ time (usecases.GetProcessPathCapacity, ADR 0002),
// and storage positions are a read model (usecases.GetStorageCapacity).
//
// UoW is REQUIRED: the processed-event claim and the tally mutation for ONE
// message run inside a single UoW.Do, so they commit or roll back together --
// the claim is never recorded for a message whose tally mutation failed.
// Retry tunes the run loop's backoff (zero value = defaults).
type StorageCapacityConsumer struct {
	Reader          Reader
	Tally           ports.StorageTallyRepository
	ProcessedEvents ports.ProcessedEventRepository
	UoW             ports.UnitOfWork
	Logger          *slog.Logger
	Retry           RetryPolicy

	// DLQ is this consumer's dead-letter writer (topic FacilityTopic +
	// DLQSuffix), built by NewStorageCapacityConsumer. A transient
	// failure is retried domainMaxHandlerAttempts times before the
	// message is published here instead of blocking the partition
	// forever (deadletter.go).
	DLQ      DeadLetterWriter
	DLQTopic string

	sleep sleepFunc // test hook; nil => real, ctx-cancellable sleep
}

// NewStorageCapacityConsumer constructs a StorageCapacityConsumer reading
// FacilityTopic from brokers under groupID (env-configured, never a
// literal).
func NewStorageCapacityConsumer(
	brokers []string,
	groupID string,
	tallyRepo ports.StorageTallyRepository,
	processedEvents ports.ProcessedEventRepository,
	uow ports.UnitOfWork,
	logger *slog.Logger,
) *StorageCapacityConsumer {
	return &StorageCapacityConsumer{
		Reader:          kafkago.NewReader(readerConfig(brokers, FacilityTopic, groupID)),
		Tally:           tallyRepo,
		ProcessedEvents: processedEvents,
		UoW:             uow,
		Logger:          defaultLogger(logger),
		DLQ:             newDomainDLQWriter(brokers, FacilityTopic),
		DLQTopic:        FacilityTopic + DLQSuffix,
	}
}

// Run consumes FacilityTopic until ctx is cancelled or the reader fails. It
// is at-least-once: a message's offset is committed only after
// HandleMessage returned nil. A transient failure retries the SAME
// message with capped exponential backoff (see consumeLoop) up to
// domainMaxHandlerAttempts times, after which it is dead-lettered rather
// than blocking the partition forever.
func (c *StorageCapacityConsumer) Run(ctx context.Context) error {
	loop := consumeLoop{
		reader: c.Reader,
		handle: func(ctx context.Context, msg kafkago.Message) error { return c.HandleMessage(ctx, msg.Value) },
		logger: defaultLogger(c.Logger),
		name:   "storage capacity consumer",
		retry:  c.Retry,
		sleep:  c.sleep,
	}
	if c.DLQ != nil {
		loop.maxAttempts = domainMaxHandlerAttempts
		loop.deadLetter = func(ctx context.Context, msg kafkago.Message, cause error) error {
			return publishDeadLetter(ctx, c.DLQ, c.DLQTopic, msg, cause)
		}
	}
	return loop.run(ctx)
}

// Close releases the underlying Kafka reader and dead-letter writer.
func (c *StorageCapacityConsumer) Close() error {
	err := c.Reader.Close()
	if c.DLQ != nil {
		err = errors.Join(err, c.DLQ.Close())
	}
	return err
}

// HandleMessage decodes one CloudEvents 1.0 message and dispatches it to
// the matching handler.
//
// It returns nil for anything deterministic -- failed CloudEvents
// validation, an unrecognized `type`, a malformed payload, missing fields,
// an already-processed event id, an untracked decommission (all logged) --
// because retrying those can never succeed. It returns a non-nil error ONLY
// for transient/infrastructure failures (begin/commit, claim, tally), after
// the unit of work has rolled back (claim and tally changes both undone), so
// the caller may retry the same message.
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
			// The slot is already tallied (a producer re-emitting a slot
			// under a NEW event id). Safe under redelivery: claim and tally
			// are one transaction, so an attempt that failed rolled its
			// tally registration back too and its retry registers for real.
			c.Logger.InfoContext(ctx, "skipping already-registered locationCode", "location_code", reg.locationCode)
		}
		return nil
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
		_, found, err := c.Tally.DecommissionSlot(ctx, data.LocationCode)
		if err != nil {
			return fmt.Errorf("decommission slot: %w", err)
		}
		if !found {
			// Never crash, never go negative: a decommission for a slot
			// this consumer never saw registered is logged and skipped.
			c.Logger.WarnContext(ctx, "decommission received for an untracked slot", "location_code", data.LocationCode, "event_id", e.ID())
		}
		return nil
	})
}
