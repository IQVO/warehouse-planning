// Package ports declares the outbound interfaces the application layer
// depends on. Adapters implement these; the application never imports an
// adapter package (see internal/architecture's hexagonal fitness tests).
package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// StorageTallyRepository is the local, consumer-owned read model fed by
// facility-layout's LocationSlotRegistered/LocationSlotDecommissioned
// events. It is NOT the ProcessCapacity repository -- it only tracks the
// raw position/station counts this phase derives a CapacityConstraint
// quantity from; internal/application/usecases.RegisterProcessCapacityConstraint
// is still the only thing that ever writes a ProcessCapacity aggregate.
type StorageTallyRepository interface {
	// RegisterSlot records locationCode's contribution (so a later
	// DecommissionSlot can find it) and increments by 1 every tally
	// bucket named by tallyKeys under (zoneID, tallyType), returning
	// their new counts. A locationCode already registered is a no-op:
	// RegisterSlot returns (nil, nil) and increments nothing, so a
	// legitimate retry with a NEW CloudEvents id (bypassing the
	// ProcessedEventRepository dedupe) still can't double-count the
	// same physical slot.
	RegisterSlot(ctx context.Context, locationCode, zoneID, tallyType string, tallyKeys []string) ([]tally.Update, error)

	// DecommissionSlot decrements (floored at 0) every tally bucket
	// locationCode previously registered against and forgets the
	// registration. found=false means locationCode was never
	// registered (an untracked slot) -- the caller logs a warning and
	// does nothing further; this is never an error.
	DecommissionSlot(ctx context.Context, locationCode string) (updates []tally.Update, found bool, err error)
}
