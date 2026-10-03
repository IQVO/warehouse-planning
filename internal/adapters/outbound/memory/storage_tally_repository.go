package memory

import (
	"context"
	"sync"

	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// slotRegistration is what a RegisterSlot call remembers for a given
// locationCode, so a later DecommissionSlot can find exactly what to
// decrement without the original payload.
type slotRegistration struct {
	zoneID    string
	tallyType string
	tallyKeys []string
}

func tallyMapKey(zoneID, tallyType, tallyKey string) string {
	return zoneID + "\x00" + tallyType + "\x00" + tallyKey
}

// StorageTallyRepo is an in-memory, mutex-guarded
// ports.StorageTallyRepository. For tests only.
type StorageTallyRepo struct {
	mu            sync.Mutex
	registrations map[string]slotRegistration
	counts        map[string]int
}

// NewStorageTallyRepo constructs an empty StorageTallyRepo.
func NewStorageTallyRepo() *StorageTallyRepo {
	return &StorageTallyRepo{
		registrations: make(map[string]slotRegistration),
		counts:        make(map[string]int),
	}
}

// RegisterSlot increments by 1 every tally bucket named by tallyKeys
// under (zoneID, tallyType) and remembers locationCode's contribution. A
// locationCode already registered is a no-op (returns nil, nil).
func (r *StorageTallyRepo) RegisterSlot(_ context.Context, locationCode, zoneID, tallyType string, tallyKeys []string) ([]tally.Update, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.registrations[locationCode]; exists {
		return nil, nil
	}
	r.registrations[locationCode] = slotRegistration{
		zoneID:    zoneID,
		tallyType: tallyType,
		tallyKeys: append([]string(nil), tallyKeys...),
	}

	updates := make([]tally.Update, 0, len(tallyKeys))
	for _, key := range tallyKeys {
		k := tallyMapKey(zoneID, tallyType, key)
		r.counts[k]++
		updates = append(updates, tally.Update{ZoneID: zoneID, TallyType: tallyType, TallyKey: key, Count: r.counts[k]})
	}
	return updates, nil
}

// DecommissionSlot decrements (floored at 0) every tally bucket
// locationCode previously registered against and forgets the
// registration. found=false means locationCode was never registered.
func (r *StorageTallyRepo) DecommissionSlot(_ context.Context, locationCode string) ([]tally.Update, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	reg, ok := r.registrations[locationCode]
	if !ok {
		return nil, false, nil
	}
	delete(r.registrations, locationCode)

	updates := make([]tally.Update, 0, len(reg.tallyKeys))
	for _, key := range reg.tallyKeys {
		k := tallyMapKey(reg.zoneID, reg.tallyType, key)
		if r.counts[k] > 0 {
			r.counts[k]--
		}
		updates = append(updates, tally.Update{ZoneID: reg.zoneID, TallyType: reg.tallyType, TallyKey: key, Count: r.counts[k]})
	}
	return updates, true, nil
}
