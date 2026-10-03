// Package tally holds the small value objects exchanged between Phase 3's
// Kafka storage/station consumer and its ports.StorageTallyRepository
// port: the raw position/station counts a facility-layout
// LocationSlotRegistered/Decommissioned stream is tallied into. These are
// consumer bookkeeping, not ubiquitous-language domain concepts (see
// .claude/rules/domain-model.md) -- kept in their own application
// subpackage, not ports, because
// internal/architecture/architecture_test.go's "ports package only
// contains interfaces" content rule would fail on a struct declared
// directly in ports.
package tally

// Tally type discriminators. LOCATION tallies a role=Storage
// facility-layout slot per (zoneId, locationType); STATION tallies a
// role=WorkCenter slot per (zoneId, activity) -- see
// docs/adr/0001-warehouse-planning-bounded-context.md's Addendum.
const (
	TypeLocation = "LOCATION"
	TypeStation  = "STATION"
)

// Update is one (zone, tally type, tally key) bucket's count after a
// StorageTallyRepository.RegisterSlot/DecommissionSlot call.
type Update struct {
	ZoneID    string
	TallyType string
	TallyKey  string
	Count     int
}
