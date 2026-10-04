package ports

import (
	"context"

	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// StorageTallyReader is the READ side of the facility-layout tally (the
// write side is StorageTallyRepository, used only by the facility consumer).
// A planning `location` is a site/building code, and the zones of that site
// are the tally zones whose zone id starts with `<location>-` (the zone id's
// first dash-separated segment is the SiteCode; ADR 0002). A zone whose id
// has no matching site simply never contributes.
type StorageTallyReader interface {
	// StationCount sums the STATION tally of activity (upper-case, e.g.
	// "PACK") across every zone of the site `location`.
	StationCount(ctx context.Context, location, activity string) (int, error)

	// SiteBuckets lists every tally bucket with a count above zero in the
	// zones of the site `location`, ordered by zone id, tally type, tally
	// key. It is empty (not an error) when the site has no tallied slots.
	SiteBuckets(ctx context.Context, location string) ([]tally.Bucket, error)
}
