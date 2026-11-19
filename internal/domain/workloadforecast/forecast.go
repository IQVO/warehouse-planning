// Package workloadforecast holds the WorkloadForecast aggregate (docs/adr/0023):
// the volume a process path is expected to receive, as CPT buckets, for one
// site. It is wes-work-planning's ChargeForecast moved here with its invariant
// unchanged (at least one bucket) and a site added to its identity.
//
// One forecast per (site id, path id); the path id is held in its canonical
// form (CanonicalPathID: trimmed, upper case), so "pick" and " PICK " are one
// forecast. A new receipt replaces the buckets as a whole. A receipt whose
// buckets equal the stored ones changes nothing: no event, no version bump.
// The pick-load annotations wes carried per bucket (sku, pick unit, handles,
// cube, weight) are not moved: nothing read them.
//
// The package performs NO I/O and imports nothing from this module.
package workloadforecast

import (
	"errors"
	"slices"
	"strings"
	"time"
)

// CanonicalPathID is the one form of a process-path code on the requirement
// side (docs/adr/0023): trimmed, upper case. Every forecast and labour plan
// key, every stored row and every published path_id carries it, so a code
// sent as "pick" and one sent as " PICK " name the same path (the LABOR
// keying and the station tally already read path codes upper-cased). A code
// blank after trimming stays blank and is refused by the constructors.
func CanonicalPathID(pathID string) string {
	return strings.ToUpper(strings.TrimSpace(pathID))
}

var (
	// ErrRequiredField is returned when the site id or the path id is blank
	// (the path id after trimming) or the receipt time is zero.
	ErrRequiredField = errors.New("workloadforecast: site id, path id and the receipt time are required")

	// ErrNoBuckets is wes's charge.ErrNoBuckets, moved verbatim: a forecast
	// carries at least one CPT bucket.
	ErrNoBuckets = errors.New("workloadforecast: a forecast requires at least one CPT bucket")

	// ErrMissingCPT is returned for a bucket without a CPT instant.
	ErrMissingCPT = errors.New("workloadforecast: every bucket needs a cpt")

	// ErrNegativeQuantity is returned for a bucket quantity below zero.
	ErrNegativeQuantity = errors.New("workloadforecast: bucket quantity must be a non-negative whole number of units")

	// ErrIdentityMismatch is returned when a receipt for another (site, path)
	// is applied to a forecast.
	ErrIdentityMismatch = errors.New("workloadforecast: a receipt can only replace the forecast of the same site and path")

	// ErrInvalidVersion is returned when a stored forecast is rehydrated with
	// a version below 1.
	ErrInvalidVersion = errors.New("workloadforecast: a stored forecast has version 1 or more")
)

// Bucket is the volume due at one critical pull time: Quantity units whose
// CPT is CPT.
type Bucket struct {
	CPT      time.Time
	Quantity int64
}

// Forecast is the WorkloadForecast aggregate. Build it with New (a first
// receipt) or Rehydrate (a stored row); replace it with Receive.
type Forecast struct {
	siteID     string
	pathID     string
	buckets    []Bucket
	receivedAt time.Time
	version    int64
}

// New validates a first receipt and returns the forecast at version 1 with
// its WorkloadForecastReceived event. The path id is stored and published in
// its canonical form.
func New(siteID, pathID string, buckets []Bucket, receivedAt time.Time) (*Forecast, Received, error) {
	pathID = CanonicalPathID(pathID)
	if err := validate(siteID, pathID, buckets, receivedAt); err != nil {
		return nil, Received{}, err
	}
	f := &Forecast{siteID: siteID, pathID: pathID, buckets: slices.Clone(buckets), receivedAt: receivedAt, version: 1}
	return f, f.received(), nil
}

// Rehydrate rebuilds a stored forecast. It re-checks every invariant, so a
// corrupt row surfaces as an error instead of a forecast nobody could have
// received, and holds the path id in its canonical form.
func Rehydrate(siteID, pathID string, buckets []Bucket, receivedAt time.Time, version int64) (*Forecast, error) {
	pathID = CanonicalPathID(pathID)
	if err := validate(siteID, pathID, buckets, receivedAt); err != nil {
		return nil, err
	}
	if version < 1 {
		return nil, ErrInvalidVersion
	}
	return &Forecast{siteID: siteID, pathID: pathID, buckets: slices.Clone(buckets), receivedAt: receivedAt, version: version}, nil
}

// Receive applies a new receipt for the same (site, path); the path ids are
// compared in their canonical form. The buckets are replaced as a whole and
// the version goes up by one; the event is returned with changed=true.
// Buckets equal to the stored ones (same order) change nothing:
// changed=false, no event, the version and the receipt time stay.
func (f *Forecast) Receive(siteID, pathID string, buckets []Bucket, receivedAt time.Time) (Received, bool, error) {
	pathID = CanonicalPathID(pathID)
	if err := validate(siteID, pathID, buckets, receivedAt); err != nil {
		return Received{}, false, err
	}
	if siteID != f.siteID || pathID != f.pathID {
		return Received{}, false, ErrIdentityMismatch
	}
	if slices.EqualFunc(buckets, f.buckets, sameBucket) {
		return Received{}, false, nil
	}
	f.buckets = slices.Clone(buckets)
	f.receivedAt = receivedAt
	f.version++
	return f.received(), true, nil
}

// SiteID is the canonical facility-layout site code the forecast is for.
func (f *Forecast) SiteID() string { return f.siteID }

// PathID is the process path the volume is due on, in its canonical form.
func (f *Forecast) PathID() string { return f.pathID }

// Buckets returns a copy of the CPT buckets in receipt order.
func (f *Forecast) Buckets() []Bucket { return slices.Clone(f.buckets) }

// ReceivedAt is when the current buckets were received.
func (f *Forecast) ReceivedAt() time.Time { return f.receivedAt }

// Version starts at 1 and goes up by one per receipt that changed the
// buckets.
func (f *Forecast) Version() int64 { return f.version }

// TotalQuantity is the sum of every bucket's quantity.
func (f *Forecast) TotalQuantity() int64 {
	var total int64
	for _, b := range f.buckets {
		total += b.Quantity
	}
	return total
}

// QuantityDueIn sums the quantity of the buckets whose CPT is in
// [from, to): start-inclusive, end-exclusive. buckets is how many buckets
// fell in the window; zero buckets is no data, which a caller must not read
// as zero volume.
func (f *Forecast) QuantityDueIn(from, to time.Time) (units int64, buckets int) {
	for _, b := range f.buckets {
		if b.CPT.Compare(from) >= 0 && b.CPT.Compare(to) < 0 {
			units += b.Quantity
			buckets++
		}
	}
	return units, buckets
}

func (f *Forecast) received() Received {
	return Received{
		SiteID:        f.siteID,
		PathID:        f.pathID,
		TotalQuantity: f.TotalQuantity(),
		BucketCount:   len(f.buckets),
		ReceivedAt:    f.receivedAt,
		Version:       f.version,
	}
}

func validate(siteID, pathID string, buckets []Bucket, receivedAt time.Time) error {
	if siteID == "" || pathID == "" || receivedAt.IsZero() {
		return ErrRequiredField
	}
	if len(buckets) == 0 {
		return ErrNoBuckets
	}
	for _, b := range buckets {
		if b.CPT.IsZero() {
			return ErrMissingCPT
		}
		if b.Quantity < 0 {
			return ErrNegativeQuantity
		}
	}
	return nil
}

func sameBucket(a, b Bucket) bool { return a.CPT.Equal(b.CPT) && a.Quantity == b.Quantity }
