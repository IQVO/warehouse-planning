package repocontract

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// brt is a non-UTC zone: a store must keep the instant, not the zone.
var brt = time.FixedZone("BRT", -3*60*60)

// instant renders t as the instant it names, to the nanosecond.
func instant(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

// forecastKeys are the forecast identities (site id, path id): two sites on
// one path, and a second path of the first site.
var forecastKeys = [][2]string{{siteA, pathP}, {siteB, pathP}, {siteA, pathQ}}

// forecastBuckets is variant v's buckets: out of CPT order, one CPT in a
// non-UTC zone, one with nanoseconds and a zero quantity, so a store that
// sorts, normalises or truncates the buckets fails.
func forecastBuckets(v int) []workloadforecast.Bucket {
	n := int64(v)
	return []workloadforecast.Bucket{
		{CPT: T0.Add(9 * time.Hour).In(brt), Quantity: 120 + n},
		{CPT: T0.Add(2*time.Hour + 123456789), Quantity: 0},
		{CPT: T0.Add(time.Duration(5+v) * time.Hour), Quantity: 7 * (n + 1)},
	}
}

func receivedAt(v int) time.Time { return T0.Add(time.Duration(v) * time.Minute).In(brt) }

var forecasts = subject[*workloadforecast.Forecast]{
	get: func(fx Fixture, key int) (*workloadforecast.Forecast, bool, error) {
		k := forecastKeys[key]
		f, found, err := fx.Forecasts.Get(ctx, k[0], k[1])
		if err == nil && found != (f != nil) {
			return nil, false, fmt.Errorf("get returned forecast %v with found %v", f, found)
		}
		return f, found, err
	},
	save: func(fx Fixture, f *workloadforecast.Forecast) error { return fx.Forecasts.Save(ctx, f) },
	first: func(t *testing.T, key, variant int) *workloadforecast.Forecast {
		t.Helper()
		k := forecastKeys[key]
		f, _, err := workloadforecast.New(k[0], k[1], forecastBuckets(variant), receivedAt(variant))
		noErr(t, err)
		return f
	},
	change: func(t *testing.T, f *workloadforecast.Forecast, variant int) {
		t.Helper()
		_, changed, err := f.Receive(f.SiteID(), f.PathID(), forecastBuckets(variant), receivedAt(variant))
		noErr(t, err)
		eq(t, "receipt changed the forecast", changed, true)
	},
	at: func(t *testing.T, key int, version int64) *workloadforecast.Forecast {
		t.Helper()
		k := forecastKeys[key]
		f, err := workloadforecast.Rehydrate(k[0], k[1], forecastBuckets(0), receivedAt(0), version)
		noErr(t, err)
		return f
	},
	fingerprint: func(f *workloadforecast.Forecast) string {
		var b strings.Builder
		fmt.Fprintf(&b, "%s/%s v%d received %s:", f.SiteID(), f.PathID(), f.Version(), instant(f.ReceivedAt()))
		for _, bucket := range f.Buckets() {
			fmt.Fprintf(&b, " %s=%d", instant(bucket.CPT), bucket.Quantity)
		}
		return b.String()
	},
}

// planKeys are the plan identities (site id, shift id, path id): one path in
// two shifts of a site, and in the first shift of a second site.
var planKeys = [][3]string{{siteA, shift1, pathP}, {siteA, shift2, pathP}, {siteB, shift1, pathP}}

// planSpec is variant v's content of a plan: an even variant has a shift
// start (in a non-UTC zone) and heads from the forecast, an odd one neither.
func planSpec(site, shift, path string, v int) labourplan.Spec {
	s := labourplan.Spec{
		SiteID: site, ShiftID: shift, PathID: path, PlannedHeads: 4 + v, RateUnitsPerHour: 37.25,
		Hours: 7.5, InstalledStations: 12, HeadsSource: labourplan.HeadsFromRequest,
	}
	if v%2 == 0 {
		start := T0.Add(time.Duration(v) * time.Hour).In(brt)
		s.ShiftStart, s.HeadsSource = &start, labourplan.HeadsFromForecast
	}
	return s
}

var plans = subject[*labourplan.Plan]{
	get: func(fx Fixture, key int) (*labourplan.Plan, bool, error) {
		k := planKeys[key]
		p, found, err := fx.Plans.Get(ctx, k[0], k[1], k[2])
		if err == nil && found != (p != nil) {
			return nil, false, fmt.Errorf("get returned plan %v with found %v", p, found)
		}
		return p, found, err
	},
	save: func(fx Fixture, p *labourplan.Plan) error { return fx.Plans.Save(ctx, p) },
	first: func(t *testing.T, key, variant int) *labourplan.Plan {
		t.Helper()
		k := planKeys[key]
		p, _, err := labourplan.New(planSpec(k[0], k[1], k[2], variant), T0)
		noErr(t, err)
		return p
	},
	change: func(t *testing.T, p *labourplan.Plan, variant int) {
		t.Helper()
		s := p.Spec()
		_, changed, err := p.Revise(planSpec(s.SiteID, s.ShiftID, s.PathID, variant), T0)
		noErr(t, err)
		eq(t, "revision changed the plan", changed, true)
	},
	at: func(t *testing.T, key int, version int64) *labourplan.Plan {
		t.Helper()
		k := planKeys[key]
		p, err := labourplan.Rehydrate(planSpec(k[0], k[1], k[2], 0), version)
		noErr(t, err)
		return p
	},
	fingerprint: planFingerprint,
}

func planFingerprint(p *labourplan.Plan) string {
	s := p.Spec()
	start := "none"
	if s.ShiftStart != nil {
		start = instant(*s.ShiftStart)
	}
	return fmt.Sprintf("%s/%s/%s v%d: %d of %d heads at %v/h for %vh from %s (%s)", s.SiteID, s.ShiftID, s.PathID,
		p.Version(), s.PlannedHeads, s.InstalledStations, s.RateUnitsPerHour, s.Hours, start, s.HeadsSource)
}
