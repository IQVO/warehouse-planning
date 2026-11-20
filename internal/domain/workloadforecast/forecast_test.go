package workloadforecast_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

var (
	t0       = time.Date(2026, 11, 20, 6, 0, 0, 0, time.UTC)
	received = time.Date(2026, 11, 20, 5, 30, 0, 0, time.UTC)
)

func buckets(qty ...int64) []workloadforecast.Bucket {
	out := make([]workloadforecast.Bucket, len(qty))
	for i, q := range qty {
		out[i] = workloadforecast.Bucket{CPT: t0.Add(time.Duration(i) * time.Hour), Quantity: q}
	}
	return out
}

func TestNew_ValidReceiptIsVersionOneWithItsEvent(t *testing.T) {
	f, ev, err := workloadforecast.New("SITE-GRU-01", "PICK", buckets(100, 0, 250), received)
	if err != nil {
		t.Fatal(err)
	}
	if f.SiteID() != "SITE-GRU-01" || f.PathID() != "PICK" || f.Version() != 1 || !f.ReceivedAt().Equal(received) {
		t.Fatalf("forecast = %s %s v%d at %v", f.SiteID(), f.PathID(), f.Version(), f.ReceivedAt())
	}
	if f.TotalQuantity() != 350 || len(f.Buckets()) != 3 {
		t.Fatalf("total %d buckets %d, want 350 and 3", f.TotalQuantity(), len(f.Buckets()))
	}
	want := workloadforecast.Received{SiteID: "SITE-GRU-01", PathID: "PICK", TotalQuantity: 350, BucketCount: 3, ReceivedAt: received, Version: 1}
	if ev != want {
		t.Fatalf("event = %+v, want %+v", ev, want)
	}
	if ev.EventName() != "WorkloadForecastReceived" || ev.AggregateID() != "SITE-GRU-01/PICK" || !ev.OccurredAt().Equal(received) {
		t.Fatalf("event header = %s %s %v", ev.EventName(), ev.AggregateID(), ev.OccurredAt())
	}
}

func TestNew_RejectsEveryBrokenInvariant(t *testing.T) {
	zeroCPT := []workloadforecast.Bucket{{CPT: t0, Quantity: 1}, {Quantity: 1}}
	cases := []struct {
		name       string
		site, path string
		bs         []workloadforecast.Bucket
		at         time.Time
		want       error
	}{
		{"blank site", "", "PICK", buckets(1), received, workloadforecast.ErrRequiredField},
		{"blank path", "S1", "", buckets(1), received, workloadforecast.ErrRequiredField},
		{"zero receipt time", "S1", "PICK", buckets(1), time.Time{}, workloadforecast.ErrRequiredField},
		{"no buckets", "S1", "PICK", nil, received, workloadforecast.ErrNoBuckets},
		{"bucket without cpt", "S1", "PICK", zeroCPT, received, workloadforecast.ErrMissingCPT},
		{"negative quantity", "S1", "PICK", buckets(5, -1), received, workloadforecast.ErrNegativeQuantity},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := workloadforecast.New(c.site, c.path, c.bs, c.at); !errors.Is(err, c.want) {
				t.Fatalf("New error = %v, want %v", err, c.want)
			}
			if _, err := workloadforecast.Rehydrate(c.site, c.path, c.bs, c.at, 3); !errors.Is(err, c.want) {
				t.Fatalf("Rehydrate error = %v, want %v", err, c.want)
			}
		})
	}
}

func TestNew_ZeroQuantityBucketIsVolumeNotAnError(t *testing.T) {
	if _, _, err := workloadforecast.New("S1", "PICK", buckets(0), received); err != nil {
		t.Fatalf("a zero-quantity bucket is valid data: %v", err)
	}
}

func TestNew_CopiesTheCallersBuckets(t *testing.T) {
	in := buckets(10)
	f, _, err := workloadforecast.New("S1", "PICK", in, received)
	if err != nil {
		t.Fatal(err)
	}
	in[0].Quantity = 999
	out := f.Buckets()
	out[0].Quantity = 777
	if f.TotalQuantity() != 10 {
		t.Fatalf("forecast shares a slice with its caller: total %d", f.TotalQuantity())
	}
}

func TestRehydrate_VersionMustBeAtLeastOne(t *testing.T) {
	f, err := workloadforecast.Rehydrate("S1", "PICK", buckets(4), received, 1)
	if err != nil || f.Version() != 1 || f.TotalQuantity() != 4 {
		t.Fatalf("version 1 = %v, %v", f, err)
	}
	if _, err := workloadforecast.Rehydrate("S1", "PICK", buckets(4), received, 0); !errors.Is(err, workloadforecast.ErrInvalidVersion) {
		t.Fatalf("version 0 error = %v, want ErrInvalidVersion", err)
	}
	if _, err := workloadforecast.Rehydrate("S1", "PICK", buckets(4), received, -1); !errors.Is(err, workloadforecast.ErrInvalidVersion) {
		t.Fatalf("version -1 error = %v, want ErrInvalidVersion", err)
	}
}

func TestReceive_NewBucketsReplaceTheForecastAndBumpTheVersion(t *testing.T) {
	f, err := workloadforecast.Rehydrate("S1", "PICK", buckets(100, 200), received, 4)
	if err != nil {
		t.Fatal(err)
	}
	later := received.Add(time.Hour)
	ev, changed, err := f.Receive("S1", "PICK", buckets(30), later)
	if err != nil || !changed {
		t.Fatalf("Receive = changed %v, %v", changed, err)
	}
	want := workloadforecast.Received{SiteID: "S1", PathID: "PICK", TotalQuantity: 30, BucketCount: 1, ReceivedAt: later, Version: 5}
	if ev != want || f.Version() != 5 || f.TotalQuantity() != 30 || !f.ReceivedAt().Equal(later) {
		t.Fatalf("after Receive: event %+v, forecast v%d total %d at %v", ev, f.Version(), f.TotalQuantity(), f.ReceivedAt())
	}
}

func TestReceive_EqualBucketsChangeNothing(t *testing.T) {
	f, err := workloadforecast.Rehydrate("S1", "PICK", buckets(100, 200), received, 2)
	if err != nil {
		t.Fatal(err)
	}
	ev, changed, err := f.Receive("S1", "PICK", buckets(100, 200), received.Add(time.Hour))
	if err != nil || changed || ev != (workloadforecast.Received{}) {
		t.Fatalf("an identical receipt = changed %v, event %+v, %v", changed, ev, err)
	}
	if f.Version() != 2 || !f.ReceivedAt().Equal(received) {
		t.Fatalf("an identical receipt moved the forecast: v%d at %v", f.Version(), f.ReceivedAt())
	}
}

func TestReceive_SameQuantitiesAtAnotherCPTIsAChange(t *testing.T) {
	f, err := workloadforecast.Rehydrate("S1", "PICK", buckets(100), received, 1)
	if err != nil {
		t.Fatal(err)
	}
	moved := []workloadforecast.Bucket{{CPT: t0.Add(time.Minute), Quantity: 100}}
	if _, changed, err := f.Receive("S1", "PICK", moved, received); err != nil || !changed {
		t.Fatalf("a moved CPT = changed %v, %v", changed, err)
	}
}

func TestReceive_RejectsAnotherIdentityAndInvalidReceipts(t *testing.T) {
	f, err := workloadforecast.Rehydrate("S1", "PICK", buckets(1), received, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := f.Receive("S2", "PICK", buckets(2), received); !errors.Is(err, workloadforecast.ErrIdentityMismatch) {
		t.Fatalf("other site error = %v", err)
	}
	if _, _, err := f.Receive("S1", "PACK", buckets(2), received); !errors.Is(err, workloadforecast.ErrIdentityMismatch) {
		t.Fatalf("other path error = %v", err)
	}
	if _, _, err := f.Receive("S1", "PICK", nil, received); !errors.Is(err, workloadforecast.ErrNoBuckets) {
		t.Fatalf("empty receipt error = %v", err)
	}
	if f.Version() != 1 || f.TotalQuantity() != 1 {
		t.Fatalf("a refused receipt changed the forecast: v%d total %d", f.Version(), f.TotalQuantity())
	}
}

func TestQuantityDueIn_IsStartInclusiveEndExclusive(t *testing.T) {
	// buckets at t0, t0+1h, t0+2h, t0+3h.
	f, _, err := workloadforecast.New("S1", "PICK", buckets(10, 20, 40, 80), received)
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name        string
		from, to    time.Time
		units       int64
		bucketCount int
	}{
		{"exact bounds", t0.Add(time.Hour), t0.Add(3 * time.Hour), 60, 2},
		{"one second before the start", t0.Add(time.Hour - time.Second), t0.Add(3 * time.Hour), 60, 2},
		{"one second after the start", t0.Add(time.Hour + time.Second), t0.Add(3 * time.Hour), 40, 1},
		{"one second after the end", t0.Add(time.Hour), t0.Add(3*time.Hour + time.Second), 140, 3},
		{"whole forecast", t0, t0.Add(4 * time.Hour), 150, 4},
		{"no bucket in the window", t0.Add(10 * time.Hour), t0.Add(12 * time.Hour), 0, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			units, n := f.QuantityDueIn(c.from, c.to)
			if units != c.units || n != c.bucketCount {
				t.Fatalf("QuantityDueIn = %d units in %d buckets, want %d in %d", units, n, c.units, c.bucketCount)
			}
		})
	}
}
