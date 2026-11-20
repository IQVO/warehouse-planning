package postgres

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// The JSONB storage format of workload_forecasts.buckets keeps the order, the
// instant to the nanosecond and every quantity, zero included; the
// database-backed path is covered by the repository contract under
// -tags=integration.
func TestBucketsJSONRoundTrip(t *testing.T) {
	brt := time.FixedZone("BRT", -3*60*60)
	buckets := []workloadforecast.Bucket{
		{CPT: time.Date(2026, 11, 18, 15, 0, 0, 0, brt), Quantity: 120},
		{CPT: time.Date(2026, 11, 18, 8, 0, 0, 123456789, time.UTC), Quantity: 0},
	}
	raw, err := encodeBuckets(buckets)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeBuckets(raw)
	if err != nil || len(got) != len(buckets) {
		t.Fatalf("decode = %+v, %v; want %+v", got, err, buckets)
	}
	for i, b := range buckets {
		if !got[i].CPT.Equal(b.CPT) || got[i].Quantity != b.Quantity {
			t.Fatalf("bucket %d = %+v, want %+v", i, got[i], b)
		}
	}
	const want = `[{"cpt":"2026-11-18T15:00:00-03:00","quantity":120},{"cpt":"2026-11-18T08:00:00.123456789Z","quantity":0}]`
	if string(raw) != want {
		t.Fatalf("encoded = %s, want %s", raw, want)
	}
}

// A stored bucket missing a field is an error, never a silent zero; JSON
// that is not an array of buckets is an error too.
func TestBucketsJSON_CorruptRowsAreErrors(t *testing.T) {
	for name, raw := range map[string]string{
		"no quantity": `[{"cpt":"2026-11-18T08:00:00Z"}]`,
		"no cpt":      `[{"quantity":3}]`,
		"null fields": `[{"cpt":null,"quantity":null}]`,
	} {
		if _, err := decodeBuckets([]byte(raw)); !errors.Is(err, errCorruptBucket) {
			t.Errorf("%s: err = %v, want errCorruptBucket", name, err)
		}
	}
	for name, raw := range map[string]string{
		"an object":         `{"cpt":"2026-11-18T08:00:00Z","quantity":3}`,
		"a string quantity": `[{"cpt":"2026-11-18T08:00:00Z","quantity":"3"}]`,
		"not a time":        `[{"cpt":"tomorrow","quantity":3}]`,
	} {
		if _, err := decodeBuckets([]byte(raw)); err == nil || errors.Is(err, errCorruptBucket) {
			t.Errorf("%s: err = %v, want a decode error", name, err)
		}
	}
	got, err := decodeBuckets([]byte(`[]`))
	if err != nil || got == nil || len(got) != 0 {
		t.Fatalf("decode([]) = %#v, %v; want an empty slice (Rehydrate refuses it)", got, err)
	}
}
