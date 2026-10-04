package memory_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/application/tally"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var (
	_ ports.StationStandardRepository = (*memory.StationStandardRepo)(nil)
	_ ports.StorageTallyRepository    = (*memory.StorageTallyRepo)(nil)
	_ ports.StorageTallyReader        = (*memory.StorageTallyRepo)(nil)
)

func standard(t *testing.T, location string, process processcapacity.ProcessType, qty float64) processcapacity.StationStandard {
	t.Helper()
	rate, err := processcapacity.NewCapacityRate(qty, processcapacity.UnitPackage, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	s, err := processcapacity.NewStationStandard(location, process, rate)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestStationStandardRepo_SaveFindListOrdered(t *testing.T) {
	ctx := context.Background()
	repo := memory.NewStationStandardRepo()
	if got, err := repo.Find(ctx, "SIM1", "PACK"); err != nil || got != nil {
		t.Fatalf("Find on empty = %v, %v, want nil, nil", got, err)
	}
	for _, s := range []processcapacity.StationStandard{
		standard(t, "SIM2", "PACK", 75), standard(t, "SIM1", "SORT", 90), standard(t, "SIM1", "PACK", 180),
	} {
		if err := repo.Save(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	// Upsert replaces.
	if err := repo.Save(ctx, standard(t, "SIM1", "PACK", 200)); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Find(ctx, "SIM1", "PACK")
	if err != nil || got == nil || got.PerStation().Quantity() != 200 {
		t.Fatalf("Find = %v, %v, want 200", got, err)
	}

	key := func(l []processcapacity.StationStandard) []string {
		var out []string
		for _, s := range l {
			out = append(out, s.Location()+"/"+string(s.ProcessType()))
		}
		return out
	}
	site, _ := repo.List(ctx, "SIM1")
	if want := []string{"SIM1/PACK", "SIM1/SORT"}; !reflect.DeepEqual(key(site), want) {
		t.Fatalf("List(SIM1) = %v, want %v", key(site), want)
	}
	all, _ := repo.List(ctx, "")
	if want := []string{"SIM1/PACK", "SIM1/SORT", "SIM2/PACK"}; !reflect.DeepEqual(key(all), want) {
		t.Fatalf("List(all) = %v, want %v", key(all), want)
	}
	none, _ := repo.List(ctx, "NOWHERE")
	if none == nil || len(none) != 0 {
		t.Fatalf("List(NOWHERE) = %v, want an empty non-nil slice", none)
	}
}

func seedTally(t *testing.T, repo *memory.StorageTallyRepo) {
	t.Helper()
	ctx := context.Background()
	regs := []struct{ code, zone, typ, key string }{
		{"a1", "SIM1-OPS-WC", tally.TypeStation, "PACK"}, {"a2", "SIM1-OPS-WC", tally.TypeStation, "PACK"},
		{"a3", "SIM1-OPS-WC2", tally.TypeStation, "PACK"}, {"a4", "SIM1-OPS-WC", tally.TypeStation, "SORT"},
		{"b1", "SIM1-STOR-AMB", tally.TypeLocation, "SimShelf"}, {"b2", "SIM1-STOR-AMB", tally.TypeLocation, "SimShelf"},
		{"c1", "SIM10-OPS-WC", tally.TypeStation, "PACK"}, {"d1", "SIM2-OPS-WC", tally.TypeStation, "PACK"},
		{"e1", "SIM1-GONE", tally.TypeStation, "PACK"},
	}
	for _, r := range regs {
		if _, err := repo.RegisterSlot(ctx, r.code, r.zone, r.typ, []string{r.key}); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := repo.DecommissionSlot(ctx, "e1"); err != nil { // leaves a zero-count bucket
		t.Fatal(err)
	}
}

func TestStorageTallyRepo_StationCountSumsTheSitesZonesByActivity(t *testing.T) {
	repo := memory.NewStorageTallyRepo()
	seedTally(t, repo)
	ctx := context.Background()
	for _, tc := range []struct {
		location, activity string
		want               int
	}{
		{"SIM1", "PACK", 3}, // WC (2) + WC2 (1); not SIM10/SIM2, not the decommissioned zone
		{"SIM1", "SORT", 1},
		{"SIM1", "QC", 0},
		{"SIM10", "PACK", 1},
		{"SIM", "PACK", 0},      // a prefix of the site code is not the site
		{"SIM1", "SimShelf", 0}, // a LOCATION bucket is not a STATION bucket
	} {
		got, err := repo.StationCount(ctx, tc.location, tc.activity)
		if err != nil || got != tc.want {
			t.Errorf("StationCount(%s, %s) = %d, %v, want %d", tc.location, tc.activity, got, err, tc.want)
		}
	}
}

func TestStorageTallyRepo_SiteBucketsAreOrderedAndSkipZeroCounts(t *testing.T) {
	repo := memory.NewStorageTallyRepo()
	seedTally(t, repo)
	got, err := repo.SiteBuckets(context.Background(), "SIM1")
	if err != nil {
		t.Fatal(err)
	}
	want := []tally.Bucket{
		{ZoneID: "SIM1-OPS-WC", TallyType: tally.TypeStation, TallyKey: "PACK", Count: 2},
		{ZoneID: "SIM1-OPS-WC", TallyType: tally.TypeStation, TallyKey: "SORT", Count: 1},
		{ZoneID: "SIM1-OPS-WC2", TallyType: tally.TypeStation, TallyKey: "PACK", Count: 1},
		{ZoneID: "SIM1-STOR-AMB", TallyType: tally.TypeLocation, TallyKey: "SimShelf", Count: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SiteBuckets = %v, want %v", got, want)
	}
	none, _ := repo.SiteBuckets(context.Background(), "NOWHERE")
	if none == nil || len(none) != 0 {
		t.Fatalf("SiteBuckets(NOWHERE) = %v, want an empty non-nil slice", none)
	}
}
