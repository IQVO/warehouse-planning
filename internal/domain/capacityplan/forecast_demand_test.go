package capacityplan

import (
	"errors"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

func TestParseDemandSource(t *testing.T) {
	for _, raw := range []string{"", "request", "orders", "forecast"} {
		got, err := ParseDemandSource(raw)
		if err != nil || string(got) != raw {
			t.Errorf("ParseDemandSource(%q) = %q, %v; want it back unchanged", raw, got, err)
		}
	}
	for _, raw := range []string{"FORECAST", "inbound", " request", "x"} {
		if got, err := ParseDemandSource(raw); !errors.Is(err, ErrUnknownDemandSource) || got != "" {
			t.Errorf("ParseDemandSource(%q) = %q, %v; want ErrUnknownDemandSource", raw, got, err)
		}
	}
}

func TestValidateForecastFactor(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	cases := []struct {
		name string
		in   *float64
		want error
	}{
		{"absent", nil, ErrMissingUnitsPerOrder},
		{"zero", f(0), processcapacity.ErrNonPositiveConversionFactor},
		{"negative", f(-2), processcapacity.ErrNonPositiveConversionFactor},
		{"a part unit per order", f(0.5), nil},
		{"several units per order", f(2.5), nil},
	}
	for _, c := range cases {
		if err := ValidateForecastFactor(c.in); !errors.Is(err, c.want) || (c.want == nil && err != nil) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
	}
}

// Forecast units become ORDERS rounded up: a part order is a whole order to
// serve, and a quotient that is whole in decimal stays whole in float64.
func TestForecastOrders_RoundsUpToWholeOrders(t *testing.T) {
	cases := []struct {
		units int64
		upo   float64
		want  float64
	}{
		{0, 2.5, 0},
		{100, 2.5, 40},
		{101, 2.5, 41},
		{10, 3, 4},
		{9, 3, 3},
		{1, 3, 1},
		{7, 0.7, 10}, // 10.000000000000002 in float64
		{3, 0.1, 30}, // 29.999999999999996 in float64
		{1, 0.5, 2},
		{8500, 1, 8500},
	}
	for _, c := range cases {
		got := ForecastOrders(c.units, c.upo)
		if got != c.want {
			t.Errorf("ForecastOrders(%d, %v) = %v, want %v", c.units, c.upo, got, c.want)
		}
	}
	if got := ForecastOrders(0, 2.5); got != 0 || 1/got < 0 {
		t.Errorf("zero units must be +0 orders (never -0 in JSON), got %v", got)
	}
}

func TestCreate_KeepsTheForecastPathOnlyForTheForecastSource(t *testing.T) {
	rate, bottleneck := phase2PathCapacity(t)
	build := func(source DemandSource) *CapacityPlan {
		plan, err := Create(CreateParams{
			ID: planID, WarehouseID: warehouseID, SiteID: siteID, Location: location,
			Window: mustWindow(t, windowStart, windowEnd), ProcessPathID: pathID,
			AssignedDemand: 40, PathRate: rate, BottleneckStep: bottleneck,
			DemandSource: source, ForecastPathID: "PICK",
		}, createdAt)
		if err != nil {
			t.Fatal(err)
		}
		return plan
	}
	if got := build(DemandSourceForecast); got.ForecastPathID() != "PICK" || got.DemandSource() != DemandSourceForecast {
		t.Fatalf("forecast plan = path %q source %q", got.ForecastPathID(), got.DemandSource())
	}
	for _, source := range []DemandSource{"", DemandSourceRequest, DemandSourceOrders} {
		if got := build(source).ForecastPathID(); got != "" {
			t.Errorf("a %q plan read no forecast but keeps forecast path %q", source, got)
		}
	}
	if got := Rehydrate(RehydrateParams{ID: planID, DemandSource: DemandSourceForecast, ForecastPathID: "PICK"}); got.ForecastPathID() != "PICK" {
		t.Fatalf("Rehydrate lost the forecast path: %q", got.ForecastPathID())
	}
}
