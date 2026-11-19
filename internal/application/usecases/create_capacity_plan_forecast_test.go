package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

// newForecastPlanFixture is the plan fixture (8000 orders of capacity over
// the 8h window, units_per_order 2.5) with a workload forecast store wired.
func newForecastPlanFixture(t *testing.T) (*planFixture, *memory.WorkloadForecastRepo) {
	t.Helper()
	f := newPlanFixture(t)
	repo := memory.NewWorkloadForecastRepo()
	f.create.Forecasts = repo
	return f, repo
}

func seedForecast(t *testing.T, repo *memory.WorkloadForecastRepo, site, path string, buckets ...workloadforecast.Bucket) {
	t.Helper()
	fc, _, err := workloadforecast.New(site, path, buckets, planCreatedAt)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Save(context.Background(), fc); err != nil {
		t.Fatal(err)
	}
}

func bucket(at time.Time, units int64) workloadforecast.Bucket {
	return workloadforecast.Bucket{CPT: at, Quantity: units}
}

func forecastCommand() CreateCapacityPlanCommand {
	cmd := omittedDemandCommand()
	cmd.DemandSource = "forecast"
	return cmd
}

// The window rule of ADR 0003/0004: a bucket on window_start is in, a bucket
// on window_end is out; the units over units_per_order are rounded UP.
func TestCreateCapacityPlan_ForecastDemandSumsTheBucketsInTheWindowAndRoundsUp(t *testing.T) {
	f, repo := newForecastPlanFixture(t)
	seedForecast(t, repo, "SIM1", "PICK-REBIN-PACK",
		bucket(planWindowStart, 20000),               // on window_start: in
		bucket(planWindowStart.Add(3*time.Hour), 3),  // in: 20003 units / 2.5 = 8001.2 -> 8002
		bucket(planWindowEnd, 999),                   // on window_end: out
		bucket(planWindowStart.Add(-time.Second), 7), // before: out
	)
	plan, err := f.create.Handle(context.Background(), forecastCommand())
	if err != nil {
		t.Fatal(err)
	}
	if plan.AssignedDemand() != 8002 || plan.Shortage() != 2 {
		t.Fatalf("demand %v shortage %v, want 8002 orders (ceil 20003/2.5) short by 2", plan.AssignedDemand(), plan.Shortage())
	}
	if plan.DemandSource() != capacityplan.DemandSourceForecast || plan.ForecastPathID() != "PICK-REBIN-PACK" {
		t.Fatalf("source %q forecast path %q", plan.DemandSource(), plan.ForecastPathID())
	}
	stored, _ := f.plans.FindByID(context.Background(), plan.ID())
	if stored == nil || stored.DemandSource() != capacityplan.DemandSourceForecast || stored.ForecastPathID() != "PICK-REBIN-PACK" {
		t.Fatalf("the stored plan lost its forecast provenance: %+v", stored)
	}
}

// forecast_path_id names another path's forecast (a wes path such as PICK);
// it is canonicalised, and the plan's own path forecast is not read.
func TestCreateCapacityPlan_ForecastPathIDOtherThanThePlanPath(t *testing.T) {
	f, repo := newForecastPlanFixture(t)
	seedForecast(t, repo, "SIM1", "PICK", bucket(planWindowStart.Add(time.Hour), 100))
	seedForecast(t, repo, "SIM1", "PICK-REBIN-PACK", bucket(planWindowStart.Add(time.Hour), 5000))
	seedForecast(t, repo, "SIM2", "PICK", bucket(planWindowStart.Add(time.Hour), 7000))
	cmd := forecastCommand()
	cmd.ForecastPathID = " pick "
	plan, err := f.create.Handle(context.Background(), cmd)
	if err != nil {
		t.Fatal(err)
	}
	if plan.AssignedDemand() != 40 || plan.ForecastPathID() != "PICK" || plan.ProcessPathID() != "pick-rebin-pack" {
		t.Fatalf("demand %v forecast path %q path %q, want 40 orders from PICK at SIM1", plan.AssignedDemand(), plan.ForecastPathID(), plan.ProcessPathID())
	}
}

func TestCreateCapacityPlan_ForecastWithoutDataIsMissingAssignedDemand(t *testing.T) {
	cases := map[string]func(*planFixture, *memory.WorkloadForecastRepo){
		"no forecast for the path": func(*planFixture, *memory.WorkloadForecastRepo) {},
		"buckets only outside the window, one on its end": func(_ *planFixture, r *memory.WorkloadForecastRepo) {
			seedForecast(t, r, "SIM1", "PICK-REBIN-PACK", bucket(planWindowEnd, 10), bucket(planWindowStart.Add(-time.Hour), 10))
		},
		"a forecast only at another site": func(_ *planFixture, r *memory.WorkloadForecastRepo) {
			seedForecast(t, r, "SIM2", "PICK-REBIN-PACK", bucket(planWindowStart, 10))
		},
		"no forecast store wired": func(f *planFixture, _ *memory.WorkloadForecastRepo) { f.create.Forecasts = nil },
	}
	for name, arrange := range cases {
		t.Run(name, func(t *testing.T) {
			f, repo := newForecastPlanFixture(t)
			arrange(f, repo)
			_, err := f.create.Handle(context.Background(), forecastCommand())
			if !errors.Is(err, ErrMissingAssignedDemand) {
				t.Fatalf("err = %v, want ErrMissingAssignedDemand (never zero demand)", err)
			}
			if len(f.outbox.Messages()) != 0 {
				t.Fatal("a rejected plan queued events")
			}
		})
	}
}

// A zero-unit bucket in the window is data: zero demand, not missing demand.
func TestCreateCapacityPlan_AZeroBucketInTheWindowIsZeroDemand(t *testing.T) {
	f, repo := newForecastPlanFixture(t)
	seedForecast(t, repo, "SIM1", "PICK-REBIN-PACK", bucket(planWindowStart.Add(time.Hour), 0))
	plan, err := f.create.Handle(context.Background(), forecastCommand())
	if err != nil || plan.AssignedDemand() != 0 {
		t.Fatalf("plan %v, err %v; want a zero-demand plan", plan, err)
	}
}

func TestCreateCapacityPlan_ForecastNeedsAPositiveUnitsPerOrder(t *testing.T) {
	f, repo := newForecastPlanFixture(t)
	seedForecast(t, repo, "SIM1", "PICK-REBIN-PACK", bucket(planWindowStart, 10))
	cmd := forecastCommand()
	cmd.UnitsPerOrder = nil
	if _, err := f.create.Handle(context.Background(), cmd); !errors.Is(err, capacityplan.ErrMissingUnitsPerOrder) {
		t.Fatalf("err = %v, want ErrMissingUnitsPerOrder", err)
	}
	cmd.UnitsPerOrder = f64ptr(0)
	if _, err := f.create.Handle(context.Background(), cmd); !errors.Is(err, processcapacity.ErrNonPositiveConversionFactor) {
		t.Fatalf("err = %v, want ErrNonPositiveConversionFactor", err)
	}
	// Checked before the forecast is read: no data does not hide it.
	f.create.Forecasts = nil
	cmd.UnitsPerOrder = nil
	if _, err := f.create.Handle(context.Background(), cmd); !errors.Is(err, capacityplan.ErrMissingUnitsPerOrder) {
		t.Fatalf("err = %v, want ErrMissingUnitsPerOrder before any lookup", err)
	}
}

func TestCreateCapacityPlan_ANamedSourceMustAgreeWithTheInputs(t *testing.T) {
	f, repo := newDemandPlanFixture(t)
	seedOrders(t, repo, "PATH-ZONE-A", 10, planWindowStart.Add(time.Hour))
	cases := []struct {
		name   string
		mutate func(*CreateCapacityPlanCommand)
		want   error
	}{
		{"unknown source", func(c *CreateCapacityPlanCommand) { c.DemandSource = "inbound" }, capacityplan.ErrUnknownDemandSource},
		{"request without assigned_demand", func(c *CreateCapacityPlanCommand) { c.DemandSource = "request"; c.DemandFromOrders = true }, ErrMissingAssignedDemand},
		{"orders with a stated assigned_demand", func(c *CreateCapacityPlanCommand) { c.DemandSource = "orders" }, capacityplan.ErrDemandSourceConflict},
		{"forecast with a stated assigned_demand", func(c *CreateCapacityPlanCommand) { c.DemandSource = "forecast" }, capacityplan.ErrDemandSourceConflict},
		{"forecast_path_id with the default source", func(c *CreateCapacityPlanCommand) { c.ForecastPathID = "PICK" }, capacityplan.ErrDemandSourceConflict},
		{"forecast_path_id with orders", func(c *CreateCapacityPlanCommand) {
			c.DemandSource, c.DemandFromOrders, c.ForecastPathID = "orders", true, "PICK"
		}, capacityplan.ErrDemandSourceConflict},
	}
	for _, c := range cases {
		cmd := planCommand(100)
		c.mutate(&cmd)
		if _, err := f.create.Handle(context.Background(), cmd); !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
	}
	if len(f.outbox.Messages()) != 0 {
		t.Fatal("a refused request queued events")
	}
}

func TestCreateCapacityPlan_NamedSourcesBehaveAsTheirDefaults(t *testing.T) {
	f, repo := newDemandPlanFixture(t)
	seedOrders(t, repo, "PATH-ZONE-A", 10, planWindowStart.Add(time.Hour))
	stated := planCommand(100)
	stated.DemandSource = "request"
	plan, err := f.create.Handle(context.Background(), stated)
	if err != nil || plan.AssignedDemand() != 100 || plan.DemandSource() != capacityplan.DemandSourceRequest || plan.ForecastPathID() != "" {
		t.Fatalf("request: plan %v err %v", plan, err)
	}
	orders := omittedDemandCommand()
	orders.DemandSource = "orders"
	plan, err = f.create.Handle(context.Background(), orders)
	if err != nil || plan.AssignedDemand() != 10 || plan.DemandSource() != capacityplan.DemandSourceOrders || plan.ForecastPathID() != "" {
		t.Fatalf("orders: plan %v err %v", plan, err)
	}
}
