package usecases

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/workloadforecast"
)

var (
	forecastFirstAt  = time.Date(2026, 11, 19, 5, 0, 0, 0, time.UTC)
	forecastSecondAt = time.Date(2026, 11, 19, 5, 30, 0, 0, time.UTC)
	forecastCPT      = time.Date(2026, 11, 19, 10, 0, 0, 0, time.UTC)
)

// conflictingForecasts answers ErrVersionConflict to the first conflicts
// Saves (a concurrent receipt won the race) and saves for real afterwards.
type conflictingForecasts struct {
	*memory.WorkloadForecastRepo
	conflicts int
	saves     int
}

func (c *conflictingForecasts) Save(ctx context.Context, f *workloadforecast.Forecast) error {
	c.saves++
	if c.saves <= c.conflicts {
		return ports.ErrVersionConflict
	}
	return c.WorkloadForecastRepo.Save(ctx, f)
}

// failingForecastGet fails every Get.
type failingForecastGet struct {
	*memory.WorkloadForecastRepo
	err error
}

func (f failingForecastGet) Get(context.Context, string, string) (*workloadforecast.Forecast, bool, error) {
	return nil, false, f.err
}

type forecastFixture struct {
	repo    *memory.WorkloadForecastRepo
	outbox  *memory.OutboxRepo
	receive *ReceiveWorkloadForecast
	now     time.Time
}

func newForecastFixture() *forecastFixture {
	repo, ob := memory.NewWorkloadForecastRepo(), memory.NewOutboxRepo()
	f := &forecastFixture{repo: repo, outbox: ob, now: forecastFirstAt}
	f.receive = &ReceiveWorkloadForecast{
		Forecasts: repo, Outbox: ob, Encoder: fakeEncoder{}, UnitOfWork: memory.NewUnitOfWork(repo, ob),
		Now: func() time.Time { return f.now },
	}
	return f
}

func forecastCmd(quantities ...int64) ReceiveWorkloadForecastCommand {
	buckets := make([]workloadforecast.Bucket, 0, len(quantities))
	for i, q := range quantities {
		buckets = append(buckets, workloadforecast.Bucket{CPT: forecastCPT.Add(time.Duration(i) * time.Hour), Quantity: q})
	}
	return ReceiveWorkloadForecastCommand{SiteID: "SIM1", PathID: "PICK", Buckets: buckets}
}

func TestReceiveWorkloadForecast_FirstReceiptStoresVersion1AndQueuesTheEvent(t *testing.T) {
	f := newForecastFixture()
	res, err := f.receive.Handle(context.Background(), forecastCmd(120, 80))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Created || !res.Changed || res.Forecast.Version() != 1 || res.Forecast.TotalQuantity() != 200 {
		t.Fatalf("result = %+v (version %d, total %d), want created v1 total 200", res, res.Forecast.Version(), res.Forecast.TotalQuantity())
	}
	stored, found, err := f.repo.Get(context.Background(), "SIM1", "PICK")
	if err != nil || !found || stored.Version() != 1 || !stored.ReceivedAt().Equal(forecastFirstAt) {
		t.Fatalf("stored = %v %v %v", stored, found, err)
	}
	msgs := f.outbox.Messages()
	if len(msgs) != 1 || msgs[0].EventType != workloadforecast.EventWorkloadForecastReceived || msgs[0].Subject != "SIM1/PICK" {
		t.Fatalf("outbox = %+v, want one WorkloadForecastReceived for SIM1/PICK", msgs)
	}
}

func TestReceiveWorkloadForecast_ADifferentReceiptReplacesTheBuckets(t *testing.T) {
	f := newForecastFixture()
	if _, err := f.receive.Handle(context.Background(), forecastCmd(120, 80)); err != nil {
		t.Fatal(err)
	}
	f.now = forecastSecondAt
	res, err := f.receive.Handle(context.Background(), forecastCmd(50))
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || !res.Changed || res.Forecast.Version() != 2 || res.Forecast.TotalQuantity() != 50 {
		t.Fatalf("result = %+v, want a replaced v2 with total 50", res)
	}
	stored, _, _ := f.repo.Get(context.Background(), "SIM1", "PICK")
	if stored.Version() != 2 || len(stored.Buckets()) != 1 || !stored.ReceivedAt().Equal(forecastSecondAt) {
		t.Fatalf("stored = v%d %v at %s", stored.Version(), stored.Buckets(), stored.ReceivedAt())
	}
	if n := len(f.outbox.Messages()); n != 2 {
		t.Fatalf("outbox has %d messages, want 2", n)
	}
}

func TestReceiveWorkloadForecast_AnIdenticalReceiptWritesNothing(t *testing.T) {
	f := newForecastFixture()
	if _, err := f.receive.Handle(context.Background(), forecastCmd(120, 80)); err != nil {
		t.Fatal(err)
	}
	f.now = forecastSecondAt
	res, err := f.receive.Handle(context.Background(), forecastCmd(120, 80))
	if err != nil {
		t.Fatal(err)
	}
	if res.Created || res.Changed || res.Forecast.Version() != 1 || !res.Forecast.ReceivedAt().Equal(forecastFirstAt) {
		t.Fatalf("result = %+v, want an unchanged v1 received at the first time", res)
	}
	if n := len(f.outbox.Messages()); n != 1 {
		t.Fatalf("outbox has %d messages, want only the first receipt's", n)
	}
}

func TestReceiveWorkloadForecast_AnInvalidReceiptIsRefusedAndStoresNothing(t *testing.T) {
	cases := map[string]struct {
		cmd  ReceiveWorkloadForecastCommand
		want error
	}{
		"no buckets":        {ReceiveWorkloadForecastCommand{SiteID: "SIM1", PathID: "PICK"}, workloadforecast.ErrNoBuckets},
		"negative quantity": {forecastCmd(10, -1), workloadforecast.ErrNegativeQuantity},
		"missing cpt":       {ReceiveWorkloadForecastCommand{SiteID: "SIM1", PathID: "PICK", Buckets: []workloadforecast.Bucket{{Quantity: 1}}}, workloadforecast.ErrMissingCPT},
		"blank site":        {ReceiveWorkloadForecastCommand{PathID: "PICK", Buckets: forecastCmd(1).Buckets}, workloadforecast.ErrRequiredField},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := newForecastFixture()
			if _, err := f.receive.Handle(context.Background(), tc.cmd); !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if _, found, _ := f.repo.Get(context.Background(), tc.cmd.SiteID, tc.cmd.PathID); found {
				t.Fatal("an invalid first receipt was stored")
			}
			if n := len(f.outbox.Messages()); n != 0 {
				t.Fatalf("outbox has %d messages, want 0", n)
			}
		})
	}
}

func TestReceiveWorkloadForecast_AnInvalidReplacementKeepsTheStoredForecast(t *testing.T) {
	f := newForecastFixture()
	if _, err := f.receive.Handle(context.Background(), forecastCmd(120)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receive.Handle(context.Background(), forecastCmd(-5)); !errors.Is(err, workloadforecast.ErrNegativeQuantity) {
		t.Fatalf("err = %v, want ErrNegativeQuantity", err)
	}
	stored, _, _ := f.repo.Get(context.Background(), "SIM1", "PICK")
	if stored.Version() != 1 || stored.TotalQuantity() != 120 || len(f.outbox.Messages()) != 1 {
		t.Fatalf("stored v%d total %d, outbox %d", stored.Version(), stored.TotalQuantity(), len(f.outbox.Messages()))
	}
}

func TestReceiveWorkloadForecast_OneVersionConflictIsRetried(t *testing.T) {
	f := newForecastFixture()
	racing := &conflictingForecasts{WorkloadForecastRepo: f.repo, conflicts: 1}
	f.receive.Forecasts = racing
	res, err := f.receive.Handle(context.Background(), forecastCmd(120))
	if err != nil {
		t.Fatal(err)
	}
	if racing.saves != 2 || res.Forecast.Version() != 1 || len(f.outbox.Messages()) != 1 {
		t.Fatalf("saves %d, version %d, outbox %d; want 2 saves, v1, one message", racing.saves, res.Forecast.Version(), len(f.outbox.Messages()))
	}
}

func TestReceiveWorkloadForecast_ASecondVersionConflictIsReturned(t *testing.T) {
	f := newForecastFixture()
	racing := &conflictingForecasts{WorkloadForecastRepo: f.repo, conflicts: 2}
	f.receive.Forecasts = racing
	if _, err := f.receive.Handle(context.Background(), forecastCmd(120)); !errors.Is(err, ports.ErrVersionConflict) {
		t.Fatalf("err = %v, want ErrVersionConflict", err)
	}
	if racing.saves != 2 || len(f.outbox.Messages()) != 0 {
		t.Fatalf("saves %d, outbox %d; want 2 attempts and nothing queued", racing.saves, len(f.outbox.Messages()))
	}
}

func TestReceiveWorkloadForecast_AnEncoderOrOutboxFailureRollsTheSaveBack(t *testing.T) {
	errBoom := errors.New("boom")
	for name, mutate := range map[string]func(*forecastFixture){
		"encoder": func(f *forecastFixture) { f.receive.Encoder = fakeEncoder{err: errBoom} },
		"outbox":  func(f *forecastFixture) { f.receive.Outbox = failingOutbox{err: errBoom} },
	} {
		t.Run(name, func(t *testing.T) {
			f := newForecastFixture()
			mutate(f)
			if _, err := f.receive.Handle(context.Background(), forecastCmd(120)); !errors.Is(err, errBoom) {
				t.Fatalf("err = %v, want boom", err)
			}
			if _, found, _ := f.repo.Get(context.Background(), "SIM1", "PICK"); found {
				t.Fatal("the forecast was stored without its event")
			}
		})
	}
}

func TestReceiveWorkloadForecast_AStoreReadFailureIsReturned(t *testing.T) {
	errBoom := errors.New("boom")
	f := newForecastFixture()
	f.receive.Forecasts = failingForecastGet{WorkloadForecastRepo: f.repo, err: errBoom}
	if _, err := f.receive.Handle(context.Background(), forecastCmd(120)); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want boom", err)
	}
}

func TestReceiveWorkloadForecast_DefaultsTheClockToUTCNow(t *testing.T) {
	f := newForecastFixture()
	f.receive.Now = nil
	before := time.Now().UTC().Add(-time.Second)
	res, err := f.receive.Handle(context.Background(), forecastCmd(1))
	if err != nil {
		t.Fatal(err)
	}
	if at := res.Forecast.ReceivedAt(); at.Before(before) || at.Location() != time.UTC {
		t.Fatalf("received_at = %s, want a UTC now", at)
	}
}

func TestGetWorkloadForecast(t *testing.T) {
	f := newForecastFixture()
	get := &GetWorkloadForecast{Forecasts: f.repo}
	if _, err := get.Handle(context.Background(), "SIM1", "PICK"); !errors.Is(err, ErrWorkloadForecastNotFound) {
		t.Fatalf("err = %v, want ErrWorkloadForecastNotFound", err)
	}
	if _, err := f.receive.Handle(context.Background(), forecastCmd(7)); err != nil {
		t.Fatal(err)
	}
	got, err := get.Handle(context.Background(), "SIM1", "PICK")
	if err != nil || got.TotalQuantity() != 7 {
		t.Fatalf("got %v, %v", got, err)
	}
	errBoom := errors.New("boom")
	get.Forecasts = failingForecastGet{err: errBoom}
	if _, err := get.Handle(context.Background(), "SIM1", "PICK"); !errors.Is(err, errBoom) {
		t.Fatalf("err = %v, want boom", err)
	}
}
