package usecases

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/memory"
	"github.com/claudioed/warehouse-planning/internal/domain/capacityplan"
	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
	"github.com/claudioed/warehouse-planning/internal/domain/processcapacity"
)

var inboundAsOf = time.Date(2026, 10, 4, 9, 15, 30, 0, time.UTC)

const (
	inboundSite = "PATH-ZONE-A" // the plan fixture's location
	inboundPath = "pick-rebin-pack"
)

func bookedAppt(t *testing.T, id, location string, start time.Time, asns int, asOf time.Time) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewBooked(inbound.BookedParams{
		AppointmentID: id, Location: location, WindowStart: start, WindowEnd: start.Add(2 * time.Hour), ASNCount: asns, AsOf: asOf,
	})
	if err != nil {
		t.Fatalf("NewBooked(%s): %v", id, err)
	}
	return a
}

func cancelledAppt(t *testing.T, id, location string, asOf time.Time) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewCancelled(id, location, asOf)
	if err != nil {
		t.Fatalf("NewCancelled(%s): %v", id, err)
	}
	return a
}

func declare(t *testing.T, repo *memory.InboundStandardRepo, location, path string, perAppt, perASN float64) {
	t.Helper()
	uc := &DeclareInboundLabourStandard{Repo: repo}
	if _, _, err := uc.Handle(context.Background(), DeclareInboundLabourStandardCommand{
		Location: location, ProcessPathID: path, MinutesPerAppointment: perAppt, MinutesPerASN: perASN,
	}); err != nil {
		t.Fatalf("declare: %v", err)
	}
}

// --- RecordInboundAppointment ------------------------------------------------

type inboundRecordFixture struct {
	repo      *memory.InboundAppointmentRepo
	processed *memory.ProcessedEventRepo
	record    *RecordInboundAppointment
}

func newInboundRecordFixture(appts interface {
	Apply(context.Context, inbound.Appointment) (bool, error)
	Demand(context.Context, string, time.Time, time.Time) (inbound.Demand, error)
}) *inboundRecordFixture {
	repo, processed := memory.NewInboundAppointmentRepo(), memory.NewProcessedEventRepo()
	if appts == nil {
		appts = repo
	}
	return &inboundRecordFixture{
		repo: repo, processed: processed,
		record: &RecordInboundAppointment{
			UoW: memory.NewUnitOfWork(repo, processed), ProcessedEvents: processed, Appointments: appts,
		},
	}
}

func TestRecordInboundAppointment_AppliesThenIgnoresAReplayOfTheSameEventID(t *testing.T) {
	f := newInboundRecordFixture(nil)
	ctx := context.Background()
	first := bookedAppt(t, "appt-1", "SIM1", planWindowStart, 2, inboundAsOf)
	if got, err := f.record.Handle(ctx, "c", "evt-1", first); err != nil || got != OutcomeApplied {
		t.Fatalf("first = %v, %v; want applied", got, err)
	}
	// Same id, a different (later) payload: a consumer that does not claim on
	// the id would apply it.
	different := bookedAppt(t, "appt-1", "SIM1", planWindowEnd, 9, inboundAsOf.Add(time.Hour))
	if got, err := f.record.Handle(ctx, "c", "evt-1", different); err != nil || got != OutcomeDuplicate {
		t.Fatalf("replay = %v, %v; want duplicate", got, err)
	}
	if stored, _ := f.repo.Get("appt-1"); stored.ASNCount() != 2 || !stored.WindowStart().Equal(planWindowStart) {
		t.Fatalf("a replayed event id changed the model: %+v", stored)
	}
}

func TestRecordInboundAppointment_CancelledBeforeBookedWins(t *testing.T) {
	f := newInboundRecordFixture(nil)
	ctx := context.Background()
	cancel := cancelledAppt(t, "appt-1", "SIM1", inboundAsOf.Add(time.Minute))
	book := bookedAppt(t, "appt-1", "SIM1", planWindowStart.Add(time.Hour), 1, inboundAsOf)

	if got, err := f.record.Handle(ctx, "c", "evt-cancel", cancel); err != nil || got != OutcomeApplied {
		t.Fatalf("cancel first = %v, %v", got, err)
	}
	got, err := f.record.Handle(ctx, "c", "evt-book", book)
	if err != nil || got != OutcomeStale {
		t.Fatalf("booked after the cancel = %v, %v; want stale", got, err)
	}
	if stored, _ := f.repo.Get("appt-1"); stored.Status() != inbound.StatusCancelled {
		t.Fatalf("status = %s, want CANCELLED", stored.Status())
	}
	d, _ := f.repo.Demand(ctx, "SIM1", planWindowStart, planWindowEnd)
	if d.Appointments != 0 {
		t.Fatalf("a cancelled appointment counted: %+v", d)
	}
	if !f.processed.Has("c", "evt-book") {
		t.Fatal("a stale event is still claimed (it was handled, it changed nothing)")
	}
}

type failingAppointments struct{ err error }

func (f failingAppointments) Apply(context.Context, inbound.Appointment) (bool, error) {
	return false, f.err
}
func (f failingAppointments) Demand(context.Context, string, time.Time, time.Time) (inbound.Demand, error) {
	return inbound.Demand{}, f.err
}

func TestRecordInboundAppointment_TransientFailureRollsBackTheClaim(t *testing.T) {
	boom := errors.New("transient database failure")
	f := newInboundRecordFixture(failingAppointments{err: boom})
	_, err := f.record.Handle(context.Background(), "c", "evt-1", bookedAppt(t, "appt-1", "SIM1", planWindowStart, 1, inboundAsOf))
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the infrastructure error", err)
	}
	if f.processed.Has("c", "evt-1") {
		t.Fatal("the claim survived the failed write: the redelivery would be skipped")
	}
}

// --- DeclareInboundLabourStandard -------------------------------------------

func TestDeclareInboundLabourStandard_CreatesThenReplaces(t *testing.T) {
	repo := memory.NewInboundStandardRepo()
	uc := &DeclareInboundLabourStandard{Repo: repo}
	ctx := context.Background()

	s, created, err := uc.Handle(ctx, DeclareInboundLabourStandardCommand{Location: "SIM1", ProcessPathID: "receive", MinutesPerAppointment: 30})
	if err != nil || !created || s.MinutesPerAppointment() != 30 {
		t.Fatalf("first = %+v created %v err %v", s, created, err)
	}
	s, created, err = uc.Handle(ctx, DeclareInboundLabourStandardCommand{Location: "SIM1", ProcessPathID: "receive", MinutesPerAppointment: 45, MinutesPerASN: 5})
	if err != nil || created || s.MinutesPerAppointment() != 45 || s.MinutesPerASN() != 5 {
		t.Fatalf("second = %+v created %v err %v; want a replacement", s, created, err)
	}
	if got, _ := repo.Find(ctx, "SIM1", "receive"); got == nil || got.MinutesPerAppointment() != 45 {
		t.Fatalf("stored = %+v", got)
	}
}

func TestDeclareInboundLabourStandard_RejectsInvalidAndStoresNothing(t *testing.T) {
	repo := memory.NewInboundStandardRepo()
	uc := &DeclareInboundLabourStandard{Repo: repo}
	_, _, err := uc.Handle(context.Background(), DeclareInboundLabourStandardCommand{Location: "SIM1", ProcessPathID: "receive", MinutesPerAppointment: 0})
	if !errors.Is(err, inbound.ErrNonPositiveAppointmentMinutes) {
		t.Fatalf("err = %v", err)
	}
	if list, _ := repo.List(context.Background(), ""); len(list) != 0 {
		t.Fatalf("an invalid declaration stored %d standards", len(list))
	}
}

// --- GetInboundDemand --------------------------------------------------------

type inboundFixture struct {
	appts     *memory.InboundAppointmentRepo
	standards *memory.InboundStandardRepo
	get       *GetInboundDemand
}

func newInboundFixture() *inboundFixture {
	a, s := memory.NewInboundAppointmentRepo(), memory.NewInboundStandardRepo()
	return &inboundFixture{appts: a, standards: s, get: &GetInboundDemand{Appointments: a, Standards: s}}
}

func (f *inboundFixture) book(t *testing.T, id string, start time.Time, asns int) {
	t.Helper()
	if _, err := f.appts.Apply(context.Background(), bookedAppt(t, id, inboundSite, start, asns, inboundAsOf)); err != nil {
		t.Fatal(err)
	}
}

func TestGetInboundDemand_PricesAppointmentsWithTheDeclaredStandard(t *testing.T) {
	f := newInboundFixture()
	f.book(t, "a1", planWindowStart, 2)
	f.book(t, "a2", planWindowStart.Add(3*time.Hour), 3)
	f.book(t, "at-end", planWindowEnd, 4)                     // next window
	f.book(t, "before", planWindowStart.Add(-time.Second), 5) // previous window
	declare(t, f.standards, inboundSite, inboundPath, 30, 5)

	res, err := f.get.Handle(context.Background(), inboundSite, inboundPath, planWindowStart, planWindowEnd)
	if err != nil {
		t.Fatal(err)
	}
	// 2 appointments x 30 + 5 ASNs x 5 = 85 minutes over 8h.
	if res.Demand.Appointments != 2 || res.Demand.ASNs != 5 || res.LabourMinutes != 85 || !res.StandardDeclared {
		t.Fatalf("result = %+v", res)
	}
	if want := 85.0 / 480; res.AverageHeads != want {
		t.Fatalf("average heads = %v, want %v", res.AverageHeads, want)
	}
	if res.StandardMissing || res.SiteHasNoStandard {
		t.Fatalf("a declared standard reported missing: %+v", res)
	}
}

func TestGetInboundDemand_NoStandardMeansNoDemandAndSaysSo(t *testing.T) {
	f := newInboundFixture()
	f.book(t, "a1", planWindowStart, 2)
	res, err := f.get.Handle(context.Background(), inboundSite, inboundPath, planWindowStart, planWindowEnd)
	if err != nil {
		t.Fatal(err)
	}
	if res.Demand.Appointments != 1 {
		t.Fatalf("the appointment must still be recorded and counted: %+v", res)
	}
	if res.LabourMinutes != 0 || res.AverageHeads != 0 || res.StandardDeclared || !res.StandardMissing || !res.SiteHasNoStandard {
		t.Fatalf("result = %+v; want 0 minutes, standard missing for the site (no default is ever guessed)", res)
	}
}

func TestGetInboundDemand_AStandardForAnotherPathIsNotASiteWideMiss(t *testing.T) {
	f := newInboundFixture()
	f.book(t, "a1", planWindowStart, 1)
	declare(t, f.standards, inboundSite, "putaway", 20, 0)
	res, err := f.get.Handle(context.Background(), inboundSite, inboundPath, planWindowStart, planWindowEnd)
	if err != nil {
		t.Fatal(err)
	}
	if !res.StandardMissing || res.SiteHasNoStandard || res.LabourMinutes != 0 {
		t.Fatalf("result = %+v; want missing for the path but not for the site", res)
	}
}

func TestGetInboundDemand_NoAppointmentsIsNotAMissingStandard(t *testing.T) {
	f := newInboundFixture()
	res, err := f.get.Handle(context.Background(), inboundSite, inboundPath, planWindowStart, planWindowEnd)
	if err != nil {
		t.Fatal(err)
	}
	if res.Demand.Appointments != 0 || res.StandardMissing || res.SiteHasNoStandard {
		t.Fatalf("result = %+v", res)
	}
}

func TestGetInboundDemand_OtherSiteAndCancelledDoNotCount(t *testing.T) {
	f := newInboundFixture()
	ctx := context.Background()
	if _, err := f.appts.Apply(ctx, bookedAppt(t, "other", "ELSEWHERE", planWindowStart, 1, inboundAsOf)); err != nil {
		t.Fatal(err)
	}
	f.book(t, "gone", planWindowStart, 1)
	if _, err := f.appts.Apply(ctx, cancelledAppt(t, "gone", inboundSite, inboundAsOf.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	declare(t, f.standards, inboundSite, inboundPath, 30, 0)
	res, err := f.get.Handle(ctx, inboundSite, inboundPath, planWindowStart, planWindowEnd)
	if err != nil || res.Demand.Appointments != 0 || res.LabourMinutes != 0 {
		t.Fatalf("result = %+v err %v", res, err)
	}
}

func TestGetInboundDemand_RejectsAnUnusableWindowAndSurfacesReadFailures(t *testing.T) {
	f := newInboundFixture()
	if _, err := f.get.Handle(context.Background(), inboundSite, inboundPath, planWindowStart, planWindowStart); !errors.Is(err, processcapacity.ErrInvalidWindow) {
		t.Fatalf("err = %v, want ErrInvalidWindow", err)
	}
	boom := errors.New("db down")
	broken := &GetInboundDemand{Appointments: failingAppointments{err: boom}, Standards: f.standards}
	if _, err := broken.Handle(context.Background(), inboundSite, inboundPath, planWindowStart, planWindowEnd); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the read failure (never masked as no data)", err)
	}
}

// --- CreateCapacityPlan integration -----------------------------------------

func newInboundPlanFixture(t *testing.T) (*planFixture, *inboundFixture) {
	t.Helper()
	f := newPlanFixture(t)
	in := newInboundFixture()
	f.create.Inbound = in.get
	return f, in
}

func TestCreateCapacityPlan_CarriesInboundDemandNextToOrderDemand(t *testing.T) {
	f, in := newInboundPlanFixture(t)
	in.book(t, "a1", planWindowStart.Add(time.Hour), 2)
	in.book(t, "a2", planWindowStart.Add(2*time.Hour), 1)
	declare(t, in.standards, inboundSite, inboundPath, 60, 0)

	plan, err := f.create.Handle(context.Background(), planCommand(12000))
	if err != nil {
		t.Fatal(err)
	}
	got := plan.Inbound()
	if got.Appointments != 2 || got.LabourMinutes != 120 || got.StandardMissing {
		t.Fatalf("Inbound = %+v", got)
	}
	if plan.AssignedDemand() != 12000 || plan.Shortage() != 4000 {
		t.Fatalf("inbound demand changed the ORDER figures: demand %v shortage %v", plan.AssignedDemand(), plan.Shortage())
	}
	if len(plan.Warnings()) != 0 {
		t.Fatalf("warnings = %v", plan.Warnings())
	}
	stored, _ := f.plans.FindByID(context.Background(), plan.ID())
	if stored == nil || stored.Inbound() != got {
		t.Fatalf("the persisted plan lost its inbound demand: %+v", stored)
	}
}

func TestCreateCapacityPlan_WarnsInboundStandardMissingWhenTheSiteDeclaredNone(t *testing.T) {
	f, in := newInboundPlanFixture(t)
	in.book(t, "a1", planWindowStart.Add(time.Hour), 1)
	in.book(t, "a2", planWindowStart.Add(2*time.Hour), 1)

	plan, err := f.create.Handle(context.Background(), planCommand(100))
	if err != nil {
		t.Fatal(err)
	}
	got := plan.Inbound()
	if got.Appointments != 2 || got.LabourMinutes != 0 || !got.StandardMissing {
		t.Fatalf("Inbound = %+v; want 2 appointments, no demand, standard missing", got)
	}
	var found string
	for _, w := range plan.Warnings() {
		if strings.HasPrefix(w, "inbound standard missing") {
			found = w
		}
	}
	if found == "" || !strings.Contains(found, "2 dock appointment(s)") || !strings.Contains(found, "/inbound-labour-standards/PATH-ZONE-A/pick-rebin-pack") {
		t.Fatalf("warnings = %v; want the inbound standard missing warning naming the count and the endpoint", plan.Warnings())
	}
}

func TestCreateCapacityPlan_NoWarningForAnOutboundPathAtASiteWithAnInboundStandard(t *testing.T) {
	f, in := newInboundPlanFixture(t)
	in.book(t, "a1", planWindowStart.Add(time.Hour), 1)
	declare(t, in.standards, inboundSite, "receive", 30, 0)

	plan, err := f.create.Handle(context.Background(), planCommand(100))
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.Inbound(); got.LabourMinutes != 0 || !got.StandardMissing {
		t.Fatalf("Inbound = %+v; the outbound path has no inbound standard of its own", got)
	}
	if len(plan.Warnings()) != 0 {
		t.Fatalf("warnings = %v; a site with an inbound standard elsewhere is not nagged", plan.Warnings())
	}
}

func TestCreateCapacityPlan_WithoutInboundWiredOrAppointmentsThereIsNoInboundDemand(t *testing.T) {
	f := newPlanFixture(t) // Inbound nil
	plan, err := f.create.Handle(context.Background(), planCommand(100))
	if err != nil || plan.Inbound().Appointments != 0 || len(plan.Warnings()) != 0 {
		t.Fatalf("plan %+v err %v", plan, err)
	}
	f2, _ := newInboundPlanFixture(t)
	plan, err = f2.create.Handle(context.Background(), planCommand(100))
	if err != nil || plan.Inbound() != (capacityplan.InboundDemand{}) || len(plan.Warnings()) != 0 {
		t.Fatalf("plan %+v err %v", plan, err)
	}
}

func TestCreateCapacityPlan_InboundReadFailureIsNotMasked(t *testing.T) {
	f, in := newInboundPlanFixture(t)
	boom := errors.New("db down")
	f.create.Inbound = &GetInboundDemand{Appointments: failingAppointments{err: boom}, Standards: in.standards}
	plan, err := f.create.Handle(context.Background(), planCommand(100))
	if !errors.Is(err, boom) || plan != nil {
		t.Fatalf("plan %v err %v", plan, err)
	}
	if stored, _ := f.plans.FindByID(context.Background(), "plan-1"); stored != nil {
		t.Fatal("a failed inbound read stored a plan")
	}
}
