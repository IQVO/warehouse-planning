package inbound_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

var (
	t0      = time.Date(2026, 10, 8, 12, 0, 0, 0, time.UTC)
	winFrom = time.Date(2026, 10, 9, 8, 0, 0, 0, time.UTC)
	winTo   = time.Date(2026, 10, 9, 16, 0, 0, 0, time.UTC)
)

func booked(t *testing.T, id string, start time.Time, asns int, at time.Time) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewBooked(inbound.BookedParams{
		AppointmentID: id, Location: "SIM1", WindowStart: start, WindowEnd: start.Add(2 * time.Hour), ASNCount: asns, AsOf: at,
	})
	if err != nil {
		t.Fatalf("NewBooked: %v", err)
	}
	return a
}

func cancelled(t *testing.T, id string, at time.Time) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewCancelled(id, "SIM1", at)
	if err != nil {
		t.Fatalf("NewCancelled: %v", err)
	}
	return a
}

func TestNewBookedValidation(t *testing.T) {
	ok := inbound.BookedParams{AppointmentID: "a1", Location: "SIM1", WindowStart: winFrom, WindowEnd: winTo, ASNCount: 1, AsOf: t0}
	cases := map[string]struct {
		mutate func(*inbound.BookedParams)
		want   error
	}{
		"blank id":            {func(p *inbound.BookedParams) { p.AppointmentID = "" }, inbound.ErrRequiredField},
		"blank location":      {func(p *inbound.BookedParams) { p.Location = "" }, inbound.ErrRequiredField},
		"no event time":       {func(p *inbound.BookedParams) { p.AsOf = time.Time{} }, inbound.ErrAsOfRequired},
		"zero window start":   {func(p *inbound.BookedParams) { p.WindowStart = time.Time{} }, inbound.ErrInvalidWindow},
		"end equals start":    {func(p *inbound.BookedParams) { p.WindowEnd = p.WindowStart }, inbound.ErrInvalidWindow},
		"end before start":    {func(p *inbound.BookedParams) { p.WindowEnd = p.WindowStart.Add(-time.Second) }, inbound.ErrInvalidWindow},
		"no ASN":              {func(p *inbound.BookedParams) { p.ASNCount = 0 }, inbound.ErrASNRequired},
		"end one second over": {func(p *inbound.BookedParams) { p.WindowEnd = p.WindowStart.Add(time.Second) }, nil},
	}
	for name, c := range cases {
		p := ok
		c.mutate(&p)
		_, err := inbound.NewBooked(p)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
	}
}

func TestNewBookedNormalizesToUTC(t *testing.T) {
	zone := time.FixedZone("BRT", -3*3600)
	a, err := inbound.NewBooked(inbound.BookedParams{
		AppointmentID: "a1", Location: "SIM1", WindowStart: winFrom.In(zone), WindowEnd: winTo.In(zone), ASNCount: 2, AsOf: t0.In(zone),
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.WindowStart().Location() != time.UTC || a.WindowEnd().Location() != time.UTC || a.AsOf().Location() != time.UTC {
		t.Fatalf("instants are not UTC: %v %v %v", a.WindowStart().Location(), a.WindowEnd().Location(), a.AsOf().Location())
	}
	if a.ID() != "a1" || a.Location() != "SIM1" || a.Status() != inbound.StatusBooked || a.ASNCount() != 2 || !a.HasWindow() {
		t.Fatalf("accessors = %+v", a)
	}
}

func TestNewCancelledValidation(t *testing.T) {
	if _, err := inbound.NewCancelled("", "SIM1", t0); !errors.Is(err, inbound.ErrRequiredField) {
		t.Errorf("blank id: %v", err)
	}
	if _, err := inbound.NewCancelled("a1", "", t0); !errors.Is(err, inbound.ErrRequiredField) {
		t.Errorf("blank location: %v", err)
	}
	if _, err := inbound.NewCancelled("a1", "SIM1", time.Time{}); !errors.Is(err, inbound.ErrAsOfRequired) {
		t.Errorf("no time: %v", err)
	}
	a := cancelled(t, "a1", t0)
	if a.Status() != inbound.StatusCancelled || a.HasWindow() || a.ASNCount() != 0 {
		t.Fatalf("a bare cancellation must be a window-less tombstone: %+v", a)
	}
}

func TestCountsInIsHalfOpenOnTheWindowStart(t *testing.T) {
	cases := map[string]struct {
		start time.Time
		want  bool
	}{
		"exactly at window start":        {winFrom, true},
		"one second before window start": {winFrom.Add(-time.Second), false},
		"inside":                         {winFrom.Add(time.Hour), true},
		"one second before window end":   {winTo.Add(-time.Second), true},
		"exactly at window end":          {winTo, false},
	}
	for name, c := range cases {
		a := booked(t, "a1", c.start, 1, t0)
		if got := a.CountsIn(winFrom, winTo); got != c.want {
			t.Errorf("%s: CountsIn = %v, want %v", name, got, c.want)
		}
	}
}

func TestACancelledAppointmentOrATombstoneIsNeverDemand(t *testing.T) {
	b := booked(t, "a1", winFrom, 1, t0)
	merged, changed := inbound.Resolve(&b, cancelled(t, "a1", t0.Add(time.Minute)))
	if !changed || merged.CountsIn(winFrom, winTo) {
		t.Fatalf("a cancelled booking counted: %+v", merged)
	}
	if cancelled(t, "a2", t0).CountsIn(winFrom, winTo) {
		t.Fatal("a tombstone counted")
	}
}

func TestResolveTable(t *testing.T) {
	b0 := booked(t, "a1", winFrom, 2, t0)
	c1 := cancelled(t, "a1", t0.Add(time.Minute))
	cEq := cancelled(t, "a1", t0)
	cOld := cancelled(t, "a1", t0.Add(-time.Minute))
	bNew := booked(t, "a1", winFrom.Add(time.Hour), 3, t0.Add(2*time.Minute))
	bEq := booked(t, "a1", winFrom.Add(time.Hour), 3, t0)

	cases := []struct {
		name       string
		prev       *inbound.Appointment
		next       inbound.Appointment
		wantStatus inbound.Status
		wantChange bool
		wantASNs   int
	}{
		{"new id books", nil, b0, inbound.StatusBooked, true, 2},
		{"cancel before booked stores a tombstone", nil, c1, inbound.StatusCancelled, true, 0},
		{"later cancel wins and keeps the booking's facts", &b0, c1, inbound.StatusCancelled, true, 2},
		{"cancel at the same time wins the tie", &b0, cEq, inbound.StatusCancelled, true, 2},
		{"older cancel is stale", &b0, cOld, inbound.StatusBooked, false, 2},
		{"booked after a later cancel is stale", &c1, b0, inbound.StatusCancelled, false, 0},
		{"booked at the same time never undoes a cancel", &cEq, b0, inbound.StatusCancelled, false, 0},
		{"a newer booking replaces an older cancellation", &c1, bNew, inbound.StatusBooked, true, 3},
		{"same-time booked does not replace booked", &b0, bEq, inbound.StatusBooked, false, 2},
		{"newer booked replaces booked", &b0, bNew, inbound.StatusBooked, true, 3},
		{"redelivered cancel is a no-op", &c1, c1, inbound.StatusCancelled, false, 0},
	}
	for _, c := range cases {
		got, changed := inbound.Resolve(c.prev, c.next)
		if got.Status() != c.wantStatus || changed != c.wantChange || got.ASNCount() != c.wantASNs {
			t.Errorf("%s: status %s changed %v asns %d, want %s %v %d", c.name, got.Status(), changed, got.ASNCount(), c.wantStatus, c.wantChange, c.wantASNs)
		}
	}
}

func TestResolveACancelledBeforeBookedWinsInEitherOrder(t *testing.T) {
	book := booked(t, "a1", winFrom, 1, t0)
	cancel := cancelled(t, "a1", t0.Add(time.Minute))

	inOrder, _ := inbound.Resolve(nil, book)
	inOrder, _ = inbound.Resolve(&inOrder, cancel)

	reversed, _ := inbound.Resolve(nil, cancel)
	reversed, _ = inbound.Resolve(&reversed, book)

	for name, a := range map[string]inbound.Appointment{"booked then cancelled": inOrder, "cancelled then booked": reversed} {
		if a.Status() != inbound.StatusCancelled || a.CountsIn(winFrom, winTo) {
			t.Errorf("%s: %+v must be cancelled and not demand", name, a)
		}
	}
}

func TestRehydrateRoundTrip(t *testing.T) {
	b := inbound.Rehydrate(inbound.RehydrateParams{AppointmentID: "a1", Location: "SIM1", Status: inbound.StatusBooked, WindowStart: winFrom, WindowEnd: winTo, ASNCount: 4, AsOf: t0})
	if !b.HasWindow() || b.ASNCount() != 4 || !b.WindowEnd().Equal(winTo) || !b.CountsIn(winFrom, winTo) {
		t.Fatalf("rehydrated booking = %+v", b)
	}
	tomb := inbound.Rehydrate(inbound.RehydrateParams{AppointmentID: "a2", Location: "SIM1", Status: inbound.StatusCancelled, AsOf: t0})
	if tomb.HasWindow() {
		t.Fatalf("rehydrated tombstone has a window: %+v", tomb)
	}
}

func TestSummarizeCountsBookedInWindowAndReportsSiteAsOf(t *testing.T) {
	in1 := booked(t, "in1", winFrom, 2, t0)
	in2 := booked(t, "in2", winTo.Add(-time.Hour), 3, t0.Add(time.Hour))
	atEnd := booked(t, "end", winTo, 1, t0)
	before := booked(t, "before", winFrom.Add(-time.Second), 1, t0)
	cancelledIn := cancelled(t, "x", t0.Add(3*time.Hour))
	b := booked(t, "cx", winFrom.Add(time.Hour), 5, t0)
	cx, _ := inbound.Resolve(&b, cancelled(t, "cx", t0.Add(2*time.Hour)))

	d := inbound.Summarize([]inbound.Appointment{in1, in2, atEnd, before, cancelledIn, cx}, winFrom, winTo)
	if d.Appointments != 2 || d.ASNs != 5 {
		t.Fatalf("demand = %+v, want 2 appointments / 5 ASNs", d)
	}
	if !d.AsOf.Equal(t0.Add(3 * time.Hour)) {
		t.Fatalf("as_of = %v, want the newest event time of the whole site", d.AsOf)
	}
	if empty := inbound.Summarize(nil, winFrom, winTo); empty.Appointments != 0 || !empty.AsOf.IsZero() {
		t.Fatalf("empty site = %+v", empty)
	}
}

func TestNewLabourStandardValidation(t *testing.T) {
	cases := []struct {
		name          string
		loc, path     string
		perAppt, perA float64
		want          error
	}{
		{"valid", "SIM1", "receive", 30, 0, nil},
		{"valid with ASN term", "SIM1", "receive", 30, 5, nil},
		{"blank location", "", "receive", 30, 0, inbound.ErrStandardRequiredField},
		{"blank path", "SIM1", "", 30, 0, inbound.ErrStandardRequiredField},
		{"zero per appointment", "SIM1", "receive", 0, 0, inbound.ErrNonPositiveAppointmentMinutes},
		{"negative per appointment", "SIM1", "receive", -1, 0, inbound.ErrNonPositiveAppointmentMinutes},
		{"NaN per appointment", "SIM1", "receive", math.NaN(), 0, inbound.ErrNonPositiveAppointmentMinutes},
		{"negative per ASN", "SIM1", "receive", 30, -0.5, inbound.ErrNegativeASNMinutes},
		{"NaN per ASN", "SIM1", "receive", 30, math.NaN(), inbound.ErrNegativeASNMinutes},
		{"infinite per appointment", "SIM1", "receive", math.Inf(1), 0, inbound.ErrNonFiniteMinutes},
		{"infinite per ASN", "SIM1", "receive", 30, math.Inf(1), inbound.ErrNonFiniteMinutes},
	}
	for _, c := range cases {
		_, err := inbound.NewLabourStandard(c.loc, c.path, c.perAppt, c.perA)
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
	}
}

func TestLabourMinutes(t *testing.T) {
	s, err := inbound.NewLabourStandard("SIM1", "receive", 30, 5)
	if err != nil {
		t.Fatal(err)
	}
	if s.Location() != "SIM1" || s.ProcessPathID() != "receive" || s.MinutesPerAppointment() != 30 || s.MinutesPerASN() != 5 {
		t.Fatalf("accessors = %+v", s)
	}
	// 2 appointments x 30 + 5 ASNs x 5 = 85.
	if got := s.LabourMinutes(inbound.Demand{Appointments: 2, ASNs: 5}); got != 85 {
		t.Fatalf("minutes = %v, want 85", got)
	}
	if got := s.LabourMinutes(inbound.Demand{}); got != 0 {
		t.Fatalf("no appointments = %v minutes, want 0", got)
	}
	flat, _ := inbound.NewLabourStandard("SIM1", "receive", 45, 0)
	if got := flat.LabourMinutes(inbound.Demand{Appointments: 3, ASNs: 9}); got != 135 {
		t.Fatalf("per-appointment only = %v, want 135 (the ASN term is optional)", got)
	}
}

func TestAverageHeads(t *testing.T) {
	if got := inbound.AverageHeads(480, 8*time.Hour); got != 1 {
		t.Errorf("480 min over 8h = %v, want 1 head", got)
	}
	if got := inbound.AverageHeads(240, 8*time.Hour); got != 0.5 {
		t.Errorf("240 min over 8h = %v, want 0.5", got)
	}
	if got := inbound.AverageHeads(100, 0); got != 0 {
		t.Errorf("zero window = %v, want 0", got)
	}
	if got := inbound.AverageHeads(100, -time.Hour); got != 0 {
		t.Errorf("negative window = %v, want 0", got)
	}
}
