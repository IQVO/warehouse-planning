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

// at is the event time of aggregate version v: it never orders anything (the
// version does), it only has to be a valid instant.
func at(v int64) time.Time { return t0.Add(time.Duration(v) * time.Minute) }

func booked(t *testing.T, id string, start time.Time, asns int, version int64) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewBooked(inbound.BookedParams{
		AppointmentID: id, Location: "SIM1", Direction: inbound.DirectionInbound,
		WindowStart: start, WindowEnd: start.Add(2 * time.Hour), ASNCount: asns, Version: version, AsOf: at(version),
	})
	if err != nil {
		t.Fatalf("NewBooked: %v", err)
	}
	return a
}

func outboundBooked(t *testing.T, id string, start time.Time, version int64) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewBooked(inbound.BookedParams{
		AppointmentID: id, Location: "SIM1", Direction: inbound.DirectionOutbound,
		WindowStart: start, WindowEnd: start.Add(2 * time.Hour), Version: version, AsOf: at(version),
	})
	if err != nil {
		t.Fatalf("NewBooked outbound: %v", err)
	}
	return a
}

func rescheduled(t *testing.T, id string, start time.Time, version int64) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewRescheduled(id, "SIM1", start, start.Add(2*time.Hour), version, at(version))
	if err != nil {
		t.Fatalf("NewRescheduled: %v", err)
	}
	return a
}

func cancelled(t *testing.T, id string, version int64) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewCancelled(id, "SIM1", version, at(version))
	if err != nil {
		t.Fatalf("NewCancelled: %v", err)
	}
	return a
}

func noShow(t *testing.T, id string, version int64) inbound.Appointment {
	t.Helper()
	a, err := inbound.NewNoShow(id, "SIM1", inbound.DirectionInbound, version, at(version))
	if err != nil {
		t.Fatalf("NewNoShow: %v", err)
	}
	return a
}

func TestNewBookedValidation(t *testing.T) {
	ok := inbound.BookedParams{AppointmentID: "a1", Location: "SIM1", Direction: inbound.DirectionInbound, WindowStart: winFrom, WindowEnd: winTo, ASNCount: 1, Version: 1, AsOf: t0}
	cases := map[string]struct {
		mutate func(*inbound.BookedParams)
		want   error
	}{
		"blank id":                {func(p *inbound.BookedParams) { p.AppointmentID = "" }, inbound.ErrRequiredField},
		"blank site":              {func(p *inbound.BookedParams) { p.Location = "" }, inbound.ErrRequiredField},
		"no event time":           {func(p *inbound.BookedParams) { p.AsOf = time.Time{} }, inbound.ErrAsOfRequired},
		"no version":              {func(p *inbound.BookedParams) { p.Version = 0 }, inbound.ErrVersionRequired},
		"negative version":        {func(p *inbound.BookedParams) { p.Version = -2 }, inbound.ErrVersionRequired},
		"no direction":            {func(p *inbound.BookedParams) { p.Direction = "" }, inbound.ErrInvalidDirection},
		"unknown direction":       {func(p *inbound.BookedParams) { p.Direction = "SIDEWAYS" }, inbound.ErrInvalidDirection},
		"zero window start":       {func(p *inbound.BookedParams) { p.WindowStart = time.Time{} }, inbound.ErrInvalidWindow},
		"end equals start":        {func(p *inbound.BookedParams) { p.WindowEnd = p.WindowStart }, inbound.ErrInvalidWindow},
		"end before start":        {func(p *inbound.BookedParams) { p.WindowEnd = p.WindowStart.Add(-time.Second) }, inbound.ErrInvalidWindow},
		"inbound without ASN":     {func(p *inbound.BookedParams) { p.ASNCount = 0 }, inbound.ErrASNRequired},
		"outbound needs no ASN":   {func(p *inbound.BookedParams) { p.Direction, p.ASNCount = inbound.DirectionOutbound, 0 }, nil},
		"end one second over":     {func(p *inbound.BookedParams) { p.WindowEnd = p.WindowStart.Add(time.Second) }, nil},
		"later version is stored": {func(p *inbound.BookedParams) { p.Version = 7 }, nil},
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
		AppointmentID: "a1", Location: "SIM1", Direction: inbound.DirectionInbound,
		WindowStart: winFrom.In(zone), WindowEnd: winTo.In(zone), ASNCount: 2, Version: 1, AsOf: t0.In(zone),
	})
	if err != nil {
		t.Fatal(err)
	}
	if a.WindowStart().Location() != time.UTC || a.WindowEnd().Location() != time.UTC || a.AsOf().Location() != time.UTC {
		t.Fatalf("instants are not UTC: %v %v %v", a.WindowStart().Location(), a.WindowEnd().Location(), a.AsOf().Location())
	}
	if a.ID() != "a1" || a.Location() != "SIM1" || a.Direction() != inbound.DirectionInbound || a.Status() != inbound.StatusBooked ||
		a.ASNCount() != 2 || a.Version() != 1 || !a.HasWindow() {
		t.Fatalf("accessors = %+v", a)
	}
}

func TestNewRescheduledValidation(t *testing.T) {
	cases := map[string]error{
		"ok":         nil,
		"blank id":   inbound.ErrRequiredField,
		"blank site": inbound.ErrRequiredField,
		"no version": inbound.ErrVersionRequired,
		"no time":    inbound.ErrAsOfRequired,
		"bad window": inbound.ErrInvalidWindow,
	}
	for name, want := range cases {
		id, site, ver, ts, end := "a1", "SIM1", int64(2), t0, winTo
		switch name {
		case "blank id":
			id = ""
		case "blank site":
			site = ""
		case "no version":
			ver = 0
		case "no time":
			ts = time.Time{}
		case "bad window":
			end = winFrom
		}
		_, err := inbound.NewRescheduled(id, site, winFrom, end, ver, ts)
		if !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", name, err, want)
		}
	}
	a := rescheduled(t, "a1", winFrom, 2)
	if a.Status() != inbound.StatusBooked || a.Direction() != "" || a.ASNCount() != 0 || !a.HasWindow() {
		t.Fatalf("a Rescheduled is a booked, direction-less, ASN-less partial: %+v", a)
	}
}

func TestNewCancelledAndNoShowValidation(t *testing.T) {
	if _, err := inbound.NewCancelled("", "SIM1", 2, t0); !errors.Is(err, inbound.ErrRequiredField) {
		t.Errorf("blank id: %v", err)
	}
	if _, err := inbound.NewCancelled("a1", "", 2, t0); !errors.Is(err, inbound.ErrRequiredField) {
		t.Errorf("blank site: %v", err)
	}
	if _, err := inbound.NewCancelled("a1", "SIM1", 0, t0); !errors.Is(err, inbound.ErrVersionRequired) {
		t.Errorf("no version: %v", err)
	}
	if _, err := inbound.NewCancelled("a1", "SIM1", 2, time.Time{}); !errors.Is(err, inbound.ErrAsOfRequired) {
		t.Errorf("no time: %v", err)
	}
	if a := cancelled(t, "a1", 2); a.Status() != inbound.StatusCancelled || a.HasWindow() || a.ASNCount() != 0 {
		t.Fatalf("a bare cancellation must be a window-less tombstone: %+v", a)
	}
	if _, err := inbound.NewNoShow("a1", "SIM1", "", 2, t0); !errors.Is(err, inbound.ErrInvalidDirection) {
		t.Errorf("no direction: %v", err)
	}
	if _, err := inbound.NewNoShow("", "SIM1", inbound.DirectionInbound, 2, t0); !errors.Is(err, inbound.ErrRequiredField) {
		t.Errorf("blank id: %v", err)
	}
	if a := noShow(t, "a1", 2); a.Status() != inbound.StatusNoShow || a.Direction() != inbound.DirectionInbound || a.HasWindow() {
		t.Fatalf("a no-show names its direction and has no window: %+v", a)
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
		a := booked(t, "a1", c.start, 1, 1)
		if got := a.CountsIn(winFrom, winTo); got != c.want {
			t.Errorf("%s: CountsIn = %v, want %v", name, got, c.want)
		}
	}
}

func TestOnlyABookedInboundAppointmentIsDemand(t *testing.T) {
	b := booked(t, "a1", winFrom, 1, 1)
	if !b.CountsIn(winFrom, winTo) {
		t.Fatal("a booked inbound appointment must count")
	}
	if outboundBooked(t, "o1", winFrom, 1).CountsIn(winFrom, winTo) {
		t.Fatal("an OUTBOUND appointment counted")
	}
	if rescheduled(t, "r1", winFrom, 2).CountsIn(winFrom, winTo) {
		t.Fatal("a partial row (direction unknown) counted")
	}
	for name, next := range map[string]inbound.Appointment{"cancelled": cancelled(t, "a1", 2), "no-show": noShow(t, "a1", 2)} {
		merged, changed := inbound.Resolve(&b, next)
		if !changed || merged.CountsIn(winFrom, winTo) {
			t.Errorf("%s booking still counts: %+v", name, merged)
		}
		if !merged.HasWindow() {
			t.Errorf("%s must keep the window it removed", name)
		}
	}
	if cancelled(t, "a2", 1).CountsIn(winFrom, winTo) || noShow(t, "a3", 1).CountsIn(winFrom, winTo) {
		t.Fatal("a tombstone counted")
	}
}

func TestResolveTable(t *testing.T) {
	b1 := booked(t, "a1", winFrom, 2, 1)
	r2 := rescheduled(t, "a1", winFrom.Add(time.Hour), 2)
	r3 := rescheduled(t, "a1", winFrom.Add(2*time.Hour), 3)
	c2 := cancelled(t, "a1", 2)
	c3 := cancelled(t, "a1", 3)
	ns3 := noShow(t, "a1", 3)
	b1Again := booked(t, "a1", winFrom, 2, 1)

	type want struct {
		status  inbound.Status
		changed bool
		start   time.Time
		asns    int
		version int64
		dir     inbound.Direction
	}
	cases := []struct {
		name string
		prev *inbound.Appointment
		next inbound.Appointment
		want want
	}{
		{"new id books", nil, b1, want{inbound.StatusBooked, true, winFrom, 2, 1, inbound.DirectionInbound}},
		{"reschedule moves the window", &b1, r2, want{inbound.StatusBooked, true, winFrom.Add(time.Hour), 2, 2, inbound.DirectionInbound}},
		{"a second reschedule moves it again", ptr(resolve(b1, r2)), r3, want{inbound.StatusBooked, true, winFrom.Add(2 * time.Hour), 2, 3, inbound.DirectionInbound}},
		{"an older reschedule is stale", ptr(resolve(b1, r3)), r2, want{inbound.StatusBooked, false, winFrom.Add(2 * time.Hour), 2, 3, inbound.DirectionInbound}},
		{"redelivered booked changes nothing", &b1, b1Again, want{inbound.StatusBooked, false, winFrom, 2, 1, inbound.DirectionInbound}},
		{"cancel wins and keeps the facts", &b1, c2, want{inbound.StatusCancelled, true, winFrom, 2, 2, inbound.DirectionInbound}},
		{"no-show removes the demand", &b1, ns3, want{inbound.StatusNoShow, true, winFrom, 2, 3, inbound.DirectionInbound}},
		{"a cancel of the same version is stale", &c2, cancelled(t, "a1", 2), want{inbound.StatusCancelled, false, time.Time{}, 0, 2, ""}},
		{"booked after a later cancel never resurrects it", ptr(c2), b1, want{inbound.StatusCancelled, true, time.Time{}, 2, 2, inbound.DirectionInbound}},
		{"a stale reschedule after a cancel changes nothing", &c3, r2, want{inbound.StatusCancelled, false, time.Time{}, 0, 3, ""}},
		{"reschedule before booked stores a partial", nil, r2, want{inbound.StatusBooked, true, winFrom.Add(time.Hour), 0, 2, ""}},
		{"booked after a later reschedule completes the facts but keeps the newer window", ptr(r2), b1, want{inbound.StatusBooked, true, winFrom.Add(time.Hour), 2, 2, inbound.DirectionInbound}},
		{"cancel before booked stores a tombstone", nil, c2, want{inbound.StatusCancelled, true, time.Time{}, 0, 2, ""}},
		{"a legacy row (version 0) is superseded by version 1", ptr(legacy(t)), b1, want{inbound.StatusBooked, true, winFrom, 2, 1, inbound.DirectionInbound}},
		{"a legacy row is superseded by a no-show", ptr(legacy(t)), ns3, want{inbound.StatusNoShow, true, winFrom.Add(-time.Hour), 3, 3, inbound.DirectionInbound}},
	}
	for _, c := range cases {
		got, changed := inbound.Resolve(c.prev, c.next)
		if got.Status() != c.want.status || changed != c.want.changed || !got.WindowStart().Equal(c.want.start) ||
			got.ASNCount() != c.want.asns || got.Version() != c.want.version || got.Direction() != c.want.dir {
			t.Errorf("%s:\n got  status %s changed %v start %v asns %d version %d dir %q\n want status %s changed %v start %v asns %d version %d dir %q",
				c.name, got.Status(), changed, got.WindowStart(), got.ASNCount(), got.Version(), got.Direction(),
				c.want.status, c.want.changed, c.want.start, c.want.asns, c.want.version, c.want.dir)
		}
	}
}

func ptr(a inbound.Appointment) *inbound.Appointment { return &a }

func resolve(prev, next inbound.Appointment) inbound.Appointment {
	got, _ := inbound.Resolve(&prev, next)
	return got
}

// legacy is a row written when inbound-receiving was the source: INBOUND, BOOKED,
// 3 ASNs, version 0.
func legacy(t *testing.T) inbound.Appointment {
	t.Helper()
	return inbound.Rehydrate(inbound.RehydrateParams{
		AppointmentID: "a1", Location: "SIM1", Direction: inbound.DirectionInbound, Status: inbound.StatusBooked,
		WindowStart: winFrom.Add(-time.Hour), WindowEnd: winFrom, ASNCount: 3, Version: 0, AsOf: t0,
	})
}

// TestResolveConvergesInAnyOrder: the version guard makes the four events
// commute -- the final state is the same for every arrival order, and it is the
// one the highest version says.
func TestResolveConvergesInAnyOrder(t *testing.T) {
	events := []inbound.Appointment{
		booked(t, "a1", winFrom, 2, 1),
		rescheduled(t, "a1", winFrom.Add(time.Hour), 2),
		rescheduled(t, "a1", winFrom.Add(3*time.Hour), 3),
		noShow(t, "a1", 4),
	}
	var ref *inbound.Appointment
	permutations(len(events), func(order []int) {
		var cur *inbound.Appointment
		for _, i := range order {
			next, _ := inbound.Resolve(cur, events[i])
			cur = &next
		}
		if ref == nil {
			ref = cur
			if cur.Status() != inbound.StatusNoShow || cur.Version() != 4 || cur.ASNCount() != 2 || cur.Direction() != inbound.DirectionInbound {
				t.Fatalf("converged state = %+v, want NO_SHOW v4 INBOUND 2 ASNs", cur)
			}
			return
		}
		// The window of a removed appointment is informational; everything
		// that decides demand must be identical.
		if cur.Status() != ref.Status() || cur.Version() != ref.Version() || cur.Direction() != ref.Direction() || cur.ASNCount() != ref.ASNCount() ||
			cur.CountsIn(winFrom, winTo) != ref.CountsIn(winFrom, winTo) {
			t.Errorf("order %v converged to %+v, want %+v", order, *cur, *ref)
		}
	})
}

// TestResolveLiveAppointmentConvergesToTheLatestWindow: with no terminal event
// the demand-deciding window (the highest version's) is identical in every order.
func TestResolveLiveAppointmentConvergesToTheLatestWindow(t *testing.T) {
	events := []inbound.Appointment{
		booked(t, "a1", winFrom, 2, 1),
		rescheduled(t, "a1", winFrom.Add(time.Hour), 2),
		rescheduled(t, "a1", winFrom.Add(3*time.Hour), 3),
	}
	permutations(len(events), func(order []int) {
		var cur *inbound.Appointment
		for _, i := range order {
			next, _ := inbound.Resolve(cur, events[i])
			cur = &next
		}
		if cur.Status() != inbound.StatusBooked || cur.Version() != 3 || cur.ASNCount() != 2 || cur.Direction() != inbound.DirectionInbound ||
			!cur.WindowStart().Equal(winFrom.Add(3*time.Hour)) {
			t.Errorf("order %v converged to %+v, want BOOKED v3 INBOUND 2 ASNs at the latest window", order, *cur)
		}
	})
}

func permutations(n int, visit func([]int)) {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	var rec func(k int)
	rec = func(k int) {
		if k == n {
			visit(append([]int(nil), idx...))
			return
		}
		for i := k; i < n; i++ {
			idx[k], idx[i] = idx[i], idx[k]
			rec(k + 1)
			idx[k], idx[i] = idx[i], idx[k]
		}
	}
	rec(0)
}

func TestRehydrateRoundTrip(t *testing.T) {
	b := inbound.Rehydrate(inbound.RehydrateParams{AppointmentID: "a1", Location: "SIM1", Direction: inbound.DirectionInbound, Status: inbound.StatusBooked, WindowStart: winFrom, WindowEnd: winTo, ASNCount: 4, Version: 5, AsOf: t0})
	if !b.HasWindow() || b.ASNCount() != 4 || b.Version() != 5 || !b.WindowEnd().Equal(winTo) || !b.CountsIn(winFrom, winTo) {
		t.Fatalf("rehydrated booking = %+v", b)
	}
	tomb := inbound.Rehydrate(inbound.RehydrateParams{AppointmentID: "a2", Location: "SIM1", Status: inbound.StatusCancelled, Version: 2, AsOf: t0})
	if tomb.HasWindow() || tomb.Direction() != "" {
		t.Fatalf("rehydrated tombstone has a window or direction: %+v", tomb)
	}
}

func TestSummarizeCountsInboundBookedInWindowAndReportsSiteAsOf(t *testing.T) {
	in1 := booked(t, "in1", winFrom, 2, 1)
	in2 := booked(t, "in2", winTo.Add(-time.Hour), 3, 9)
	atEnd := booked(t, "end", winTo, 1, 1)
	before := booked(t, "before", winFrom.Add(-time.Second), 1, 1)
	out := outboundBooked(t, "out", winFrom.Add(time.Hour), 1)
	tomb := cancelled(t, "x", 12)
	moved := resolve(booked(t, "mv", winTo.Add(24*time.Hour), 4, 1), rescheduled(t, "mv", winFrom.Add(2*time.Hour), 2))
	cx := resolve(booked(t, "cx", winFrom.Add(time.Hour), 5, 1), cancelled(t, "cx", 3))
	ns := resolve(booked(t, "ns", winFrom.Add(time.Hour), 5, 1), noShow(t, "ns", 3))

	d := inbound.Summarize([]inbound.Appointment{in1, in2, atEnd, before, out, tomb, moved, cx, ns}, winFrom, winTo)
	if d.Appointments != 3 || d.ASNs != 9 {
		t.Fatalf("demand = %+v, want 3 appointments (in1, in2, the rescheduled one) / 9 ASNs", d)
	}
	if !d.AsOf.Equal(at(12)) {
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
