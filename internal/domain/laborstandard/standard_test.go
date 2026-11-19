package laborstandard_test

import (
	"errors"
	"testing"
	"time"

	"github.com/claudioed/warehouse-planning/internal/domain/laborstandard"
)

var (
	t0          = time.Date(2026, 11, 2, 6, 0, 0, 0, time.UTC)
	windowStart = time.Date(2026, 11, 20, 8, 0, 0, 0, time.UTC)
)

func int64p(v int64) *int64 { return &v }

func mustNew(t *testing.T, p laborstandard.Params) laborstandard.Standard {
	t.Helper()
	s, err := laborstandard.New(p)
	if err != nil {
		t.Fatalf("New(%+v): %v", p, err)
	}
	return s
}

// std builds a valid task-type-only PICK standard; callers adjust the fields
// the case is about.
func std(id string, mutate func(*laborstandard.Params)) laborstandard.Params {
	p := laborstandard.Params{
		StandardID:      id,
		TaskType:        "PICK",
		ExpectedSeconds: 45,
		EffectiveFrom:   t0,
		AsOf:            t0,
	}
	if mutate != nil {
		mutate(&p)
	}
	return p
}

func TestNewValidation(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*laborstandard.Params)
		want   error
	}{
		{"valid task-type-only", nil, nil},
		{"blank standard id", func(p *laborstandard.Params) { p.StandardID = "" }, laborstandard.ErrRequiredField},
		{"blank task type", func(p *laborstandard.Params) { p.TaskType = "" }, laborstandard.ErrRequiredField},
		{"no effective_from", func(p *laborstandard.Params) { p.EffectiveFrom = time.Time{} }, laborstandard.ErrRequiredField},
		{"no event time", func(p *laborstandard.Params) { p.AsOf = time.Time{} }, laborstandard.ErrRequiredField},
		{"zero expected seconds", func(p *laborstandard.Params) { p.ExpectedSeconds = 0 }, laborstandard.ErrNonPositiveExpectedSeconds},
		{"negative expected seconds", func(p *laborstandard.Params) { p.ExpectedSeconds = -1 }, laborstandard.ErrNonPositiveExpectedSeconds},
		{"one expected second", func(p *laborstandard.Params) { p.ExpectedSeconds = 1 }, nil},
		{"zero travel component", func(p *laborstandard.Params) { p.TravelComponentSeconds = int64p(0) }, nil},
		{"negative travel component", func(p *laborstandard.Params) { p.TravelComponentSeconds = int64p(-1) }, laborstandard.ErrNegativeTravelComponent},
		{"EACH", func(p *laborstandard.Params) { p.HandlingUnit = laborstandard.HandlingUnitEach }, nil},
		{"CASE", func(p *laborstandard.Params) { p.HandlingUnit = laborstandard.HandlingUnitCase }, nil},
		{"PALLET", func(p *laborstandard.Params) { p.HandlingUnit = laborstandard.HandlingUnitPallet }, nil},
		{"unknown handling unit", func(p *laborstandard.Params) { p.HandlingUnit = "INNER" }, laborstandard.ErrUnknownHandlingUnit},
		{"site scoped", func(p *laborstandard.Params) { p.SiteCode = "SIM1" }, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := laborstandard.New(std("std-1", tc.mutate))
			if !errors.Is(err, tc.want) {
				t.Fatalf("New error = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestNewKeepsTheFactsInUTC(t *testing.T) {
	local := time.FixedZone("BRT", -3*3600)
	s := mustNew(t, laborstandard.Params{
		StandardID:             "std-9",
		TaskType:               "PACK",
		HandlingUnit:           laborstandard.HandlingUnitCase,
		SiteCode:               "SIM1",
		ExpectedSeconds:        30,
		TravelComponentSeconds: int64p(12),
		EffectiveFrom:          t0.In(local),
		AsOf:                   t0.Add(time.Minute).In(local),
	})
	if s.ID() != "std-9" || s.TaskType() != "PACK" || s.HandlingUnit() != laborstandard.HandlingUnitCase ||
		s.SiteCode() != "SIM1" || !s.SiteScoped() || s.ExpectedSeconds() != 30 {
		t.Fatalf("facts = %+v", s)
	}
	if s.EffectiveFrom().Location() != time.UTC || !s.EffectiveFrom().Equal(t0) {
		t.Errorf("EffectiveFrom = %v, want %v in UTC", s.EffectiveFrom(), t0)
	}
	if s.AsOf().Location() != time.UTC || !s.AsOf().Equal(t0.Add(time.Minute)) {
		t.Errorf("AsOf = %v, want %v in UTC", s.AsOf(), t0.Add(time.Minute))
	}
	travel := s.TravelComponentSeconds()
	if travel == nil || *travel != 12 {
		t.Fatalf("TravelComponentSeconds = %v, want 12", travel)
	}
	*travel = 99
	if again := s.TravelComponentSeconds(); *again != 12 {
		t.Errorf("TravelComponentSeconds is not a copy: %d after mutating the returned pointer", *again)
	}
}

func TestNewCopiesTheTravelComponent(t *testing.T) {
	travel := int64p(15)
	s := mustNew(t, std("std-1", func(p *laborstandard.Params) { p.TravelComponentSeconds = travel }))
	*travel = 1
	if got := s.TravelComponentSeconds(); got == nil || *got != 15 {
		t.Fatalf("TravelComponentSeconds = %v, want 15 (the caller's pointer must not alias)", got)
	}
	none := mustNew(t, std("std-2", nil))
	if none.TravelComponentSeconds() != nil || none.SiteScoped() {
		t.Errorf("a standard without travel or site = travel %v scoped %v, want nil/false", none.TravelComponentSeconds(), none.SiteScoped())
	}
}

func TestResolveTable(t *testing.T) {
	cases := []struct {
		name      string
		in        []laborstandard.Params
		site      string
		taskType  string
		wantID    string
		wantFound bool
	}{
		{name: "no candidate", site: "SIM1", taskType: "PICK"},
		{name: "site-less applies to every site", in: []laborstandard.Params{std("std-a", nil)}, site: "SIM1", taskType: "PICK", wantID: "std-a", wantFound: true},
		{name: "other task type does not apply", in: []laborstandard.Params{std("std-a", nil)}, site: "SIM1", taskType: "PACK"},
		{name: "handling-unit standard does not apply", in: []laborstandard.Params{
			std("std-case", func(p *laborstandard.Params) { p.HandlingUnit = laborstandard.HandlingUnitCase }),
		}, site: "SIM1", taskType: "PICK"},
		{name: "another site's standard does not apply", in: []laborstandard.Params{
			std("std-x", func(p *laborstandard.Params) { p.SiteCode = "SIM2" }),
		}, site: "SIM1", taskType: "PICK"},
		{name: "effective exactly at the window start applies", in: []laborstandard.Params{
			std("std-a", func(p *laborstandard.Params) { p.EffectiveFrom = windowStart }),
		}, site: "SIM1", taskType: "PICK", wantID: "std-a", wantFound: true},
		{name: "effective one second after the window start does not apply", in: []laborstandard.Params{
			std("std-a", func(p *laborstandard.Params) { p.EffectiveFrom = windowStart.Add(time.Second) }),
		}, site: "SIM1", taskType: "PICK"},
		{name: "site-specific wins over a newer site-less one", in: []laborstandard.Params{
			std("std-site", func(p *laborstandard.Params) { p.SiteCode = "SIM1" }),
			std("std-new", func(p *laborstandard.Params) { p.EffectiveFrom, p.AsOf = t0.Add(time.Hour), t0.Add(time.Hour) }),
		}, site: "SIM1", taskType: "PICK", wantID: "std-site", wantFound: true},
		{name: "site-less wins when the site-specific one is another site's", in: []laborstandard.Params{
			std("std-other", func(p *laborstandard.Params) { p.SiteCode = "SIM2" }),
			std("std-a", nil),
		}, site: "SIM1", taskType: "PICK", wantID: "std-a", wantFound: true},
		{name: "latest effective_from wins over a later as_of", in: []laborstandard.Params{
			std("std-old", func(p *laborstandard.Params) { p.AsOf = t0.Add(48 * time.Hour) }),
			std("std-rev", func(p *laborstandard.Params) { p.EffectiveFrom, p.AsOf = t0.Add(time.Hour), t0.Add(time.Hour) }),
		}, site: "SIM1", taskType: "PICK", wantID: "std-rev", wantFound: true},
		{name: "same effective_from: latest as_of wins", in: []laborstandard.Params{
			std("std-z", nil),
			std("std-b", func(p *laborstandard.Params) { p.AsOf = t0.Add(time.Second) }),
		}, site: "SIM1", taskType: "PICK", wantID: "std-b", wantFound: true},
		{name: "same effective_from and as_of: greatest id wins", in: []laborstandard.Params{
			std("std-b", nil), std("std-c", nil), std("std-a", nil),
		}, site: "SIM1", taskType: "PICK", wantID: "std-c", wantFound: true},
		{name: "a plan without a site only sees site-less standards", in: []laborstandard.Params{
			std("std-site", func(p *laborstandard.Params) { p.SiteCode = "SIM1" }),
			std("std-a", nil),
		}, site: "", taskType: "PICK", wantID: "std-a", wantFound: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			candidates := make([]laborstandard.Standard, 0, len(tc.in))
			for _, p := range tc.in {
				candidates = append(candidates, mustNew(t, p))
			}
			got, found := laborstandard.Resolve(candidates, tc.site, tc.taskType, windowStart)
			if found != tc.wantFound {
				t.Fatalf("found = %v, want %v (got %+v)", found, tc.wantFound, got)
			}
			if found && got.ID() != tc.wantID {
				t.Fatalf("resolved %q, want %q", got.ID(), tc.wantID)
			}
			if !found && got.ID() != "" {
				t.Fatalf("not found must return the zero Standard, got %+v", got)
			}
		})
	}
}

// TestResolvePrecedenceIsNotTheFirstCandidate pins every precedence rule in
// both orders: the winner is decided by the rule, never by position.
func TestResolvePrecedenceIsNotTheFirstCandidate(t *testing.T) {
	pairs := []struct {
		name          string
		winner, loser laborstandard.Params
	}{
		{"site scope", std("std-site", func(p *laborstandard.Params) { p.SiteCode = "SIM1" }), std("std-a", nil)},
		{"effective_from", std("std-new", func(p *laborstandard.Params) { p.EffectiveFrom = t0.Add(time.Minute) }), std("std-old", func(p *laborstandard.Params) { p.AsOf = t0.Add(time.Hour) })},
		{"as_of", std("std-late", func(p *laborstandard.Params) { p.AsOf = t0.Add(time.Minute) }), std("std-zz", nil)},
		{"id", std("std-b", nil), std("std-a", nil)},
	}
	for _, pair := range pairs {
		w, l := mustNew(t, pair.winner), mustNew(t, pair.loser)
		for _, order := range [][]laborstandard.Standard{{w, l}, {l, w}} {
			got, ok := laborstandard.Resolve(order, "SIM1", "PICK", windowStart)
			if !ok || got.ID() != w.ID() {
				t.Errorf("%s: resolved %q (found %v), want %q", pair.name, got.ID(), ok, w.ID())
			}
		}
	}
}

// TestResolveIgnoresArrivalOrder is the permutation test of ADR 0022
// decision 3: a fleet-wide standard, its revision, a same-instant revision
// that arrived later, a site-specific standard and its revision, a future
// revision, a handling-unit standard and another site's standard converge to
// the same standard in every one of the 8! arrival orders.
func TestResolveIgnoresArrivalOrder(t *testing.T) {
	params := []laborstandard.Params{
		std("std-1", nil),
		std("std-2", func(p *laborstandard.Params) { p.EffectiveFrom, p.AsOf = t0.Add(24*time.Hour), t0.Add(24*time.Hour) }),
		std("std-3", func(p *laborstandard.Params) { p.EffectiveFrom, p.AsOf = t0.Add(24*time.Hour), t0.Add(25*time.Hour) }),
		std("std-4", func(p *laborstandard.Params) { p.SiteCode = "SIM1"; p.ExpectedSeconds = 50 }),
		std("std-5", func(p *laborstandard.Params) {
			p.SiteCode, p.ExpectedSeconds, p.EffectiveFrom, p.AsOf = "SIM1", 41, t0.Add(48*time.Hour), t0.Add(48*time.Hour)
		}),
		std("std-6", func(p *laborstandard.Params) { p.SiteCode, p.EffectiveFrom = "SIM1", windowStart.Add(time.Hour) }),
		std("std-7", func(p *laborstandard.Params) { p.SiteCode, p.HandlingUnit = "SIM1", laborstandard.HandlingUnitPallet }),
		std("std-8", func(p *laborstandard.Params) { p.SiteCode, p.EffectiveFrom = "SIM2", t0.Add(72*time.Hour) }),
	}
	standards := make([]laborstandard.Standard, 0, len(params))
	for _, p := range params {
		standards = append(standards, mustNew(t, p))
	}
	orders := 0
	permutations(len(standards), func(order []int) {
		orders++
		arrived := make([]laborstandard.Standard, 0, len(order))
		for _, i := range order {
			arrived = append(arrived, standards[i])
		}
		site, ok := laborstandard.Resolve(arrived, "SIM1", "PICK", windowStart)
		if !ok || site.ID() != "std-5" || site.ExpectedSeconds() != 41 {
			t.Fatalf("order %v: SIM1 resolved %q (found %v), want std-5", order, site.ID(), ok)
		}
		fleet, ok := laborstandard.Resolve(arrived, "SIM3", "PICK", windowStart)
		if !ok || fleet.ID() != "std-3" {
			t.Fatalf("order %v: SIM3 resolved %q (found %v), want the latest site-less std-3", order, fleet.ID(), ok)
		}
	})
	if orders != 40320 {
		t.Fatalf("visited %d arrival orders, want 8! = 40320", orders)
	}
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
