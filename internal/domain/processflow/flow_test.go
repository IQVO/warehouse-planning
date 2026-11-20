package processflow

import (
	"errors"
	"testing"
	"time"
)

var at = time.Date(2026, 11, 20, 8, 0, 0, 0, time.FixedZone("x", -3*3600))

func valid() Params {
	return Params{
		FlowID: "single-unit", Name: "Single unit",
		Stages: []Stage{{"PICK", Pick}, {"REBIN", Rebin}, {"PACK", Pack}},
		Status: StatusActive, Version: 2, UpdatedAt: at,
	}
}

func TestNewKeepsStageOrderAndNormalizesTime(t *testing.T) {
	f, err := New(valid())
	if err != nil {
		t.Fatal(err)
	}
	steps := f.Steps()
	if len(steps) != 3 || steps[0] != Pick || steps[1] != Rebin || steps[2] != Pack {
		t.Fatalf("steps = %v, want PICK REBIN PACK in order", steps)
	}
	if got := f.Stages(); len(got) != 3 || got[1].PathID != "REBIN" || got[1].ProcessType != Rebin {
		t.Fatalf("stages = %v", got)
	}
	if f.UpdatedAt().Location() != time.UTC || !f.UpdatedAt().Equal(at) {
		t.Fatalf("updated_at = %v, want %v in UTC", f.UpdatedAt(), at)
	}
}

func TestNewExposesTheFlowsIdentity(t *testing.T) {
	f, _ := New(valid())
	if f.ID() != "single-unit" || f.Name() != "Single unit" || f.Version() != 2 || !f.Active() || f.Status() != StatusActive {
		t.Fatalf("unexpected flow %+v", f)
	}
}

func TestNewRejects(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Params)
		want   error
	}{
		{"no flow id", func(p *Params) { p.FlowID = "" }, ErrRequiredField},
		{"no event time", func(p *Params) { p.UpdatedAt = time.Time{} }, ErrRequiredField},
		{"no stages", func(p *Params) { p.Stages = nil }, ErrNoStages},
		{"blank stage path id", func(p *Params) { p.Stages = []Stage{{"", Pick}} }, ErrRequiredField},
		{"unknown process type", func(p *Params) { p.Stages = []Stage{{"X", "TELEPORT"}} }, ErrUnknownProcessType},
		{"lower-case process type", func(p *Params) { p.Stages = []Stage{{"X", "pick"}} }, ErrUnknownProcessType},
		{"unknown status", func(p *Params) { p.Status = "PAUSED" }, ErrUnknownStatus},
		{"empty status", func(p *Params) { p.Status = "" }, ErrUnknownStatus},
		{"version zero", func(p *Params) { p.Version = 0 }, ErrNonPositiveVersion},
		{"negative version", func(p *Params) { p.Version = -1 }, ErrNonPositiveVersion},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := valid()
			c.mutate(&p)
			if _, err := New(p); !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestNewAcceptsEveryProcessTypeOfTheClosedSet(t *testing.T) {
	for _, pt := range []ProcessType{Pick, Rebin, Pack, Slam, Dispatch, Arrival, Relocate, Replenish} {
		p := valid()
		p.Stages = []Stage{{"S", pt}}
		if _, err := New(p); err != nil {
			t.Errorf("%s rejected: %v", pt, err)
		}
	}
}

func TestRetiredFlowIsNotActive(t *testing.T) {
	p := valid()
	p.Status = StatusRetired
	f, err := New(p)
	if err != nil {
		t.Fatal(err)
	}
	if f.Active() {
		t.Fatal("a RETIRED flow must not be active")
	}
}

func TestSupersedesOnlyAGreaterVersion(t *testing.T) {
	f, _ := New(valid()) // version 2
	for stored, want := range map[int64]bool{0: true, 1: true, 2: false, 3: false} {
		if got := f.Supersedes(stored); got != want {
			t.Errorf("version 2 supersedes stored %d = %v, want %v", stored, got, want)
		}
	}
}

func TestStagesAndStepsAreDefensiveCopies(t *testing.T) {
	in := valid()
	f, _ := New(in)
	in.Stages[0].ProcessType = Slam
	f.Stages()[0].ProcessType = Slam
	f.Steps()[0] = Slam
	if f.Steps()[0] != Pick || f.Stages()[0].ProcessType != Pick {
		t.Fatal("the flow's stages were mutated through a caller's slice")
	}
}
