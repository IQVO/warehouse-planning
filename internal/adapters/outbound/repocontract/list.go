package repocontract

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
	"github.com/claudioed/warehouse-planning/internal/domain/labourplan"
)

// savePlans stores a version-1 plan of (site, shift) for every path id.
func savePlans(t *testing.T, fx Fixture, site, shift string, paths ...string) {
	t.Helper()
	for _, path := range paths {
		p, _, err := labourplan.New(planSpec(site, shift, path, 0), T0)
		noErr(t, err)
		noErr(t, fx.Plans.Save(ctx, p))
	}
}

// page lists one page of (siteA, shift1) and renders its path ids and the
// more flag.
func page(t *testing.T, fx Fixture, after string, limit int) string {
	t.Helper()
	got, more, err := fx.Plans.List(ctx, siteA, shift1, after, limit)
	noErr(t, err)
	if got == nil {
		t.Fatal("List returned a nil slice; want an empty, non-nil one")
	}
	ids := make([]string, 0, len(got))
	for _, p := range got {
		ids = append(ids, p.Spec().PathID)
	}
	return fmt.Sprintf("%v more=%v", ids, more)
}

// mixedPaths sort differently by bytes than by a natural-language collation
// ("B1" < "a2") and by number ("path-10" < "path-2").
var mixedPaths = []string{"pick-pack", "a2", "path-10", "B1", "path-2"}

var listCases = []named{
	{"an empty shift lists nothing", func(t *testing.T, fx Fixture) {
		savePlans(t, fx, siteA, shift2, pathP)
		eq(t, "empty shift", page(t, fx, "", 10), "[] more=false")
	}},
	{"plans list by path id in byte order, only those of the site and shift", func(t *testing.T, fx Fixture) {
		savePlans(t, fx, siteA, shift1, mixedPaths...)
		savePlans(t, fx, siteA, shift2, "a1")
		savePlans(t, fx, siteB, shift1, "a0")
		eq(t, "whole shift", page(t, fx, "", 10), "[B1 a2 path-10 path-2 pick-pack] more=false")
	}},
	{"after is exclusive, limit caps the page and more says whether rows follow", func(t *testing.T, fx Fixture) {
		savePlans(t, fx, siteA, shift1, mixedPaths...)
		for _, c := range []struct {
			after string
			limit int
			want  string
		}{
			{"", 2, "[B1 a2] more=true"},
			{"a2", 2, "[path-10 path-2] more=true"},
			{"path-2", 2, "[pick-pack] more=false"},
			{"pick-pack", 2, "[] more=false"},
			{"b", 2, "[path-10 path-2] more=true"}, // an after id nobody stored
			{"", 5, "[B1 a2 path-10 path-2 pick-pack] more=false"},
			{"", 0, "[] more=true"},
			{"", -1, "[] more=true"},
		} {
			eq(t, fmt.Sprintf("page after %q, limit %d", c.after, c.limit), page(t, fx, c.after, c.limit), c.want)
		}
	}},
	{"a walk visits every plan once, in order", func(t *testing.T, fx Fixture) {
		paths := []string{"p-0", "p-1", "p-2", "p-3", "p-4", "p-5", "p-6"}
		savePlans(t, fx, siteA, shift1, paths...)
		seen, pages := walk(t, fx, 3)
		eq(t, "walk", fmt.Sprint(seen), fmt.Sprint(paths))
		eq(t, "pages walked", pages, 3)
	}},
	{"a listed plan is the stored plan, as a copy", func(t *testing.T, fx Fixture) {
		p := plans.first(t, 0, 0)
		noErr(t, fx.Plans.Save(ctx, p))
		listed, _, err := fx.Plans.List(ctx, siteA, shift1, "", 1)
		noErr(t, err)
		if len(listed) != 1 {
			t.Fatalf("listed %d plans, want 1", len(listed))
		}
		eq(t, "listed plan", planFingerprint(listed[0]), planFingerprint(p))
		plans.change(t, listed[0], 1)
		storedAs(t, fx, plans, 0, planFingerprint(p))
	}},
}

// walk pages through (siteA, shift1) limit plans at a time, following the
// last path id, until more is false.
func walk(t *testing.T, fx Fixture, limit int) (seen []string, pages int) {
	t.Helper()
	after := ""
	for more := true; more; pages++ {
		if pages > 10 {
			t.Fatal("the walk does not end")
		}
		got, m, err := fx.Plans.List(ctx, siteA, shift1, after, limit)
		noErr(t, err)
		for _, p := range got {
			seen = append(seen, p.Spec().PathID)
		}
		if len(seen) > 0 {
			after = seen[len(seen)-1]
		}
		more = m
	}
	return seen, pages
}

// errInjected fails a unit of work after its writes.
var errInjected = errors.New("repocontract: injected failure after the writes")

var unitOfWorkCases = []named{
	{"a failed unit of work leaves neither aggregate behind", func(t *testing.T, fx Fixture) {
		f, p := forecasts.first(t, 0, 0), plans.first(t, 0, 0)
		err := fx.UoW.Do(ctx, func(ctx context.Context) error {
			if err := fx.Forecasts.Save(ctx, f); err != nil {
				return err
			}
			if err := fx.Plans.Save(ctx, p); err != nil {
				return err
			}
			return errInjected
		})
		wantErr(t, err, errInjected)
		absent(t, fx, forecasts, 0)
		absent(t, fx, plans, 0)
	}},
	{"a committed unit of work keeps both", func(t *testing.T, fx Fixture) {
		f, p := forecasts.first(t, 0, 0), plans.first(t, 0, 0)
		noErr(t, fx.UoW.Do(ctx, func(ctx context.Context) error {
			if err := fx.Forecasts.Save(ctx, f); err != nil {
				return err
			}
			return fx.Plans.Save(ctx, p)
		}))
		storedAs(t, fx, forecasts, 0, forecasts.fingerprint(f))
		storedAs(t, fx, plans, 0, planFingerprint(p))
	}},
	{"a version conflict inside a unit of work rolls the other write back", func(t *testing.T, fx Fixture) {
		noErr(t, fx.Forecasts.Save(ctx, forecasts.first(t, 0, 0)))
		p, duplicate := plans.first(t, 0, 0), forecasts.first(t, 0, 1)
		err := fx.UoW.Do(ctx, func(ctx context.Context) error {
			if err := fx.Plans.Save(ctx, p); err != nil {
				return err
			}
			return fx.Forecasts.Save(ctx, duplicate)
		})
		wantErr(t, err, ports.ErrVersionConflict)
		absent(t, fx, plans, 0)
	}},
}
