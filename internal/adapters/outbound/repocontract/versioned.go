package repocontract

import (
	"testing"

	"github.com/claudioed/warehouse-planning/internal/application/ports"
)

// aggregate is what the versioned cases need of an aggregate.
type aggregate interface{ Version() int64 }

// subject is one versioned aggregate's repository, seen through what the
// shared cases need. key picks one of the subject's three identities;
// variant picks one of several distinct contents of an identity.
type subject[A aggregate] struct {
	// get reads identity key; found is false (and the aggregate nil) when
	// nothing is stored there.
	get  func(fx Fixture, key int) (A, bool, error)
	save func(fx Fixture, a A) error
	// first builds a version-1 aggregate of identity key holding variant.
	first func(t *testing.T, key, variant int) A
	// change applies variant to a, failing the test unless a changed (and
	// so moved to the next version).
	change func(t *testing.T, a A, variant int)
	// at rehydrates identity key holding variant 0 at version.
	at func(t *testing.T, key int, version int64) A
	// fingerprint renders every persisted field of a, its version included.
	fingerprint func(a A) string
}

type versionedCase[A aggregate] struct {
	name string
	fn   func(*testing.T, Fixture, subject[A])
}

// versionedCases is the behaviour both versioned repositories share: the
// round trip, the version guard of Save and copies in and out.
func versionedCases[A aggregate](s subject[A]) []named {
	cases := []versionedCase[A]{
		{"get of an unknown identity is not found", notFound[A]},
		{"a first save reads back whole", roundTrip[A]},
		{"a change saves at the next version", nextVersion[A]},
		{"a second first save is a version conflict", duplicateFirst[A]},
		{"a stale update is a version conflict", staleUpdate[A]},
		{"an update with no stored row, a skipped or a repeated version is a version conflict", missingOrSkipped[A]},
		{"stored and returned aggregates are copies", copies[A]},
		{"each identity is stored apart", identities[A]},
	}
	out := make([]named, 0, len(cases))
	for _, c := range cases {
		out = append(out, named{name: c.name, fn: func(t *testing.T, fx Fixture) { c.fn(t, fx, s) }})
	}
	return out
}

func mustGet[A aggregate](t *testing.T, fx Fixture, s subject[A], key int) A {
	t.Helper()
	a, found, err := s.get(fx, key)
	noErr(t, err)
	if !found {
		t.Fatalf("identity %d: nothing stored", key)
	}
	return a
}

// storedAs asserts what identity key holds now.
func storedAs[A aggregate](t *testing.T, fx Fixture, s subject[A], key int, want string) {
	t.Helper()
	eq(t, "stored aggregate", s.fingerprint(mustGet(t, fx, s, key)), want)
}

func absent[A aggregate](t *testing.T, fx Fixture, s subject[A], key int) {
	t.Helper()
	_, found, err := s.get(fx, key)
	noErr(t, err)
	if found {
		t.Fatalf("identity %d: found a stored aggregate, want none", key)
	}
}

func notFound[A aggregate](t *testing.T, fx Fixture, s subject[A]) {
	absent(t, fx, s, 0)
}

func roundTrip[A aggregate](t *testing.T, fx Fixture, s subject[A]) {
	for key, variant := range []int{1, 2} {
		a := s.first(t, key, variant)
		noErr(t, s.save(fx, a))
		storedAs(t, fx, s, key, s.fingerprint(a))
	}
}

func nextVersion[A aggregate](t *testing.T, fx Fixture, s subject[A]) {
	noErr(t, s.save(fx, s.first(t, 0, 0)))
	a := mustGet(t, fx, s, 0)
	for i, variant := range []int{1, 2} {
		s.change(t, a, variant)
		eq(t, "version after a change", a.Version(), int64(i+2))
		noErr(t, s.save(fx, a))
		storedAs(t, fx, s, 0, s.fingerprint(a))
	}
}

func duplicateFirst[A aggregate](t *testing.T, fx Fixture, s subject[A]) {
	original := s.first(t, 0, 0)
	noErr(t, s.save(fx, original))
	wantErr(t, s.save(fx, s.first(t, 0, 1)), ports.ErrVersionConflict)
	storedAs(t, fx, s, 0, s.fingerprint(original))
}

func staleUpdate[A aggregate](t *testing.T, fx Fixture, s subject[A]) {
	noErr(t, s.save(fx, s.first(t, 0, 0)))
	winner, loser := mustGet(t, fx, s, 0), mustGet(t, fx, s, 0)
	s.change(t, winner, 1)
	noErr(t, s.save(fx, winner))
	s.change(t, loser, 2)
	wantErr(t, s.save(fx, loser), ports.ErrVersionConflict)
	storedAs(t, fx, s, 0, s.fingerprint(winner))
}

func missingOrSkipped[A aggregate](t *testing.T, fx Fixture, s subject[A]) {
	wantErr(t, s.save(fx, s.at(t, 0, 2)), ports.ErrVersionConflict)
	absent(t, fx, s, 0)

	first := s.first(t, 0, 0)
	noErr(t, s.save(fx, first))
	wantErr(t, s.save(fx, s.at(t, 0, 3)), ports.ErrVersionConflict)
	storedAs(t, fx, s, 0, s.fingerprint(first))

	second := mustGet(t, fx, s, 0)
	s.change(t, second, 1)
	noErr(t, s.save(fx, second))
	wantErr(t, s.save(fx, s.at(t, 0, 2)), ports.ErrVersionConflict)
	storedAs(t, fx, s, 0, s.fingerprint(second))
}

func copies[A aggregate](t *testing.T, fx Fixture, s subject[A]) {
	a := s.first(t, 0, 0)
	noErr(t, s.save(fx, a))
	want := s.fingerprint(a)
	s.change(t, a, 1) // the saved aggregate moves on, unsaved
	storedAs(t, fx, s, 0, want)
	got := mustGet(t, fx, s, 0)
	s.change(t, got, 2) // so does a read one
	storedAs(t, fx, s, 0, want)
}

func identities[A aggregate](t *testing.T, fx Fixture, s subject[A]) {
	want := make([]string, 3)
	for key := range want {
		a := s.first(t, key, 0)
		noErr(t, s.save(fx, a))
		want[key] = s.fingerprint(a)
	}
	for key := range want {
		storedAs(t, fx, s, key, want[key])
	}
}
