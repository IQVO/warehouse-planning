package http

import (
	"context"
	"net/http"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres/pgtx"
)

// ExportIdempotent exposes the middleware to the external test package.
func (s *Server) ExportIdempotent(next http.HandlerFunc) http.HandlerFunc { return s.idempotent(next) }

// ProblemTypeForTest is one problem type of the service's problem catalogue,
// with the HTTP status it is answered with, for the replay-class tests
// (idempotency_replay_classes_integration_test.go).
type ProblemTypeForTest struct {
	Slug   string
	Status int
	Class  string
}

// ExportProblemTypesForTest returns every problem type the idempotent routes
// can answer, from the production catalogue. The replay-class rollout fills
// Class from the catalogue entry; until the class field exists every entry
// reports the empty class, which is exactly what the template's first test
// flags.
func ExportProblemTypesForTest() []ProblemTypeForTest {
	out := make([]ProblemTypeForTest, 0, 64)
	for _, entry := range problemCatalog() {
		out = append(out, ProblemTypeForTest{Slug: entry.info.slug, Status: statusFor(entry.err)})
	}
	for _, p := range standaloneProblems() {
		out = append(out, ProblemTypeForTest{Slug: p.info.slug, Status: p.status})
	}
	return out
}

// ExportRefuseForTest answers slug through the production problem writer, so
// the middleware sees exactly what a handler of that problem type answers.
func ExportRefuseForTest(w http.ResponseWriter, r *http.Request, slug string) {
	for _, entry := range problemCatalog() {
		if entry.info.slug == slug {
			writeProblem(w, statusFor(entry.err), entry.info, "replay-class test refusal", r.URL.Path)
			return
		}
	}
	for _, p := range standaloneProblems() {
		if p.info.slug == slug {
			writeProblem(w, p.status, p.info, "replay-class test refusal", r.URL.Path)
			return
		}
	}
	panic("idempotency replay-class test: unknown problem slug " + slug)
}

// ExportWriteInRequestUnit runs sql with args inside the unit of work the
// middleware opened for the request (the transaction bound into ctx), the way
// a use case's repository writes.
func ExportWriteInRequestUnit(ctx context.Context, sql string, args ...any) error {
	tx, ok := pgtx.From(ctx)
	if !ok {
		panic("idempotency replay-class test: no unit of work bound into the request context")
	}
	_, err := tx.Exec(ctx, sql, args...)
	return err
}
