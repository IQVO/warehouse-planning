package postgres

import (
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

// Only a unique violation of migration 0017's one-PUBLISHED-per-key index is
// a lost publish race (ports.ErrVersionConflict); the database-backed path is
// covered by the integration tests.
func TestIsOnePublishedPerKeyViolation(t *testing.T) {
	violation := &pgconn.PgError{Code: "23505", ConstraintName: onePublishedPerKey}
	for name, c := range map[string]struct {
		err  error
		want bool
	}{
		"the index's unique violation":        {violation, true},
		"the same, wrapped":                   {fmt.Errorf("save: %w", violation), true},
		"a unique violation of another index": {&pgconn.PgError{Code: "23505", ConstraintName: "capacity_plans_pkey"}, false},
		"another error naming the index":      {&pgconn.PgError{Code: "23514", ConstraintName: onePublishedPerKey}, false},
		"an error that is no Postgres error":  {errors.New("connection reset"), false},
		"no error":                            {nil, false},
	} {
		if got := isOnePublishedPerKeyViolation(c.err); got != c.want {
			t.Errorf("%s: got %v, want %v", name, got, c.want)
		}
	}
}
