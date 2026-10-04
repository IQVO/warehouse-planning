package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/processpath"
)

// ProcessPathRepo is a pgxpool-backed ports.ProcessPathRepository. Inside a
// ports.UnitOfWork it uses the transaction carried by ctx (pgtx).
type ProcessPathRepo struct {
	pool *pgxpool.Pool
}

// NewProcessPathRepo constructs a ProcessPathRepo over pool.
func NewProcessPathRepo(pool *pgxpool.Pool) *ProcessPathRepo {
	return &ProcessPathRepo{pool: pool}
}

// Save upserts path under its id: an existing row's name and steps are
// replaced wholesale, exactly like the in-memory repository. Steps are
// stored in a text[] column, which preserves their order.
func (r *ProcessPathRepo) Save(ctx context.Context, path processpath.ProcessPath) error {
	steps := make([]string, 0, len(path.Steps()))
	for _, s := range path.Steps() {
		steps = append(steps, string(s))
	}
	_, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO process_paths (id, name, steps)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, steps = EXCLUDED.steps
	`, path.ID(), path.Name(), steps)
	return err
}

// FindByID returns the stored ProcessPath for id, or (nil, nil) if none has
// been registered.
func (r *ProcessPathRepo) FindByID(ctx context.Context, id string) (*processpath.ProcessPath, error) {
	var (
		name  string
		steps []string
	)
	err := queryFor(ctx, r.pool).QueryRow(ctx,
		`SELECT name, steps FROM process_paths WHERE id = $1`, id).Scan(&name, &steps)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	types := make([]processpath.ProcessType, 0, len(steps))
	for _, s := range steps {
		types = append(types, processpath.ProcessType(s))
	}
	path, err := processpath.NewProcessPath(id, name, types)
	if err != nil {
		return nil, err
	}
	return &path, nil
}

// List returns every stored ProcessPath ordered by id. Steps come back in
// their declared order (text[]), exactly as FindByID returns them.
func (r *ProcessPathRepo) List(ctx context.Context) ([]processpath.ProcessPath, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `SELECT id, name, steps FROM process_paths ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]processpath.ProcessPath, 0)
	for rows.Next() {
		var (
			id, name string
			steps    []string
		)
		if err := rows.Scan(&id, &name, &steps); err != nil {
			return nil, err
		}
		types := make([]processpath.ProcessType, 0, len(steps))
		for _, s := range steps {
			types = append(types, processpath.ProcessType(s))
		}
		path, err := processpath.NewProcessPath(id, name, types)
		if err != nil {
			return nil, err
		}
		out = append(out, path)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
