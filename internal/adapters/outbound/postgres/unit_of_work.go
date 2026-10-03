package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/adapters/outbound/postgres/pgtx"
)

// querier is the subset of pgx shared by *pgxpool.Pool and pgx.Tx that the
// repositories need, so the same SQL runs against either.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// UnitOfWork is the pgx-backed ports.UnitOfWork: Do begins one transaction,
// carries it in the ctx handed to fn (see package pgtx), commits on a nil
// return and rolls back otherwise. If ctx already carries a transaction Do
// joins it instead of beginning a nested one.
type UnitOfWork struct {
	pool *pgxpool.Pool
}

// NewUnitOfWork constructs a UnitOfWork over pool.
func NewUnitOfWork(pool *pgxpool.Pool) *UnitOfWork {
	return &UnitOfWork{pool: pool}
}

// Do implements ports.UnitOfWork.
func (u *UnitOfWork) Do(ctx context.Context, fn func(ctx context.Context) error) error {
	if _, ok := pgtx.From(ctx); ok {
		return fn(ctx)
	}

	tx, err := u.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin unit of work: %w", err)
	}
	// Rollback after a successful Commit is a harmless no-op
	// (pgx.ErrTxClosed); it also covers a panic in fn. Use a context that
	// survives cancellation of ctx so a cancelled message still rolls back.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	if err := fn(pgtx.With(ctx, tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit unit of work: %w", err)
	}
	return nil
}

// queryFor returns the transaction carried by ctx, or pool when there is
// none (repositories used outside a UnitOfWork keep their old behaviour).
func queryFor(ctx context.Context, pool *pgxpool.Pool) querier {
	if tx, ok := pgtx.From(ctx); ok {
		return tx
	}
	return pool
}

// inTx runs fn atomically. When ctx already carries a unit-of-work
// transaction fn simply runs on it -- the repo MUST NOT begin or commit its
// own (the outer unit of work owns that). When there is none, inTx begins,
// commits and rolls back its own transaction exactly as the repositories
// did before UnitOfWork existed, so REST paths are unchanged.
func inTx(ctx context.Context, pool *pgxpool.Pool, fn func(q querier) error) error {
	if tx, ok := pgtx.From(ctx); ok {
		return fn(tx)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
