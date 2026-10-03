package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/application/tally"
)

// StorageTallyRepo is a pgxpool-backed implementation of
// ports.StorageTallyRepository (Phase 3 facility-layout read model).
type StorageTallyRepo struct {
	pool *pgxpool.Pool
}

// NewStorageTallyRepo constructs a StorageTallyRepo over pool.
func NewStorageTallyRepo(pool *pgxpool.Pool) *StorageTallyRepo {
	return &StorageTallyRepo{pool: pool}
}

// RegisterSlot increments by 1 every tally bucket named by tallyKeys
// under (zoneID, tallyType) and remembers locationCode's contribution,
// all inside one transaction (the ctx's, when inside a
// ports.UnitOfWork; its own otherwise). A locationCode already registered is a
// no-op (returns nil, nil, nothing incremented).
func (r *StorageTallyRepo) RegisterSlot(ctx context.Context, locationCode, zoneID, tallyType string, tallyKeys []string) ([]tally.Update, error) {
	var updates []tally.Update
	err := inTx(ctx, r.pool, func(q querier) error {
		var err error
		updates, err = registerSlot(ctx, q, locationCode, zoneID, tallyType, tallyKeys)
		return err
	})
	if err != nil {
		return nil, err
	}
	return updates, nil
}

func registerSlot(ctx context.Context, q querier, locationCode, zoneID, tallyType string, tallyKeys []string) ([]tally.Update, error) {
	tag, err := q.Exec(ctx, `
		INSERT INTO location_slot_registration (location_code, zone_id, tally_type, tally_keys)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (location_code) DO NOTHING
	`, locationCode, zoneID, tallyType, tallyKeys)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		// Already registered -- nothing to increment. The caller logs
		// this as a skipped re-registration.
		return nil, nil
	}

	updates := make([]tally.Update, 0, len(tallyKeys))
	for _, key := range tallyKeys {
		var count int
		err := q.QueryRow(ctx, `
			INSERT INTO location_slot_tally (zone_id, tally_type, tally_key, count)
			VALUES ($1, $2, $3, 1)
			ON CONFLICT (zone_id, tally_type, tally_key)
			DO UPDATE SET count = location_slot_tally.count + 1
			RETURNING count
		`, zoneID, tallyType, key).Scan(&count)
		if err != nil {
			return nil, err
		}
		updates = append(updates, tally.Update{ZoneID: zoneID, TallyType: tallyType, TallyKey: key, Count: count})
	}
	return updates, nil
}

// DecommissionSlot decrements (floored at 0) every tally bucket
// locationCode previously registered against and forgets the
// registration, all inside one transaction (the ctx's, when inside a
// ports.UnitOfWork). found=false means locationCode was never registered.
func (r *StorageTallyRepo) DecommissionSlot(ctx context.Context, locationCode string) ([]tally.Update, bool, error) {
	var (
		updates []tally.Update
		found   bool
	)
	err := inTx(ctx, r.pool, func(q querier) error {
		var err error
		updates, found, err = decommissionSlot(ctx, q, locationCode)
		return err
	})
	if err != nil {
		return nil, false, err
	}
	return updates, found, nil
}

func decommissionSlot(ctx context.Context, q querier, locationCode string) ([]tally.Update, bool, error) {
	var (
		zoneID    string
		tallyType string
		tallyKeys []string
	)
	err := q.QueryRow(ctx, `
		DELETE FROM location_slot_registration
		WHERE location_code = $1
		RETURNING zone_id, tally_type, tally_keys
	`, locationCode).Scan(&zoneID, &tallyType, &tallyKeys)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}

	updates := make([]tally.Update, 0, len(tallyKeys))
	for _, key := range tallyKeys {
		var count int
		err := q.QueryRow(ctx, `
			UPDATE location_slot_tally
			SET count = GREATEST(count - 1, 0)
			WHERE zone_id = $1 AND tally_type = $2 AND tally_key = $3
			RETURNING count
		`, zoneID, tallyType, key).Scan(&count)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				// No tally row exists for this key (should not normally
				// happen -- a registration always increments its own
				// tally row first) -- treat as already at the floor
				// rather than failing the whole decommission.
				count = 0
			} else {
				return nil, false, err
			}
		}
		updates = append(updates, tally.Update{ZoneID: zoneID, TallyType: tallyType, TallyKey: key, Count: count})
	}
	return updates, true, nil
}
