package postgres

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/pagecursor"
	"github.com/claudioed/warehouse-planning/internal/domain/processflow"
)

// ProcessFlowRepo is a pgxpool-backed ports.ProcessFlowRepository over the
// process_flows table (migration 0018): the local copy of
// process-path-management's flows, one row per flow id (docs/adr/0025). Inside
// a ports.UnitOfWork it uses the transaction carried by ctx, so the consumer's
// processed-event claim and the Record commit or roll back together.
type ProcessFlowRepo struct {
	pool *pgxpool.Pool
}

// NewProcessFlowRepo constructs a ProcessFlowRepo over pool.
func NewProcessFlowRepo(pool *pgxpool.Pool) *ProcessFlowRepo {
	return &ProcessFlowRepo{pool: pool}
}

// stageJSON is the stored shape of one stage.
type stageJSON struct {
	PathID      string `json:"path_id"`
	ProcessType string `json:"process_type"`
}

// Record implements ports.ProcessFlowRepository: an UPSERT whose WHERE is the
// version guard (a stored row is replaced only by a GREATER version), so the
// affected-row count says whether this call applied the message.
func (r *ProcessFlowRepo) Record(ctx context.Context, f processflow.Flow) (bool, error) {
	stages := make([]stageJSON, 0, len(f.Stages()))
	for _, s := range f.Stages() {
		stages = append(stages, stageJSON{PathID: s.PathID, ProcessType: string(s.ProcessType)})
	}
	raw, err := json.Marshal(stages)
	if err != nil {
		return false, err
	}
	tag, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO process_flows (flow_id, name, stages, status, version, updated_at)
		VALUES ($1, $2, $3::jsonb, $4, $5, $6)
		ON CONFLICT (flow_id) DO UPDATE
		   SET name = EXCLUDED.name, stages = EXCLUDED.stages, status = EXCLUDED.status,
		       version = EXCLUDED.version, updated_at = EXCLUDED.updated_at
		 WHERE process_flows.version < EXCLUDED.version
	`, f.ID(), f.Name(), string(raw), string(f.Status()), f.Version(), f.UpdatedAt())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// FindActive implements ports.ProcessFlowRepository.
func (r *ProcessFlowRepo) FindActive(ctx context.Context, id string) (*processflow.Flow, error) {
	row := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT flow_id, name, stages, status, version, updated_at
		FROM process_flows WHERE flow_id = $1 AND status = 'ACTIVE'`, id)
	f, err := scanFlow(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &f, nil
}

// ListActivePage implements ports.ProcessFlowRepository.
func (r *ProcessFlowRepo) ListActivePage(ctx context.Context, after *pagecursor.Cursor, limit int) ([]processflow.Flow, error) {
	afterID := ""
	if after != nil {
		afterID = after.ID
	}
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT flow_id, name, stages, status, version, updated_at
		FROM process_flows
		WHERE status = 'ACTIVE' AND ($1::text = '' OR flow_id > $1::text)
		ORDER BY flow_id
		LIMIT $2`, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]processflow.Flow, 0)
	for rows.Next() {
		f, err := scanFlow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// scanFlow rebuilds a Flow from one process_flows row.
func scanFlow(row pgx.Row) (processflow.Flow, error) {
	var (
		p      processflow.Params
		status string
		raw    []byte
	)
	if err := row.Scan(&p.FlowID, &p.Name, &raw, &status, &p.Version, &p.UpdatedAt); err != nil {
		return processflow.Flow{}, err
	}
	var stages []stageJSON
	if err := json.Unmarshal(raw, &stages); err != nil {
		return processflow.Flow{}, err
	}
	for _, s := range stages {
		p.Stages = append(p.Stages, processflow.Stage{PathID: s.PathID, ProcessType: processflow.ProcessType(s.ProcessType)})
	}
	p.Status = processflow.Status(status)
	return processflow.New(p)
}
