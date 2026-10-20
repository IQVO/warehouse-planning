package postgres

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/claudioed/warehouse-planning/internal/domain/inbound"
)

// InboundAppointmentRepo is a pgxpool-backed ports.InboundAppointmentRepository
// over the inbound_appointments table (migration 0009). Inside a
// ports.UnitOfWork it uses the transaction carried by ctx, so the consumer's
// processed-event claim and Apply commit or roll back together.
type InboundAppointmentRepo struct {
	pool *pgxpool.Pool
}

// NewInboundAppointmentRepo constructs an InboundAppointmentRepo over pool.
func NewInboundAppointmentRepo(pool *pgxpool.Pool) *InboundAppointmentRepo {
	return &InboundAppointmentRepo{pool: pool}
}

// Apply implements ports.InboundAppointmentRepository. The decision is
// inbound.Resolve -- the SAME function the in-memory adapter uses -- over the
// stored row read under a transaction-scoped advisory lock keyed on the
// appointment id: the row may not exist yet (a Cancelled before its Booked), so
// FOR UPDATE alone would not serialize two concurrent first writes.
func (r *InboundAppointmentRepo) Apply(ctx context.Context, next inbound.Appointment) (bool, error) {
	changed := false
	err := inTx(ctx, r.pool, func(q querier) error {
		if _, err := q.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "inbound_appointments:"+next.ID()); err != nil {
			return err
		}
		prev, err := findInboundAppointment(ctx, q, next.ID())
		if err != nil {
			return err
		}
		resolved, ok := inbound.Resolve(prev, next)
		if !ok {
			return nil
		}
		var windowStart, windowEnd *time.Time
		var asns *int
		if resolved.HasWindow() {
			ws, we, n := resolved.WindowStart(), resolved.WindowEnd(), resolved.ASNCount()
			windowStart, windowEnd, asns = &ws, &we, &n
		}
		if _, err := q.Exec(ctx, `
			INSERT INTO inbound_appointments (appointment_id, location, status, window_start, window_end, asn_count, as_of)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (appointment_id) DO UPDATE SET
				location = EXCLUDED.location,
				status = EXCLUDED.status,
				window_start = EXCLUDED.window_start,
				window_end = EXCLUDED.window_end,
				asn_count = EXCLUDED.asn_count,
				as_of = EXCLUDED.as_of
		`, resolved.ID(), resolved.Location(), string(resolved.Status()), windowStart, windowEnd, asns, resolved.AsOf()); err != nil {
			return err
		}
		changed = true
		return nil
	})
	if err != nil {
		return false, err
	}
	return changed, nil
}

func findInboundAppointment(ctx context.Context, q querier, id string) (*inbound.Appointment, error) {
	var (
		location, status       string
		windowStart, windowEnd *time.Time
		asns                   *int
		asOf                   time.Time
	)
	err := q.QueryRow(ctx, `
		SELECT location, status, window_start, window_end, asn_count, as_of
		FROM inbound_appointments WHERE appointment_id = $1
	`, id).Scan(&location, &status, &windowStart, &windowEnd, &asns, &asOf)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	p := inbound.RehydrateParams{AppointmentID: id, Location: location, Status: inbound.Status(status), AsOf: asOf}
	if windowStart != nil && windowEnd != nil {
		p.WindowStart, p.WindowEnd = *windowStart, *windowEnd
	}
	if asns != nil {
		p.ASNCount = *asns
	}
	a := inbound.Rehydrate(p)
	return &a, nil
}

// Demand implements ports.InboundAppointmentRepository. The window is
// half-open on the door-window start ([start, end)), matching
// inbound.Appointment.CountsIn; as_of is the site's newest event time
// regardless of the window or the status.
func (r *InboundAppointmentRepo) Demand(ctx context.Context, location string, start, end time.Time) (inbound.Demand, error) {
	var (
		appointments int64
		asns         int64
		asOf         *time.Time
	)
	err := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT count(*) FILTER (WHERE status = 'BOOKED' AND window_start >= $2 AND window_start < $3),
		       COALESCE(sum(asn_count) FILTER (WHERE status = 'BOOKED' AND window_start >= $2 AND window_start < $3), 0),
		       max(as_of)
		FROM inbound_appointments WHERE location = $1
	`, location, start, end).Scan(&appointments, &asns, &asOf)
	if err != nil {
		return inbound.Demand{}, err
	}
	d := inbound.Demand{Appointments: int(appointments), ASNs: int(asns)}
	if asOf != nil {
		d.AsOf = asOf.UTC()
	}
	return d, nil
}

// InboundStandardRepo is a pgxpool-backed ports.InboundStandardRepository
// (table inbound_labour_standards, migration 0009). Inside a ports.UnitOfWork
// it uses the transaction carried by ctx (pgtx).
type InboundStandardRepo struct {
	pool *pgxpool.Pool
}

// NewInboundStandardRepo constructs an InboundStandardRepo over pool.
func NewInboundStandardRepo(pool *pgxpool.Pool) *InboundStandardRepo {
	return &InboundStandardRepo{pool: pool}
}

// Save upserts standard under its (location, process_path_id) key; an existing
// row's figures are replaced and updated_at refreshed.
func (r *InboundStandardRepo) Save(ctx context.Context, standard inbound.LabourStandard) error {
	_, err := queryFor(ctx, r.pool).Exec(ctx, `
		INSERT INTO inbound_labour_standards (location, process_path_id, minutes_per_appointment, minutes_per_asn, updated_at)
		VALUES ($1, $2, $3, $4, now())
		ON CONFLICT (location, process_path_id) DO UPDATE SET
			minutes_per_appointment = EXCLUDED.minutes_per_appointment,
			minutes_per_asn = EXCLUDED.minutes_per_asn,
			updated_at = now()
	`, standard.Location(), standard.ProcessPathID(), standard.MinutesPerAppointment(), standard.MinutesPerASN())
	return err
}

// Find returns the standard declared for location + processPathID, or (nil, nil).
func (r *InboundStandardRepo) Find(ctx context.Context, location, processPathID string) (*inbound.LabourStandard, error) {
	row := queryFor(ctx, r.pool).QueryRow(ctx, `
		SELECT location, process_path_id, minutes_per_appointment, minutes_per_asn
		FROM inbound_labour_standards WHERE location = $1 AND process_path_id = $2
	`, location, processPathID)
	standard, err := scanInboundStandard(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return &standard, nil
}

// List returns the standards of location (every standard when location is
// empty) ordered by location, then process path id.
func (r *InboundStandardRepo) List(ctx context.Context, location string) ([]inbound.LabourStandard, error) {
	rows, err := queryFor(ctx, r.pool).Query(ctx, `
		SELECT location, process_path_id, minutes_per_appointment, minutes_per_asn
		FROM inbound_labour_standards
		WHERE $1 = '' OR location = $1
		ORDER BY location, process_path_id
	`, location)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []inbound.LabourStandard{}
	for rows.Next() {
		standard, err := scanInboundStandard(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, standard)
	}
	return out, rows.Err()
}

func scanInboundStandard(row pgx.Row) (inbound.LabourStandard, error) {
	var (
		location, processPathID string
		perAppointment, perASN  float64
	)
	if err := row.Scan(&location, &processPathID, &perAppointment, &perASN); err != nil {
		return inbound.LabourStandard{}, err
	}
	return inbound.NewLabourStandard(location, processPathID, perAppointment, perASN)
}
