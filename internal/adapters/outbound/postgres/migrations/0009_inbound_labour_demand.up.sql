-- 0009_inbound_labour_demand.up.sql: the inbound labour demand read model and
-- its planning standard (docs/adr/0017-inbound-labour-demand-and-observed-
-- capacity-feedback.md). Purely additive.
--
-- inbound_appointments: ONE row per inbound-receiving dock appointment id,
-- fed by DockAppointmentBooked / DockAppointmentCancelled. The ordering guard is
-- last-writer-wins on the producer's CloudEvents `time` (as_of) with Cancelled
-- winning a tie (inbound.Resolve), so a Cancelled that arrives before its Booked
-- is stored as a TOMBSTONE: status CANCELLED, no window, no ASN count.
-- inbound-receiving's events carry no site, so `location` is the ONE configured
-- site (DEMAND_SITE_ID). An appointment is demand in a window [start, end) when
-- it is BOOKED and start <= window_start < end.
CREATE TABLE inbound_appointments (
    appointment_id TEXT        PRIMARY KEY,
    location       TEXT        NOT NULL,
    status         TEXT        NOT NULL CHECK (status IN ('BOOKED', 'CANCELLED')),
    window_start   TIMESTAMPTZ,
    window_end     TIMESTAMPTZ,
    asn_count      INTEGER     CHECK (asn_count >= 0),
    as_of          TIMESTAMPTZ NOT NULL,
    CHECK ((window_start IS NULL) = (window_end IS NULL)),
    CHECK (status <> 'BOOKED' OR (window_start IS NOT NULL AND asn_count >= 1))
);

-- The only read: BOOKED appointments of a site by door-window start.
CREATE INDEX idx_inbound_appointments_location_window
    ON inbound_appointments (location, window_start) WHERE status = 'BOOKED';

-- inbound_labour_standards: the operator-declared labour a dock appointment
-- costs at a site for a process path (never defaulted): a fixed
-- minutes_per_appointment plus an optional minutes_per_asn per covered ASN. The
-- booked event carries no units, pallets or lines.
CREATE TABLE inbound_labour_standards (
    location                TEXT             NOT NULL,
    process_path_id         TEXT             NOT NULL,
    minutes_per_appointment DOUBLE PRECISION NOT NULL CHECK (minutes_per_appointment > 0),
    minutes_per_asn         DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (minutes_per_asn >= 0),
    updated_at              TIMESTAMPTZ      NOT NULL DEFAULT now(),
    PRIMARY KEY (location, process_path_id)
);

-- capacity_plans: the dock-appointment demand a plan was created against, next to
-- (never summed into) the order demand. Informational: no published event
-- carries them. Existing plans keep 0 / 0 / false.
ALTER TABLE capacity_plans
    ADD COLUMN inbound_appointments      INTEGER          NOT NULL DEFAULT 0 CHECK (inbound_appointments >= 0),
    ADD COLUMN inbound_labour_minutes    DOUBLE PRECISION NOT NULL DEFAULT 0 CHECK (inbound_labour_minutes >= 0),
    ADD COLUMN inbound_standard_missing  BOOLEAN          NOT NULL DEFAULT false;
