-- 0012_dock_scheduling_appointments.up.sql: the inbound demand read model
-- changes its SOURCE from inbound-receiving's DockAppointmentBooked/Cancelled to
-- dock-scheduling's DockAppointmentBooked / Rescheduled / Cancelled / NoShow
-- (docs/adr/0020, superseding the source of docs/adr/0017). Additive on the data
-- and loosening on the constraints.
--
-- * direction: INBOUND or OUTBOUND, from the event. Only INBOUND rows are demand.
--   NULL while only a Rescheduled or a Cancelled has been seen (the Booked
--   fills it in later). Every row written before this migration came from
--   inbound-receiving, which only knew inbound appointments: INBOUND.
-- * version: the producer's aggregate version, the new ordering key (apply only
--   version > stored). Existing rows get 0, so any dock-scheduling event for
--   the same appointment id (the legacy importer keeps ids) wins.
-- * status gains NO_SHOW (it removes the demand, like CANCELLED).
-- * location is now the event's site_id; rows written under the retired
--   DEMAND_SITE_ID attribution keep that value until their appointment is
--   next written by dock-scheduling, whose site_id replaces it.
-- * the checks that required a window and >= 1 ASN on every BOOKED row are
--   relaxed: a Rescheduled that arrives before its Booked stores a window with
--   no direction and no ASN count yet.
ALTER TABLE inbound_appointments
    ADD COLUMN direction TEXT CHECK (direction IN ('INBOUND', 'OUTBOUND')),
    ADD COLUMN version   BIGINT NOT NULL DEFAULT 0 CHECK (version >= 0);

UPDATE inbound_appointments SET direction = 'INBOUND';

DO $$
DECLARE c record;
BEGIN
    -- The 0009 checks carry generated names; drop every CHECK except the new
    -- direction/version ones by rebuilding all of them below.
    FOR c IN SELECT conname FROM pg_constraint
             WHERE conrelid = 'inbound_appointments'::regclass AND contype = 'c'
    LOOP
        EXECUTE format('ALTER TABLE inbound_appointments DROP CONSTRAINT %I', c.conname);
    END LOOP;
END $$;

ALTER TABLE inbound_appointments
    ADD CONSTRAINT inbound_appointments_status_check    CHECK (status IN ('BOOKED', 'CANCELLED', 'NO_SHOW')),
    ADD CONSTRAINT inbound_appointments_direction_check CHECK (direction IN ('INBOUND', 'OUTBOUND')),
    ADD CONSTRAINT inbound_appointments_version_check   CHECK (version >= 0),
    ADD CONSTRAINT inbound_appointments_asn_count_check CHECK (asn_count >= 0),
    ADD CONSTRAINT inbound_appointments_window_pair     CHECK ((window_start IS NULL) = (window_end IS NULL)),
    ADD CONSTRAINT inbound_appointments_booked_window   CHECK (status <> 'BOOKED' OR window_start IS NOT NULL);

-- The only read: BOOKED INBOUND appointments of a site by door-window start.
DROP INDEX IF EXISTS idx_inbound_appointments_location_window;
CREATE INDEX idx_inbound_appointments_location_window
    ON inbound_appointments (location, window_start) WHERE status = 'BOOKED' AND direction = 'INBOUND';
