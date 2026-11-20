-- 0012_dock_scheduling_appointments.down.sql: restore the inbound-receiving
-- shaped table. NO_SHOW rows become CANCELLED, OUTBOUND rows and rows with an
-- unknown direction are dropped (the old source never had them), and BOOKED
-- rows lacking an ASN count get 1 so the 0009 checks hold.
DELETE FROM inbound_appointments WHERE direction IS DISTINCT FROM 'INBOUND' AND status = 'BOOKED';
UPDATE inbound_appointments SET status = 'CANCELLED' WHERE status = 'NO_SHOW';
UPDATE inbound_appointments SET asn_count = 1 WHERE status = 'BOOKED' AND COALESCE(asn_count, 0) < 1;
DELETE FROM inbound_appointments WHERE direction = 'OUTBOUND';

DROP INDEX IF EXISTS idx_inbound_appointments_location_window;

DO $$
DECLARE c record;
BEGIN
    FOR c IN SELECT conname FROM pg_constraint
             WHERE conrelid = 'inbound_appointments'::regclass AND contype = 'c'
    LOOP
        EXECUTE format('ALTER TABLE inbound_appointments DROP CONSTRAINT %I', c.conname);
    END LOOP;
END $$;

ALTER TABLE inbound_appointments
    DROP COLUMN direction,
    DROP COLUMN version;

ALTER TABLE inbound_appointments
    ADD CONSTRAINT inbound_appointments_status_check CHECK (status IN ('BOOKED', 'CANCELLED')),
    ADD CONSTRAINT inbound_appointments_asn_count_check CHECK (asn_count >= 0),
    ADD CONSTRAINT inbound_appointments_window_pair CHECK ((window_start IS NULL) = (window_end IS NULL)),
    ADD CONSTRAINT inbound_appointments_booked_complete CHECK (status <> 'BOOKED' OR (window_start IS NOT NULL AND asn_count >= 1));

CREATE INDEX idx_inbound_appointments_location_window
    ON inbound_appointments (location, window_start) WHERE status = 'BOOKED';
