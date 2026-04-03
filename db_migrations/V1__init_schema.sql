CREATE TABLE IF NOT EXISTS config_machine (
    id SERIAL PRIMARY KEY,
    machine_name TEXT NOT NULL UNIQUE,
    ip_connection TEXT,
    ams_net_id TEXT,
    ip_data TEXT
);

CREATE TABLE IF NOT EXISTS config_signals (
    id SERIAL PRIMARY KEY,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    poll_type TEXT,
    poll_frequency INT,
    param_type TEXT,
    signal_category TEXT,
    CONSTRAINT uniq_signal_per_machine UNIQUE (machine_name, tag_name)
);

CREATE TABLE IF NOT EXISTS telemetry_count (
    id BIGSERIAL, 
    time TIMESTAMPTZ NOT NULL,
    arrived_time TIMESTAMPTZ NOT NULL,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value BIGINT,
    evt_id VARCHAR(64)
);
SELECT create_hypertable('telemetry_count', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS telemetry_event (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    arrived_time TIMESTAMPTZ NOT NULL,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value INTEGER,
    evt_id VARCHAR(64)
);
SELECT create_hypertable('telemetry_event', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS telemetry_process (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    arrived_time TIMESTAMPTZ NOT NULL,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value DOUBLE PRECISION,
    value_str TEXT,
    evt_id VARCHAR(64)
);
SELECT create_hypertable('telemetry_process', 'time', if_not_exists => TRUE);

CREATE UNIQUE INDEX IF NOT EXISTS idx_tel_evt_uniq ON telemetry_event (time, machine_name, tag_name, evt_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tel_cnt_uniq ON telemetry_count (time, machine_name, tag_name, evt_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_tel_prc_uniq ON telemetry_process (time, machine_name, tag_name, evt_id);

CREATE INDEX IF NOT EXISTS idx_telemetry_process_machine_tag ON telemetry_process (machine_name, tag_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_event_machine_tag ON telemetry_event (machine_name, tag_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_count_machine_tag ON telemetry_count (machine_name, tag_name, time DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_hourly_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) as bucket,
    machine_name,
    tag_name,
    COUNT(*) as total_events,
    AVG(value) as avg_val,
    MAX(value) as max_val,
    MIN(value) as min_val
FROM telemetry_count
GROUP BY bucket, machine_name, tag_name
WITH NO DATA;

CREATE OR REPLACE VIEW view_mes_anomalies AS
WITH last_hour AS (
    SELECT machine_name, COUNT(*) as stops_count 
    FROM telemetry_event 
    WHERE time > NOW() - INTERVAL '1 hour' AND value = 0 
    GROUP BY machine_name
),
monthly_avg AS (
    SELECT machine_name, COUNT(*) / 14.0 as avg_daily_stops 
    FROM telemetry_event 
    WHERE time > NOW() - INTERVAL '14 days' AND value = 0
    GROUP BY machine_name
)
SELECT 
    row_number() OVER () as id,
    lh.machine_name,
    lh.stops_count as current_stops,
    COALESCE(ma.avg_daily_stops, 0) as historical_avg,
    CASE 
        WHEN lh.stops_count > (ma.avg_daily_stops / 24 * 1.5) THEN 'critical'
        ELSE 'normal' 
    END as status
FROM last_hour lh
LEFT JOIN monthly_avg ma ON lh.machine_name = ma.machine_name;

DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.compression_settings WHERE hypertable_name = 'telemetry_count') THEN
        ALTER TABLE telemetry_count SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'machine_name, tag_name',
            timescaledb.compress_orderby = 'time DESC'
        );
    END IF;
END $$;
SELECT add_compression_policy('telemetry_count', INTERVAL '14 days', if_not_exists => TRUE);

DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.compression_settings WHERE hypertable_name = 'telemetry_event') THEN
        ALTER TABLE telemetry_event SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'machine_name, tag_name',
            timescaledb.compress_orderby = 'time DESC'
        );
    END IF;
END $$;
SELECT add_compression_policy('telemetry_event', INTERVAL '14 days', if_not_exists => TRUE);

DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.compression_settings WHERE hypertable_name = 'telemetry_process') THEN
        ALTER TABLE telemetry_process SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'machine_name, tag_name',
            timescaledb.compress_orderby = 'time DESC'
        );
    END IF;
END $$;
SELECT add_compression_policy('telemetry_process', INTERVAL '14 days', if_not_exists => TRUE);