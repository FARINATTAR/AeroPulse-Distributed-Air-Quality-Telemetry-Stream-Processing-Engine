-- =======================================================
-- AeroPulse TimescaleDB & PostgreSQL Schema Initialization
-- Time-Series Hypertables, Continuous Aggregates & Partitioning
-- =======================================================

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Main Telemetry Raw Hypertable
CREATE TABLE IF NOT EXISTS air_quality_telemetry (
    id BIGSERIAL,
    city VARCHAR(50) NOT NULL,
    country VARCHAR(50) NOT NULL,
    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    aqi_us INT,
    aqi_cpcb INT,
    cpcb_category VARCHAR(30),
    pm2_5 DOUBLE PRECISION,
    pm10 DOUBLE PRECISION,
    no2 DOUBLE PRECISION,
    so2 DOUBLE PRECISION,
    co DOUBLE PRECISION,
    o3 DOUBLE PRECISION,
    temperature DOUBLE PRECISION,
    humidity DOUBLE PRECISION,
    wind_speed DOUBLE PRECISION,
    data_source VARCHAR(30) DEFAULT 'openweather',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (recorded_at, city, id)
);

-- Convert standard table to TimescaleDB hypertable partitioned by recorded_at
SELECT create_hypertable('air_quality_telemetry', 'recorded_at', if_not_exists => TRUE);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_telemetry_city_time ON air_quality_telemetry (city, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_cpcb ON air_quality_telemetry (aqi_cpcb, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_pm25 ON air_quality_telemetry (pm2_5, recorded_at DESC);

-- Materialized Continuous Aggregate View: Hourly Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_city_air_quality
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', recorded_at) AS bucket,
    city,
    country,
    AVG(aqi_cpcb)::INT AS avg_cpcb_aqi,
    MAX(aqi_cpcb) AS max_cpcb_aqi,
    AVG(pm2_5)::NUMERIC(6,2) AS avg_pm2_5,
    AVG(pm10)::NUMERIC(6,2) AS avg_pm10,
    AVG(temperature)::NUMERIC(4,1) AS avg_temperature,
    AVG(humidity)::NUMERIC(4,1) AS avg_humidity,
    COUNT(*) AS sample_count
FROM air_quality_telemetry
GROUP BY bucket, city, country;

-- Materialized Continuous Aggregate View: Daily Rollup
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_city_air_quality
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', recorded_at) AS bucket,
    city,
    country,
    AVG(aqi_cpcb)::INT AS avg_cpcb_aqi,
    MAX(aqi_cpcb) AS max_cpcb_aqi,
    AVG(pm2_5)::NUMERIC(6,2) AS avg_pm2_5,
    AVG(pm10)::NUMERIC(6,2) AS avg_pm10
FROM air_quality_telemetry
GROUP BY bucket, city, country;

-- Retention policy (automatic 90 days retention management)
SELECT add_retention_policy('air_quality_telemetry', INTERVAL '90 days', if_not_exists => TRUE);
