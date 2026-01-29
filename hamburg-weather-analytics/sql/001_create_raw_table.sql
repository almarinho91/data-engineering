CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.dwd_hourly_temperature (
  station_id STRING,
  datetime_utc TIMESTAMP,
  temperature_c DOUBLE,
  humidity_pct DOUBLE,
  source_file STRING,
  ingested_at TIMESTAMP DEFAULT current_timestamp
);

CREATE VIEW IF NOT EXISTS raw.v_dwd_hourly_temperature_latest AS
SELECT *
FROM raw.dwd_hourly_temperature;