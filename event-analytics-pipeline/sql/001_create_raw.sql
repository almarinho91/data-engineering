CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.events (
  event_id STRING,
  event_time_utc TIMESTAMP,
  ingested_at_utc TIMESTAMP,
  user_id STRING,
  event_type STRING,
  page STRING,
  referrer STRING,
  device STRING,
  country STRING,
  error_code STRING,

  source_file STRING,
  loaded_at TIMESTAMP DEFAULT current_timestamp
);