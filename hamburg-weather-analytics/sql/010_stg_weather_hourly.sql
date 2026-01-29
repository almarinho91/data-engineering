CREATE SCHEMA IF NOT EXISTS stg;

CREATE OR REPLACE VIEW stg.weather_hourly AS
WITH ranked AS (
  SELECT
    station_id,
    datetime_utc,
    temperature_c,
    humidity_pct,
    source_file,
    ingested_at,
    ROW_NUMBER() OVER (
      PARTITION BY station_id, datetime_utc
      ORDER BY ingested_at DESC
    ) AS rn
  FROM raw.dwd_hourly_temperature
  WHERE datetime_utc IS NOT NULL
)
SELECT
  station_id,
  datetime_utc,
  temperature_c,
  humidity_pct,
  source_file,
  ingested_at
FROM ranked
WHERE rn = 1;
