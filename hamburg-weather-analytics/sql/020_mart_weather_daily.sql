CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE TABLE mart.weather_daily AS
SELECT
  station_id,
  CAST(datetime_utc AS DATE) AS date_utc,
  COUNT(*) AS hours_count,
  MIN(temperature_c) AS temp_min_c,
  MAX(temperature_c) AS temp_max_c,
  AVG(temperature_c) AS temp_avg_c,
  AVG(humidity_pct) AS humidity_avg_pct
FROM stg.weather_hourly
GROUP BY 1, 2
ORDER BY date_utc;
