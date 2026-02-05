CREATE OR REPLACE TABLE features.engine_features AS
WITH base AS (
  SELECT
    unit,
    cycle,

    -- choose a subset of sensors
    sensor_2,
    sensor_3,
    sensor_4,
    sensor_7,
    sensor_11,
    sensor_12,
    sensor_15,
    sensor_20,
    sensor_21

  FROM stg.cmapss_cycles
),

-- label: RUL = max(cycle for unit) - current cycle
labeled AS (
  SELECT
    b.*,
    (MAX(cycle) OVER (PARTITION BY unit) - cycle) AS rul
  FROM base b
),

feats AS (
  SELECT
    unit,
    cycle,
    rul,

    -- raw sensor values
    sensor_2, sensor_3, sensor_4, sensor_7, sensor_11, sensor_12, sensor_15, sensor_20, sensor_21,

    -- deltas (change since previous cycle)
    sensor_2 - LAG(sensor_2) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_2_delta,
    sensor_3 - LAG(sensor_3) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_3_delta,
    sensor_4 - LAG(sensor_4) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_4_delta,
    sensor_7 - LAG(sensor_7) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_7_delta,
    sensor_11 - LAG(sensor_11) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_11_delta,
    sensor_12 - LAG(sensor_12) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_12_delta,
    sensor_15 - LAG(sensor_15) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_15_delta,
    sensor_20 - LAG(sensor_20) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_20_delta,
    sensor_21 - LAG(sensor_21) OVER (PARTITION BY unit ORDER BY cycle) AS sensor_21_delta,

    -- rolling mean (window=5 cycles)
    AVG(sensor_2) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_2_rollmean_5,
    AVG(sensor_3) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_3_rollmean_5,
    AVG(sensor_4) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_4_rollmean_5,
    AVG(sensor_7) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_7_rollmean_5,
    AVG(sensor_11) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_11_rollmean_5,
    AVG(sensor_12) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_12_rollmean_5,
    AVG(sensor_15) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_15_rollmean_5,
    AVG(sensor_20) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_20_rollmean_5,
    AVG(sensor_21) OVER (PARTITION BY unit ORDER BY cycle ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS sensor_21_rollmean_5

  FROM labeled
)

SELECT *
FROM feats;
