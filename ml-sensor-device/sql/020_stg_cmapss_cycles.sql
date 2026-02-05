CREATE OR REPLACE TABLE stg.cmapss_cycles AS
WITH ranked AS (
  SELECT
    unit,
    cycle,
    op_setting_1, op_setting_2, op_setting_3,
    sensor_1, sensor_2, sensor_3, sensor_4, sensor_5, sensor_6, sensor_7,
    sensor_8, sensor_9, sensor_10, sensor_11, sensor_12, sensor_13, sensor_14,
    sensor_15, sensor_16, sensor_17, sensor_18, sensor_19, sensor_20, sensor_21,
    source_file,
    loaded_at,

    ROW_NUMBER() OVER (
      PARTITION BY unit, cycle, source_file
      ORDER BY loaded_at DESC
    ) AS rn
  FROM raw.cmapss_cycles
)
SELECT
  unit,
  cycle,
  op_setting_1, op_setting_2, op_setting_3,
  sensor_1, sensor_2, sensor_3, sensor_4, sensor_5, sensor_6, sensor_7,
  sensor_8, sensor_9, sensor_10, sensor_11, sensor_12, sensor_13, sensor_14,
  sensor_15, sensor_16, sensor_17, sensor_18, sensor_19, sensor_20, sensor_21,
  source_file,
  loaded_at
FROM ranked
WHERE rn = 1;
