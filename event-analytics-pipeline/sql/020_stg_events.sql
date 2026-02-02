CREATE OR REPLACE TABLE stg.events AS
WITH ranked AS (
    SELECT
        event_id,
        CAST(event_time_utc AS TIMESTAMP) AS event_time_utc,
        CAST(ingested_at_utc AS TIMESTAMP) AS ingested_at_utc,
        user_id,
        LOWER(event_type) AS event_type,
        page,
        referrer,
        device,
        country,
        error_code,
        source_file,
        loaded_at,

        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY loaded_at DESC
        ) AS rn
    FROM raw.events
    WHERE event_id IS NOT NULL
)
SELECT
    event_id,
    event_time_utc,
    ingested_at_utc,
    user_id,
    event_type,
    page,
    referrer,
    device,
    country,
    error_code,
    source_file,
    loaded_at
FROM ranked
WHERE rn = 1;
