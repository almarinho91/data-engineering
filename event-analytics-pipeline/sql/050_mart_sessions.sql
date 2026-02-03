CREATE OR REPLACE TABLE mart.sessions AS
WITH ordered AS (
    SELECT
        user_id,
        event_id,
        event_time_utc,

        LAG(event_time_utc) OVER (
            PARTITION BY user_id
            ORDER BY event_time_utc
        ) AS prev_event_time
    FROM stg.events
    WHERE user_id IS NOT NULL
      AND event_time_utc IS NOT NULL
),

flags AS (
    SELECT
        *,
        CASE
            WHEN prev_event_time IS NULL THEN 1
            WHEN event_time_utc - prev_event_time > INTERVAL '30 minutes' THEN 1
            ELSE 0
        END AS is_new_session
    FROM ordered
),

session_numbers AS (
    SELECT
        *,
        SUM(is_new_session) OVER (
            PARTITION BY user_id
            ORDER BY event_time_utc
            ROWS UNBOUNDED PRECEDING
        ) AS session_number
    FROM flags
)

SELECT
    user_id,
    session_number,

    MIN(event_time_utc) AS session_start,
    MAX(event_time_utc) AS session_end,
    COUNT(*) AS events_in_session,

    EXTRACT(EPOCH FROM MAX(event_time_utc) - MIN(event_time_utc)) AS session_duration_seconds
FROM session_numbers
GROUP BY user_id, session_number
ORDER BY user_id, session_number;
