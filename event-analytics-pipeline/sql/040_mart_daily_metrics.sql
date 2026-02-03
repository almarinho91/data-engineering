CREATE OR REPLACE TABLE mart.daily_metrics AS
WITH base AS (
  SELECT
    CAST(event_time_utc AS DATE) AS date_utc,
    user_id,
    event_type
  FROM stg.events
  WHERE event_time_utc IS NOT NULL
)
SELECT
  date_utc,
  COUNT(*) AS total_events,
  COUNT(DISTINCT user_id) AS active_users,

  SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_views,
  SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END) AS clicks,
  SUM(CASE WHEN event_type = 'signup' THEN 1 ELSE 0 END) AS signups,
  SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) AS errors,

  ROUND(
    SUM(CASE WHEN event_type = 'error' THEN 1 ELSE 0 END) * 1.0
    / NULLIF(COUNT(*), 0),
    4
  ) AS error_rate
FROM base
GROUP BY date_utc
ORDER BY date_utc;
