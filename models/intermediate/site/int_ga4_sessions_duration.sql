{{ config(materialized='table') }}

WITH durations AS (
  SELECT 
    session_id,
    SUM(CASE WHEN event_table_name = 'page_view' THEN 1 ELSE 0 END) AS pages_viewed,
    MIN(time) AS start,
    MAX(time) AS last,
    TIMESTAMP_DIFF(MAX(time), MIN(time), MILLISECOND) / 1000.0 AS duration
  FROM {{ ref('stg_ga4_events') }}
  -- modify your date range here 
  GROUP BY session_id
)

SELECT *
FROM durations
