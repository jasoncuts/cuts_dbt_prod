{{ config(materialized='table') }}

WITH sessions AS (
  SELECT
    DATETIME(s.time, 'America/Los_Angeles') AS time,
    {{ dbt_utils.star(from=ref('stg_ga4_pageviews'), except=["rn", "time"], relation_alias='s') }}
  FROM {{ ref('stg_ga4_pageviews') }} s
)

SELECT *
FROM sessions
WHERE time > DATETIME '2023-08-01 00:00:00'
