{{ config(materialized='table') }}

WITH sessions AS (
  SELECT 
    DATE_TRUNC(time, DAY) AS date,
    COALESCE(channel, 'Direct') AS channel,
    CONCAT(utm_source, '/', utm_medium) AS source_medium,
    utm_campaign,
    device_type,
    country,
    landing_page,
    SUM(duration) AS total_time,
    SUM(pages_viewed) AS pages,
    COUNT(DISTINCT session_id) AS sessions,
    SUM(CASE WHEN pages_viewed <= 1 THEN 1 ELSE 0 END) AS bounces,
    COUNT(DISTINCT user_id) AS users,
    SUM(CASE WHEN converted IS TRUE THEN 1 ELSE 0 END) AS conversions,
    SUM(order_revenue) AS order_revenue
  FROM {{ ref('mart_sessions') }} p 
  GROUP BY 1,2,3,4,5,6,7
)

SELECT *
FROM sessions
