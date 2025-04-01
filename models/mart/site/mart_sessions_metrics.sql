
{{ config(materialized='table') }}

WITH sessions AS 
(
    SELECT *
    FROM {{ref('int_sessions_metrics_agg_by_day')}}
)

SELECT *
FROM sessions