
{{ config(materialized='table')}}

WITH sessions AS 
(
    SELECT
        {{dbt_utils.star(from=ref('stg_ga4_sessions'), except=["rn"], relation_alias = 's')}}
    FROM {{ ref('stg_ga4_sessions')}} s 
)

SELECT *
FROM sessions