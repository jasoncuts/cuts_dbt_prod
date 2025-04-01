{{ config(materialized='table') }}

WITH this_year AS (
    SELECT *
    FROM {{ ref('int_pacing_master') }}
),

last_year AS (
    SELECT *
    FROM {{ ref('int_pacing_master_ly') }}
),

pacing AS (
    SELECT 
        t.*,
        {{ dbt_utils.star(
            ref('int_pacing_master_ly'),
            except=['date'],
            relation_alias='l',
            prefix='ly_'
        ) }}
    FROM this_year t 
    LEFT JOIN last_year l
        ON t.date = l.date
)

SELECT *
FROM pacing
