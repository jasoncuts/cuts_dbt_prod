
{{ config(materialized='table') }}

WITH plats AS 
(
    SELECT date
        ,{{ dbt_utils.pivot('platform', dbt_utils.get_column_values(ref('int_daily_spend_unions_platforms'), 'platform')
            , agg='sum'
            , then_value='spend'
            , suffix = '_tot')}}
        ,sum(spend) AS tot_spend
    FROM {{ref('int_daily_spend_unions_platforms')}}
    GROUP BY 1
)
SELECT *
FROM plats