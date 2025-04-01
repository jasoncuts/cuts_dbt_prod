
{{ config(materialized='table') }}

WITH plats AS 
(
    SELECT date
        ,{{ dbt_utils.pivot('campaign_gender', dbt_utils.get_column_values(ref('int_daily_spend_unions_platforms'), 'campaign_gender')
            , agg='sum'
            , then_value='spend'
            , suffix = '_spend')}}
    FROM {{ref('int_daily_spend_unions_platforms')}}
    GROUP BY 1
)
SELECT *
FROM plats