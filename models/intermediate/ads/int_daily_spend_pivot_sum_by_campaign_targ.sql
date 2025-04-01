
{{ config(materialized='table') }}

WITH plats AS 
(
    SELECT date
        {# ,campaign_gender AS gender #}
        ,{{ dbt_utils.pivot('platform_targ', dbt_utils.get_column_values(ref('int_daily_spend_unions_platforms'), 'platform_targ')
            , agg='sum'
            , then_value='spend')}}
    FROM {{ref('int_daily_spend_unions_platforms')}}
    GROUP BY 1
)
SELECT *
FROM plats