
{{ config(materialized='table') }}

WITH all_plats AS 
(
    SELECT *
    FROM {{ref('int_daily_spend_unions_platforms')}} 
),

by_gender AS 
(
    SELECT date
        ,case when lower(campaign_type) like '%wom%' then 'wom'
        else 'men'
        end as campaign_gender
        ,platform 
        ,sum(spend) AS spend
        ,sum(impressions) AS impressions
        ,sum(clicks) AS clicks  
    FROM all_plats 
    GROUP BY 1,2,3 
)

SELECT *
FROM by_gender 
ORDER BY date DESC