

SELECT date(case when post_date = '(null)' then null else post_date end) AS date
    ,'influencer' AS platform 
    ,CASE WHEN lower(spend_type) LIKE '%(w)%' THEN 'wom_prospecting'
        ELSE 'men_prospecting' END AS campaign_type
    ,sum(coalesce(cast(case when spend = '(null)' then '0' else spend end as float64),0)) AS spend 
    ,sum(0) AS impressions
    ,sum(0) AS clicks
FROM {{source('influencer','daily_spend')}}
GROUP BY 1,2,3