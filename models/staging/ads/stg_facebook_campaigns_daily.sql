
SELECT  date_start as date
    ,'facebook' AS platform
    ,CASE WHEN lower(ad_name) LIKE '%women%' AND (lower(campaign_name) LIKE '%retarget%' OR lower(campaign_name) LIKE '%consider%' AND lower(campaign_name) NOT LIKE '%winback%') THEN 'wom_retargeting'
    WHEN lower(ad_name) LIKE '%women%' AND (lower(campaign_name) LIKE '%winback%' OR lower(campaign_name) LIKE '%past%') THEN 'wom_winback'
    WHEN lower(ad_name) NOT LIKE '%women%' AND (lower(campaign_name) LIKE '%retarget%' OR lower(campaign_name) LIKE '%consider%' AND lower(campaign_name) NOT LIKE '%winback%') THEN 'men_retargeting'
    WHEN lower(ad_name) NOT LIKE '%women%' AND (lower(campaign_name) LIKE '%winback%' OR lower(campaign_name) LIKE '%past%') THEN 'men_winback'
    ELSE
        CASE WHEN lower(ad_name) LIKE '%women%' THEN 'wom_prospecting'
        ELSE 'men_prospecting' END 
    END AS campaign_type
    ,round(sum(spend),2) AS spend 
    ,sum(impressions) AS impressions 
    ,sum(inline_link_clicks) AS clicks 
FROM  {{source('facebook','ads_daily')}}
GROUP BY 1,2,3 
ORDER BY 1 ASC
