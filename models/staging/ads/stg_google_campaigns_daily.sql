select
   segments_date
    ,'google' AS platform
    ,CASE WHEN  lower(campaign_name) LIKE '%women%' AND lower(segments_ad_network_type) LIKE 'youtube%' THEN 'wom_youtube'
    WHEN  lower(campaign_name) LIKE '%women%' AND lower(segments_ad_network_type) LIKE 'search%' AND lower(campaign_name) LIKE '%brand%'   THEN 'wom_branded_search'
    WHEN  lower(campaign_name) LIKE '%women%' AND lower(segments_ad_network_type) LIKE 'search%' AND lower(campaign_name) NOT LIKE '%brand%' THEN 'wom_nonbranded_search'
    WHEN  lower(campaign_name) LIKE '%women%' AND lower(segments_ad_network_type) LIKE '%mixed%' THEN 'wom_mixed'
    WHEN  lower(campaign_name) LIKE '%women%' AND lower(segments_ad_network_type) LIKE '%content%' THEN 'wom_display'
    WHEN  lower(campaign_name) NOT LIKE '%women%' AND lower(segments_ad_network_type) LIKE 'youtube%' THEN 'men_youtube'
    WHEN  lower(campaign_name) NOT LIKE '%women%' AND lower(segments_ad_network_type) LIKE 'search%' AND lower(campaign_name) LIKE '%brand%'  THEN 'men_branded_search'
    WHEN  lower(campaign_name) NOT LIKE '%women%' AND lower(segments_ad_network_type) LIKE 'search%' AND lower(campaign_name) NOT LIKE '%brand%' THEN 'men_nonbranded_search'
    WHEN  lower(campaign_name) NOT LIKE '%women%' AND lower(segments_ad_network_type) LIKE '%mixed%' THEN 'men_mixed'
    WHEN  lower(campaign_name) NOT LIKE '%women%' AND lower(segments_ad_network_type) LIKE '%content%' THEN 'men_display'
    END AS campaign_type
    ,cast(sum(metrics_cost_micros)/1000000 as float64) AS spend 
    ,sum(metrics_impressions) AS impressions
    ,sum(metrics_clicks) AS clicks
from {{source('google','campaigns_daily')}}
group by 1,2,3
