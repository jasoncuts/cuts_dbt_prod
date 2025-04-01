SELECT date(stat_time_day) AS date
    ,'tiktok' AS platform
    ,round(sum(cast(json_extract_scalar(metrics, '$.spend') as float64)),2) AS spend 
    ,round(sum(cast(json_extract_scalar(metrics, '$.impressions') as float64)),2) AS impressions
    ,round(sum(cast(json_extract_scalar(metrics, '$.clicks') as float64)),2) AS clicks
FROM {{source('tiktok','adgroup_daily')}}
GROUP BY 1