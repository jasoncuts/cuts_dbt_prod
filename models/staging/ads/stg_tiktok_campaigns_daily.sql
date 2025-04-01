WITH adgroup_latest AS
(
    SELECT *
        ,ROW_NUMBER() OVER (PARTITION BY adgroup_id ORDER BY _airbyte_extracted_at DESC ) AS row_count
    FROM {{source('tiktok','adgroup_history')}}
),

campaign_latest AS 
(
    SELECT *
        ,ROW_NUMBER() OVER (PARTITION BY campaign_id  ORDER BY _airbyte_extracted_at DESC ) AS row_count
    FROM {{source('tiktok','campaign_history')}}
),

adgroup_info AS
(
    SELECT a.adgroup_id
        ,a.campaign_id
        ,a.create_time
        ,a._airbyte_extracted_at
        ,a.adgroup_name
        ,a.gender
        ,a.budget AS adgroup_budget
        ,c.campaign_name
        ,c.budget AS campaign_budget
    FROM adgroup_latest a 
    LEFT JOIN campaign_latest c
    ON a.campaign_id = c.campaign_id 
    WHERE a.row_count = 1 
    AND c.row_count = 1 
),

adgroup_spend AS 
(
    SELECT d.adgroup_id
        ,date(d.stat_time_day) AS date
        ,round(cast(json_extract_scalar(d.metrics, '$.spend') as float64),2) AS spend 
        ,round(cast(json_extract_scalar(d.metrics, '$.impressions') as float64),2) AS impressions
        ,round(cast(json_extract_scalar(d.metrics, '$.clicks') as float64),2) AS clicks
        ,i.adgroup_name
        ,i.campaign_name
        ,i.gender
        ,i.create_time
    FROM {{source('tiktok','adgroup_daily')}} d
    LEFT JOIN adgroup_info i 
    ON d.adgroup_id = i.adgroup_id
)

SELECT date
    ,'tiktok' AS platform
    ,CASE 
    WHEN gender = 'GENDER_FEMALE' AND lower(campaign_name) LIKE '%retargeting%' THEN 'wom_retargeting'
    WHEN gender = 'GENDER_FEMALE' AND lower(campaign_name) NOT LIKE '%retargeting%' THEN 'wom_prospecting'
    WHEN gender = 'GENDER_MALE' AND lower(campaign_name) LIKE '%retargeting%' THEN 'men_retargeting'
    WHEN gender = 'GENDER_MALE' AND lower(campaign_name) NOT LIKE '%retargeting%' THEN 'men_prospecting'
    WHEN gender = 'GENDER_UNLIMITED' AND lower(campaign_name) LIKE '%retargeting%' THEN 'men_mixed_retargeting'
    WHEN gender = 'GENDER_UNLIMITED' AND lower(campaign_name) NOT LIKE '%retargeting%' THEN 'men_mixed_prospecting'
    END AS campaign_type
    ,round(sum(spend),2) AS spend 
    ,sum(impressions) AS impressions 
    ,sum(clicks) AS clicks
FROM adgroup_spend
GROUP BY 1, 2, 3
ORDER BY 1 DESC 