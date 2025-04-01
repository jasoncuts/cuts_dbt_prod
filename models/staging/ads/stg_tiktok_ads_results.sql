WITH ads AS 
(
    SELECT ad_id
        ,adgroup_id
        ,campaign_id
        ,ad_name
        --,status
        --,opt_status
        ,ad_text
       -- ,split_part(landing_page_url,'?', 1) AS landing_page
        ,display_name
        ,row_number() over (partition by ad_id order by _airbyte_extracted_at desc) = 1 as is_most_recent_record
    FROM {{source('tiktok','ad_history')}} ah 
),

adgroup AS 
(
    SELECT adgroup_id
        ,adgroup_name
        ,gender
       -- ,external_action
        ,placement_type
       -- ,optimize_goal
        ,billing_event
        ,bid_type
       -- ,status
        --,opt_status
        ,row_number() over (partition by adgroup_id order by _airbyte_extracted_at desc) = 1 as is_most_recent_record
    FROM {{source('tiktok','adgroup_history')}}
),

campaigns AS 
(
    SELECT campaign_id
        ,campaign_name
        ,campaign_type
        ,objective_type
        --,status AS campaign_status
        ,row_number() over (partition by campaign_id order by _airbyte_extracted_at desc) = 1 as is_most_recent_record
    FROM {{source('tiktok','campaign_history')}}
),

info AS 
(
    SELECT a.*
        ,g.gender
        ,CASE 
        WHEN g.gender = 'GENDER_FEMALE' AND lower(campaign_name) LIKE '%retargeting%' THEN 'wom_retargeting'
        WHEN g.gender = 'GENDER_FEMALE' AND lower(campaign_name) NOT LIKE '%retargeting%' THEN 'wom_prospecting'
        WHEN g.gender = 'GENDER_MALE' AND lower(campaign_name) LIKE '%retargeting%' THEN 'men_retargeting'
        WHEN g.gender = 'GENDER_MALE' AND lower(campaign_name) NOT LIKE '%retargeting%' THEN 'men_prospecting'
        WHEN g.gender = 'GENDER_UNLIMITED' AND lower(campaign_name) LIKE '%retargeting%' THEN 'men_mixed_retargeting'
        WHEN g.gender = 'GENDER_UNLIMITED' AND lower(campaign_name) NOT LIKE '%retargeting%' THEN 'men_mixed_prospecting'
        ELSE 'men_prospecting'
        END AS campaign_type
        ,c.campaign_name
    FROM ads a 
    LEFT JOIN adgroup g 
    ON a.adgroup_id = g.adgroup_id
    LEFT JOIN campaigns c 
    ON a.campaign_id = c.campaign_id
    WHERE a.is_most_recent_record IS TRUE 
    AND g.is_most_recent_record IS TRUE 
    AND c.is_most_recent_record IS TRUE 
)

SELECT *
FROM info