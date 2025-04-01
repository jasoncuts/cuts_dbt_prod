
{{ config(materialized='table') }}

WITH facebook AS 
(
    SELECT *
    FROM {{ref('stg_facebook_campaigns_daily')}}
),

tiktok AS 
(
    SELECT *
    FROM {{ref('stg_tiktok_campaigns_daily')}}
),

google AS 
(
    SELECT *
    FROM {{ref('stg_google_campaigns_daily')}}
),

influencer AS 
(
    SELECT *
    FROM {{ref('stg_influencer_campaigns_daily')}}
),

all_plat AS 
(
    {{ dbt_utils.union_relations(
        relations=[ref('stg_facebook_campaigns_daily'),ref('stg_tiktok_campaigns_daily'),ref('stg_google_campaigns_daily'),ref('stg_influencer_campaigns_daily')]
    ) }}
)

SELECT *
    ,concat(platform, concat('_',campaign_type)) AS platform_breakout
    ,case when lower(campaign_type) like '%retargeting%' or lower(campaign_type) like '%winback%' or lower(campaign_type) like '%_branded%' then 'returning'
    else 'new'
    end as campaign_targ
    ,concat(platform, concat('_',case when lower(campaign_type) like '%retargeting%' or lower(campaign_type) like '%winback%' or lower(campaign_type) like '%_branded%' then 'returning'
    else 'new'
    end)) AS platform_targ
    ,case when campaign_type like '%wom%' then 'wom'
    else 'men'
    end as campaign_gender
FROM all_plat