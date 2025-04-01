{{
        config(
            materialized='table'
        )
    }}
with product_mapping as (
select
  title,
  url,
  product
from {{ref('stg_product_title_url_mapping')}}
group by 1,2,3
),

ad_history as (
select
  ad_id,
  ad_name,
  landing_page_url,
  rank() over(partition by ad_id order by _airbyte_extracted_at desc) as rn
from {{source('tiktok', 'ad_history')}}
),

ad_latest_name as (
select
  ad_id,
  ad_name,
  landing_page_url
from ad_history
where rn = 1
),

final_tab as (
select
  date(ard.stat_time_day) as date,
  ard.ad_id,
  regexp_substr(an.landing_page_url, '^[^?]*') as landing_url,
  replace(regexp_substr(regexp_substr(an.landing_page_url, '^[^?]*'), '[^/]+/?$'), '/','') as product,
  an.ad_name,
  pm.title,
  round(sum(cast(json_extract_scalar(metrics, '$.spend') as float64)),2) AS total_spend
from {{source('tiktok', 'ads_daily')}} as ard 
left join ad_latest_name as an 
  on ard.ad_id = an.ad_id
left join product_mapping as pm 
  on pm.product = replace(regexp_substr(regexp_substr(an.landing_page_url, '^[^?]*'), '[^/]+/?$'), '/','')
group by 1,2,3,4,5,6
order by 7 desc
)

select
  *
from final_tab
order by date desc