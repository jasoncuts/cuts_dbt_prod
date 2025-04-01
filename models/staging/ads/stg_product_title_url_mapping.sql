{{
        config(
            materialized='table'
        )
    }}
select
  title,
  url,
  replace(regexp_substr(url, '[^/]+/?$'), '/','') as product
from {{source('cuts_combined','product_title_url_mapping')}}
group by 1,2,3