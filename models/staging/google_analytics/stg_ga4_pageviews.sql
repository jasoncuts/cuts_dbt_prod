WITH pageviews AS
(
    SELECT TIMESTAMP_MICROS(event_timestamp) time
        ,concat(user_pseudo_id, concat('.',param.value.int_value)) AS session_id
        ,e.user_pseudo_id AS user_id
        ,lower(platform) AS library
        ,e.device.operating_system_version AS platform
        ,e.device.category AS device_type
       ,e.device.web_info.browser||' '||INITCAP(e.device.category) AS browser_type
        ,e.geo.country AS country
        ,e.geo.region AS region 
        ,e.geo.city AS city
        ,NULL AS ip
        ,param_page_reffer.value.string_value AS referrer 
        ,split(param_page_location.value.string_value,'?')[safe_offset(0)] AS page_path
        ,split(param_page_location.value.string_value,'?')[safe_offset(1)] AS page_path_query
        ,e.traffic_source.source AS utm_source
        ,e.traffic_source.medium AS utm_medium
        ,row_number() OVER (PARTITION BY concat(user_pseudo_id, concat('.',param.value.int_value)) ORDER BY TIMESTAMP_MICROS(event_timestamp)) AS rn
    FROM {{source('ga4','event')}}  e 
    CROSS JOIN UNNEST(event_params) AS param 
    CROSS JOIN UNNEST(event_params) AS param_page_reffer
    CROSS JOIN UNNEST(event_params) AS param_page_location
    WHERE event_name = 'page_view' and param.key = 'ga_session_id' and param_page_reffer.key = 'page_referrer' and param_page_location.key = 'page_location'


)

SELECT *
    ,{{ga_utm_logic()}} 
FROM pageviews 