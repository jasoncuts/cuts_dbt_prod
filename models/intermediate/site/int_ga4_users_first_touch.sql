{{ config(materialized='incremental') }}

WITH first_touch_time AS 
(
    SELECT user_id
        ,min(time) AS first_touch_time
    FROM {{ref('stg_ga4_sessions')}}
    GROUP BY 1 
),

first_touch_channel AS 
(
    SELECT f.user_id
        ,f.first_touch_time
        ,s.channel AS first_touch_channel
    FROM first_touch_time f 
    LEFT JOIN  {{ref('stg_ga4_sessions')}} s 
    ON s.user_id = f.user_id AND s.time = f.first_touch_time
)

SELECT *
FROM first_touch_channel

