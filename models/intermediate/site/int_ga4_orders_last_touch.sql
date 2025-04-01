{{ config(materialized='table') }}

WITH last_touch_channel AS 
(
    SELECT o.session_id
        ,o.user_id
        ,o.order_id
        ,o.time AS order_session_time
        ,s.channel AS last_touch_channel
        ,s.paid_platform
    FROM {{ref('stg_ga4_orders')}} o 
    LEFT JOIN {{ref("stg_ga4_sessions")}} s
    ON o.session_id = s.session_id
)

SELECT *
FROM last_touch_channel