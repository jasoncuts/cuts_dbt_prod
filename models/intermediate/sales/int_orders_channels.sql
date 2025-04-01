
{{ config(materialized='table') }}


WITH orders AS 
(
    SELECT o.*
        ,'' AS heap_user_id
        ,'' AS heap_session_id
        ,'' AS last_touch_channel
        ,'' as first_touch_channel
        , '' as paid_platform
    FROM {{ref('stg_shopify_orders')}} o 
)

SELECT *
FROM orders 