
{{ config(materialized='table') }}

WITH lineitems AS 
(
    SELECT *
    FROM {{ref('stg_shopify_lineitems')}}
),

orders AS 
(
    SELECT *
    FROM {{ref('stg_shopify_orders')}} 
),

lines AS 
(
    SELECT l.*
        ,CASE WHEN lower(l.properties) LIKE '%kit%' THEN true ELSE false END AS kit_discount
        ,CASE WHEN lower(l.sku) LIKE '%gift%' THEN 'gift'
        ELSE l.gender END AS ns_gender
        ,o.financial_status
        ,o.ordered_at
        ,date_trunc(o.ordered_at, month) AS ordered_month
        ,o.total_discounts
        ,o.taxes_included
        ,o.total_tax
        ,o.total_shipping
        ,o.total_order_price
        ,o.subtotal_order_price
        ,o.orders_running_count
        ,o.days_since_last_order
        ,o.days_since_first_order
        ,o.customer_id 
        ,o.order_source
    FROM lineitems l 
    LEFT JOIN orders o 
    ON l.order_id = o.id
    WHERE o.cancelled_at IS NULL 

)

SELECT *
FROM lines 