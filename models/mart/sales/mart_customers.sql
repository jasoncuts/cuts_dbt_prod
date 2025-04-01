
{{ config(materialized='table') }}

WITH customers AS 
(
    SELECT *
    FROM {{ref('stg_shopify_customers')}}
),

order_info as 
(
    SELECT customer_id
        ,min(ordered_at) AS first_order_date
        ,count(distinct id) AS total_sales_orders
        ,max(heap_user_id) AS latest_heap_id 
    FROM {{ref("mart_orders")}}
    GROUP BY 1 
),

lineitem_info AS 
(
    SELECT customer_id
        ,SUM(quantity) AS total_items_bought
        ,SUM(coalesce(qty_refunded,0)) AS total_items_refunded 
        ,SUM(item_total_price_paid) AS total_items_revenue
        ,SUM(rev_refunded) AS item_rev_refunded
    FROM {{ref('mart_lineitems')}}
    GROUP BY 1 
)

SELECT *
FROM customers 