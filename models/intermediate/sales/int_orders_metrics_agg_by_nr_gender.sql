
{{ config(materialized='table') }}


WITH orders AS 
(
    SELECT date(ordered_at) AS date
        ,order_type
        ,order_gender
        ,case when shipping_address_country = 'United States' then 'US' else 'Internation' end as demography
        ,count(DISTINCT id) AS orders_count
        ,count(DISTINCT customer_id) AS customers_count
        ,sum(subtotal_order_price+discounted_shipping) AS order_revenue
        ,sum(item_count) AS item_count
        ,sum(total_tax) AS order_taxes
        ,sum(items_cost) AS items_cost
    FROM {{ref('mart_orders')}} 
    GROUP BY 1,2,3,4
)

SELECT *
    ,CASE WHEN lower(order_gender) LIKE '%men%' THEN 'men'
     WHEN lower(order_gender) LIKE '%wom%' THEN 'wom'
     END AS gender
    ,concat(concat(CASE WHEN lower(order_gender) LIKE '%men%' THEN 'men'
     WHEN lower(order_gender) LIKE '%wom%' THEN 'wom'
     END, concat('_',order_type)),  concat('_',demography)) AS order_def
FROM orders 