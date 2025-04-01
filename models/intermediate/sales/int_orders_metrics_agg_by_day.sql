
{{ config(materialized='table') }}


WITH orders AS 
(
    SELECT date(ordered_at) AS date
        ,count(DISTINCT id) AS orders_count
        {# ,sum(item_count) AS items_count #}
        ,count(DISTINCT customer_id) AS customers_count
        ,sum(subtotal_order_price+discounted_shipping) AS order_revenue
        ,sum(subtotal_order_price) AS item_revenue
        ,sum(discounted_shipping) AS shipping_revenue
        ,sum(total_tax) AS order_taxes
        ,sum(items_cost) AS items_cost
    FROM {{ref('mart_orders')}} 
    GROUP BY 1
)

SELECT *
FROM orders 