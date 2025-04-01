
{{ config(materialized='table') }}

WITH discounts AS 
(
    SELECT discount_keyword
        ,sum(discount_amount) AS total_discount
        ,count(distinct id) AS items_sold
        ,count(distinct order_id) AS orders_used
    FROM {{ref('mart_lineitems_breakdown')}}
    GROUP BY 1
)

SELECT *
FROM discounts 