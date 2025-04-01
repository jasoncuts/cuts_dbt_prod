
{{ config(materialized='table') }}


WITH orders AS 
(
    SELECT date(ordered_at) AS date
        {# ,split_part(order_gender, '_', 1) AS gender #}
        ,{{ dbt_utils.pivot('nc_rc', dbt_utils.get_column_values(ref('mart_orders'),'nc_rc')
            , agg='sum'
            , then_value='discounted_shipping'
            , suffix = '_shipping_revenue')}}
        ,sum(discounted_shipping) AS tot_shipping_revenue
        ,{{ dbt_utils.pivot('nc_rc', dbt_utils.get_column_values(ref('mart_orders'),'nc_rc')
            , agg='sum'
            , then_value='subtotal_order_price'
            , suffix = '_item_revenue')}}
        ,sum(subtotal_order_price) AS tot_item_revenue
        ,{{ dbt_utils.pivot('nc_rc', dbt_utils.get_column_values(ref('mart_orders'),'nc_rc')
            , agg='sum'
            , then_value='subtotal_order_price+discounted_shipping'
            , suffix = '_order_revenue')}}
        ,sum(subtotal_order_price+discounted_shipping) AS tot_order_revenue
        ,{{ dbt_utils.pivot('nc_rc', dbt_utils.get_column_values(ref('mart_orders'),'nc_rc')
            , agg='sum'
            , then_value='item_count'
            , suffix = '_items')}}
        ,sum(item_count) AS tot_items
        ,{{ dbt_utils.pivot('nc_rc', dbt_utils.get_column_values(ref('mart_orders'),'nc_rc')
            , agg='sum'
            , then_value='1'
            , suffix = '_orders')}}
        ,count(id) AS tot_orders
    FROM {{ref('mart_orders')}}
    GROUP BY 1
)

SELECT *
FROM orders 