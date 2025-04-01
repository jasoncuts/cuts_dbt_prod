
{{ config(materialized='table') }}


WITH orders AS 
(
    SELECT date
        ,{{ dbt_utils.pivot('order_def', dbt_utils.get_column_values(ref('int_orders_metrics_agg_by_nr_gender'), 'order_def')
            , agg='sum'
            , then_value='orders_count'
            , suffix = '_orders') }}
        ,{{ dbt_utils.pivot('order_def', dbt_utils.get_column_values(ref('int_orders_metrics_agg_by_nr_gender'), 'order_def')
            , agg='sum'
            , then_value='customers_count'
            , suffix = '_customers') }}
        ,{{ dbt_utils.pivot('order_def', dbt_utils.get_column_values(ref('int_orders_metrics_agg_by_nr_gender'), 'order_def')
            , agg='sum'
            , then_value='order_revenue'
            , suffix = '_revenue') }}
        ,{{ dbt_utils.pivot('order_def', dbt_utils.get_column_values(ref('int_orders_metrics_agg_by_nr_gender'), 'order_def')
            , agg='sum'
            , then_value='item_count'
            , suffix = '_items') }}
        ,{{ dbt_utils.pivot('order_def', dbt_utils.get_column_values(ref('int_orders_metrics_agg_by_nr_gender'), 'order_def')
            , agg='sum'
            , then_value='items_cost'
            , suffix = '_cost') }}
    FROM {{ref('int_orders_metrics_agg_by_nr_gender')}} 
    GROUP BY 1
)

SELECT *
FROM orders 