
{{ config(materialized='table') }}

WITH order_breakdown AS 
(
    SELECT *
    FROM {{ref('int_orders_metrics_agg_by_nr_gender')}}
)

SELECT *
FROM order_breakdown