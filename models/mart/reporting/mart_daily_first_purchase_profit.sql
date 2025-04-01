
{{ config(materialized='table') }}

WITH paid AS
(
    SELECT *
    FROM {{ref('int_daily_spend_pivot_sum_by_campaign_gender')}}
),

orders AS 
(
    SELECT *
    FROM {{ref('int_orders_metrics_pivot_by_gender')}}
),

transactions AS
(
    SELECT *
    FROM {{ref('int_transactions_metrics_agg_by_day')}}
),

all_data AS 
(
    SELECT o.*
        ,{{ dbt_utils.star(ref('int_daily_spend_pivot_sum_by_campaign_gender'),except=['date']) }}
        ,round(t.refund,2) AS refund 
        ,round(t.capture,2) AS captured_payment
        ,round(t.sale,2) AS sale_payment
    FROM orders o
    LEFT JOIN paid p
    ON o.date = p.date
    LEFT JOIN transactions t 
    ON o.date = t.date
)

SELECT *
FROM all_data