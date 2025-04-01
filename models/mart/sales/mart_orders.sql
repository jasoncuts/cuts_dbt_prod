{{ config(materialized='table') }}

WITH orders_channel AS (
    SELECT *
    FROM {{ ref('int_orders_channels') }}
    WHERE cancelled_at IS NULL
      AND order_source IN ('Sales Order', 'Mobile App')
),

orders_with_features AS (
    SELECT 
        *,
        SPLIT(order_type, '_')[OFFSET(0)] AS nc_rc,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY ordered_at
        ) AS sale_orders_running_count,
        MIN(ordered_at) OVER (
            PARTITION BY customer_id
        ) AS first_sales_date,
        LAG(ordered_at) OVER (
            PARTITION BY customer_id
            ORDER BY ordered_at
        ) AS previous_sale_date
    FROM orders_channel
)

SELECT 
    *,
    DATE_DIFF(ordered_at, first_sales_date, DAY) AS days_since_first_sale,
    DATE_DIFF(ordered_at, previous_sale_date, DAY) AS days_since_last_sale
FROM orders_with_features
