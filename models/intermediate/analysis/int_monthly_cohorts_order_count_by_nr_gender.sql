{{ config(materialized='table') }}

WITH retention AS (
    SELECT 
        DATE_TRUNC(first_order_date, MONTH) AS cohort_month,
        months_since_first_order,
        CASE 
            WHEN orders_running_count = 1 THEN 0
            WHEN orders_running_count > 1 THEN 1
        END AS new_vs_return,
        order_gender,
        COUNT(DISTINCT id) AS order_count
    FROM {{ ref('stg_shopify_orders') }}
    WHERE order_source = 'Sales Order'
      AND ordered_at <= DATE_TRUNC(CURRENT_DATE(), MONTH)
    GROUP BY 1, 2, 3, 4
    ORDER BY 1 DESC, 2 ASC, 3
)

SELECT 
    EXTRACT(MONTH FROM cohort_month) AS month_num,
    DATE_DIFF(CURRENT_DATE(), cohort_month, MONTH) AS months_passed,
    cohort_month,
    months_since_first_order,
    new_vs_return,
    months_since_first_order + new_vs_return AS adjusted_month_sfo,
    order_gender,
    order_count
FROM retention
