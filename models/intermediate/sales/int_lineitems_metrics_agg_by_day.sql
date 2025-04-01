{{ config(materialized='table', cluster_by=['date']) }}

WITH lineitems AS (
  SELECT
    DATE(ordered_at) AS date,
    SUM(quantity) AS items_count,
    -- COUNT(DISTINCT order_id) AS orders_count,
    -- COUNT(DISTINCT customer_id) AS customers_count,
    SUM(item_total_price_paid) AS items_sales,
    SUM(full_price) AS gross_item_sales,
    SUM(item_total_discount) AS items_discount,
    SUM(rev_refunded) AS rev_refunded,
    CAST(SUM(CASE 
               WHEN ordered_at <= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
               THEN qty_refunded 
             END) AS FLOAT64) / CAST(SUM(quantity) AS FLOAT64) AS refund_rate,
    CAST(SUM(CASE 
               WHEN ordered_at <= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
               THEN rev_refunded 
             END) AS FLOAT64) / CAST(SUM(item_total_price_paid) AS FLOAT64) AS rev_refund_rate
  FROM {{ ref('mart_lineitems') }}
  GROUP BY date
)

SELECT
  *,
  CASE 
    WHEN 1 - (items_sales / CAST(gross_item_sales AS FLOAT64)) >= 0 
    THEN 1 - (items_sales / CAST(gross_item_sales AS FLOAT64))
    ELSE 0 
  END AS discount_rate
FROM lineitems
