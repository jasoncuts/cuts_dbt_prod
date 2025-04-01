WITH sales AS (
    SELECT 
        DATE_TRUNC(ordered_at, DAY) AS date,
        product_handle,
        SUM(quantity) AS items_sold
    FROM {{ ref('mart_lineitems') }} ml
--    WHERE ordered_at < CURRENT_DATE
    GROUP BY 1, 2
),

product_info AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_handle 
            ORDER BY qty_onhand DESC
        ) AS rn
    FROM {{ ref('mart_netsuite_products_master') }}
),

google_views AS (
    SELECT 
        DATE_TRUNC(time, DAY) AS date,
        SPLIT(page_path, 'products/')[SAFE_OFFSET(1)] AS product_handle,
        COUNT(session_id) AS items_views,
        SUM(CASE WHEN LOWER(channel) LIKE '%paid%' THEN 1 ELSE 0 END) AS paid_views,
        SAFE_DIVIDE(
            SUM(CASE WHEN LOWER(channel) LIKE '%paid%' THEN 1 ELSE 0 END),
            COUNT(session_id)
        ) AS paid_ratio
    FROM {{ ref("int_ga4_pageviews") }}
    WHERE LOWER(page_path) LIKE '%product%'
      AND time >= DATE('2023-08-01')
    GROUP BY 1, 2
),

views AS (
    SELECT *
    FROM google_views
),

joins AS (
    SELECT 
        s.*,
        pi.product_gender,
        pi.forecast_cat,
        pi.display_name,
        pi.color,
        v.items_views,
        v.paid_views,
        v.paid_ratio
    FROM sales s 
    LEFT JOIN views v 
        ON s.date = v.date
        AND s.product_handle = v.product_handle
    LEFT JOIN product_info pi 
        ON s.product_handle = pi.product_handle
    WHERE pi.rn = 1
)

SELECT *
FROM joins
