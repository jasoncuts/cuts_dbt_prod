{{ config(materialized='table') }}

WITH q4_23_forecasts AS 
(
    SELECT 
        DATE(date) AS date,
        CAST(adjusted_orders_count AS INT64) AS orders,
        CAST(adjusted_orders_count * nc_rc_ratio AS INT64) AS rc_orders,
        CAST(adjusted_orders_count - (adjusted_orders_count * nc_rc_ratio) AS INT64) AS nc_orders,
        rc_aov,
        nc_aov,
        CAST(adjusted_orders_count * nc_rc_ratio AS INT64) * rc_aov AS rc_revenue,
        CAST(adjusted_orders_count - (adjusted_orders_count * nc_rc_ratio) AS INT64) * nc_aov AS nc_revenue,
        meta_ AS facebook,
        google_ AS google, 
        tik_tok,
        0 AS influencer,
        total AS total_spend
    FROM {{ source('gsheet', 'forecasts') }}
),

q1_24_forecasts AS 
(
    SELECT 
        DATE(date) AS date,
        CAST(adjusted_orders_count AS INT64) AS orders,
        CAST(rc_adjusted AS INT64) AS rc_orders,
        CAST(nc_adjusted AS INT64) AS nc_orders,
        rc_aov,
        nc_aov,
        CAST(rc_adjusted AS INT64) * rc_aov AS rc_revenue,
        CAST(nc_adjusted AS INT64) * nc_aov AS nc_revenue,
        meta_ AS facebook,
        google_ AS google, 
        tik_tok,
        0 AS influencer,
        total AS total_spend
    FROM {{ source('gsheet', 'q124_forecasts') }}
),

q2_24_forecasts AS 
(
    SELECT 
        DATE(date) AS date,
        CAST(adjusted_orders_count AS INT64) AS orders,
        CAST(rc_adjusted AS INT64) AS rc_orders,
        CAST(nc_adjusted AS INT64) AS nc_orders,
        rc_aov,
        nc_aov,
        CAST(rc_adjusted AS INT64) * rc_aov AS rc_revenue,
        CAST(nc_adjusted AS INT64) * nc_aov AS nc_revenue,
        meta_ AS facebook,
        google_ AS google, 
        tik_tok,
        affiliate AS influencer,
        total AS total_spend
    FROM {{ source('gsheet', 'q_224_plan') }}
),

q3_24_forecasts AS 
(
    SELECT 
        DATE(date) AS date,
        CAST(adjusted_orders_count AS INT64) AS orders,
        CAST(rc_adjusted AS INT64) AS rc_orders,
        CAST(nc_adjusted AS INT64) AS nc_orders,
        rc_aov,
        nc_aov,
        CAST(rc_adjusted AS INT64) * rc_aov AS rc_revenue,
        CAST(nc_adjusted AS INT64) * nc_aov AS nc_revenue,
        meta_ AS facebook,
        google_ AS google, 
        tik_tok,
        affiliate AS influencer,
        total AS total_spend
    FROM {{ source('gsheet', 'q_324_plan') }}
)

SELECT * FROM q4_23_forecasts 
UNION ALL 
SELECT * FROM q1_24_forecasts
UNION ALL 
SELECT * FROM q2_24_forecasts
UNION ALL 
SELECT * FROM q3_24_forecasts
