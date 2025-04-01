{{ config(materialized='table') }}

WITH hist AS (
    SELECT *
    FROM {{ ref('stg_past_dates') }}
),

futu AS (
    SELECT *
    FROM {{ ref('stg_future_365_days') }}
),

all_dates AS (
    SELECT COALESCE(DATE(h.date), f.date) AS date
    FROM hist h 
    FULL JOIN futu f 
        ON h.date = f.date
),

spend_plat AS (
    SELECT *
    FROM {{ ref('int_daily_spend_pivot_sum_by_platform') }}
),

spend_camp AS (
    SELECT *
    FROM {{ ref('int_daily_spend_pivot_sum_by_campaign_targ') }}
),

orders AS (
    SELECT *
    FROM {{ ref('int_orders_metrics_pivot_by_nc_rc') }}
),

transactions AS (
    SELECT *
    FROM {{ ref('int_transactions_refund_agg_by_nr') }}
),

refund_adjusted AS (
    SELECT *
    FROM {{ ref('int_transactions_refund_agg_by_order_date') }}
),

forecasts AS (
    SELECT *
    FROM {{ ref('stg_forecast') }}
),

cogs AS (
    SELECT *
    FROM {{ ref('stg_estimated_cogs_daily') }}
),

all_data AS (
    SELECT 
        CASE 
            WHEN EXTRACT(MONTH FROM d.date) = 2 AND EXTRACT(DAY FROM d.date) = 29 
                THEN NULL 
            ELSE DATE_ADD(d.date, INTERVAL 1 YEAR) 
        END AS date,
        
        {{ dbt_utils.star(ref('int_daily_spend_pivot_sum_by_platform'), except=['date']) }},
        {{ dbt_utils.star(ref('int_daily_spend_pivot_sum_by_campaign_targ'), except=['date']) }},
        {{ dbt_utils.star(ref('int_orders_metrics_pivot_by_nc_rc'), except=['date']) }},
        {{ dbt_utils.star(ref('int_transactions_refund_agg_by_nr'), except=['date']) }},
        {{ dbt_utils.star(ref('int_transactions_refund_agg_by_order_date'), except=['date']) }},
        {{ dbt_utils.star(ref('stg_estimated_cogs_daily'), except=['date']) }},
        {{ dbt_utils.star(ref('stg_forecast'), except=['date'], suffix='_forecast') }}

    FROM all_dates d 
    LEFT JOIN orders o ON d.date = o.date 
    LEFT JOIN spend_camp sc ON d.date = sc.date
    LEFT JOIN spend_plat sp ON d.date = sp.date 
    LEFT JOIN transactions t ON d.date = t.date 
    LEFT JOIN refund_adjusted r ON d.date = r.date 
    LEFT JOIN cogs c ON d.date = DATE(c.date)
    LEFT JOIN forecasts f ON d.date = f.date
)

SELECT *
FROM all_data
WHERE date IS NOT NULL
