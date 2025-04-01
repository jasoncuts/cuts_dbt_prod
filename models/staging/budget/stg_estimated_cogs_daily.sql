{{ config(materialized='table') }}

WITH cogs AS 
(
    SELECT *
    FROM {{ ref('stg_estimated_cogs') }}
),

dates AS 
(
    SELECT *,
        EXTRACT(DAY FROM DATE_SUB(DATE_ADD(DATE_TRUNC(date, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY)) AS days_in_month
    FROM {{ ref('stg_all_dates') }}
),

daily_cogs AS 
(
    SELECT 
        d.date,
        d.days_in_month,
        c.dcogs,
        c.net_income / d.days_in_month AS avg_daily_net_income,
        c.assumed_expenses / d.days_in_month AS avg_daily_expenses
    FROM dates d  
    LEFT JOIN cogs c 
    ON DATE_TRUNC(d.date, MONTH) = DATE_TRUNC(c.date, MONTH)
)

SELECT *
FROM daily_cogs
