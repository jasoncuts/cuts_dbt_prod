
{{ config(materialized='table') }}

WITH paid AS
(
    SELECT *
    FROM {{ref('int_daily_spend_pivot_sum_by_campaign_type')}}
),

spend AS
(
    SELECT *
    FROM {{ref('stg_otherspends')}}
),

orders AS 
(
    SELECT *
    FROM {{ref('int_orders_metrics_pivot_by_nr')}}
),

refunds as (
    SELECT *
    FROM {{ref('int_transactions_refund_agg_by_nr_gender')}}  
),

cogs AS 
(
    SELECT *
    FROM {{ref('stg_estimated_cogs_daily')}}
),

forecast as (
    SELECT *
    FROM {{ref('stg_forecast')}}
),

influencers_breakdown as (
    SELECT *
    FROM {{ref('stg_influencer_spend_breakdown')}}
),

all_data AS 
(
    SELECT o.*
        ,{{ dbt_utils.star(ref('int_daily_spend_pivot_sum_by_campaign_type'),except=['date']) }}
        ,{{ dbt_utils.star(ref('int_transactions_refund_agg_by_nr_gender'),except=['date']) }}
        ,{{ dbt_utils.star(ref('stg_estimated_cogs_daily'),except=['date']) }}
        ,{{ dbt_utils.star(ref('stg_forecast'),except=['date']) }}
        ,{{ dbt_utils.star(ref('stg_otherspends'),except=['date']) }}
        ,{{ dbt_utils.star(ref('stg_influencer_spend_breakdown'),except=['date']) }}
    FROM orders o
    LEFT JOIN paid p
    ON o.date = p.date
    LEFT JOIN refunds as r
    ON o.date = r.date
    LEFT JOIN cogs c 
    ON o.date = date(c.date)
    LEFT JOIN forecast f 
    ON o.date = date(f.date)
    LEFT JOIN spend s
    ON o.date = date(s.date)
    LEFT JOIN influencers_breakdown ib
    ON o.date = date(ib.date)
)

SELECT *
FROM all_data