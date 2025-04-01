
{{ config(materialized='table') }}


WITH refund AS 
(
    SELECT date(ordered_at) AS date
    {# ,split_part(order_gender, '_', 1) AS gender #}
    ,{{ dbt_utils.pivot('nc_rc', dbt_utils.get_column_values(ref('mart_transactions'), 'nc_rc')
        , agg='sum'
        , then_value='usd_amount'
        , prefix = 'adjusted_'
        , suffix = '_refund')}}
    ,sum(usd_amount) AS adjusted_tot_refund
    FROM {{ref('mart_transactions')}}
    WHERE transaction_type = 'refund'
    GROUP BY 1
)

SELECT *
FROM refund