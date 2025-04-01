{{ config(materialized='table') }}


WITH refund AS 
(
    SELECT date(created_at) AS date
    ,{{ dbt_utils.pivot('nc_rc_gender', dbt_utils.get_column_values(ref('mart_transactions'), 'nc_rc_gender')
        , agg='sum'
        , then_value='usd_amount'
        , suffix = '_refund')}}
    ,sum(usd_amount) AS tot_refund
    FROM {{ref('mart_transactions')}}
    WHERE transaction_type = 'refund'
    GROUP BY 1
)

SELECT *
FROM refund