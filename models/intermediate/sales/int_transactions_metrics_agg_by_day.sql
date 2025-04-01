
{{ config(materialized='table') }}


WITH transactions AS 
(
    SELECT  date(created_at) as date
        ,{{ dbt_utils.pivot('transaction_type', dbt_utils.get_column_values(ref('mart_transactions'), 'transaction_type')
            , agg = 'sum'
            , then_value='usd_amount')}}
    FROM {{ref('mart_transactions')}}
    GROUP BY 1 
)

SELECT *
FROM transactions