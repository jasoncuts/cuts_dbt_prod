{{ config(materialized='table') }}

WITH prepped AS (
  SELECT 
    date,
    product_cat,
    REPLACE(product_cat, '/', '_') AS product_cat_sanitized,
    items_count
  FROM {{ ref('int_lineitems_sold_sum_by_forecast_cat') }}
  WHERE product_cat IS NOT NULL
)

SELECT 
  date,
  {{ dbt_utils.pivot(
      'product_cat_sanitized',
      dbt_utils.get_column_values(ref('int_lineitems_sold_sum_by_forecast_cat'), 'product_cat') 
        | map('replace', '/', '_') | list,
      agg='sum',
      then_value='items_count',
      suffix='_sold'
    ) }}
FROM prepped
GROUP BY date
