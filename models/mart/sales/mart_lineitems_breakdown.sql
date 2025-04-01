
{{ config(materialized='table') }}

WITH lineitems AS 
(
    {{ dbt_utils.union_relations(
    relations=[ref('int_lineitems_prices_unpivoted'), ref('stg_shopify_discounts')])
    }}
)

SELECT *
    ,coalesce(price_amount,discount_amount) AS amount
    ,coalesce(title,code) AS discount_keyword
FROM lineitems 