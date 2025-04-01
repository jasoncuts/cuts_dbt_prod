
{{ config(materialized='table') }}

WITH products AS 
(
    SELECT *
    FROM {{ref('stg_shopify_products')}} 
)

SELECT *
FROM products