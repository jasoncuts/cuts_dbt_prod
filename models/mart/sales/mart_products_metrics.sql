
{{ config(materialized='table') }}

WITH products AS 
(
    SELECT *
    FROM {{ref('int_products_sold_v_views')}} 
)

SELECT *
FROM products