
{{ config(materialized='table') }}

WITH transactions AS 
(
    SELECT *,
    concat(nc_rc, concat('_',order_gender)) as nc_rc_gender
    FROM {{ref('stg_shopify_transactions')}}
)

SELECT *
FROM transactions 
WHERE lower(status) = 'success'