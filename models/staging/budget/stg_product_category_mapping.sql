{{ config(materialized='table') }}

SELECT  
  *
FROM {{source('gsheet', 'product_category_mapping')}}